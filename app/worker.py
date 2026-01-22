import asyncio
import glob
import math
import os
import shutil
import time
import traceback
from datetime import datetime

import aiohttp
import pandas as pd
from celery import Celery

from app.cleaner_engine import CleanerEngine
from app.database import get_columns, get_db_engine
from app.enrichment_engine import EnrichmentEngine
from app.insight_engine import InsightEngine
from app.parser_engine import ParserEngine

# --- STARTUP ---
print("--- WORKER STARTUP: Loading Masters... ---")
try:
    EnrichmentEngine.load_masters()
    print("--- WORKER STARTUP: Masters Loaded. Ready. ---")
except Exception as e:
    print(f"--- WORKER STARTUP ERROR: {e} ---")

celery_app = Celery(
    "etl_worker", broker="redis://localhost:6379/0", backend="redis://localhost:6379/0"
)


def cancel_task(task_id):
    celery_app.control.revoke(task_id, terminate=True)
    return True


# --- 1. ENABLED URL FETCHER (FIXED) ---
async def fetch_url(session, url):
    try:
        async with session.get(url, timeout=15, ssl=False) as response:
            if response.status == 200:
                try:
                    return await response.json()
                except:
                    return {"error": "INVALID_JSON_CONTENT", "url": url}
            else:
                return {"error": f"HTTP_{response.status}", "url": url}
    except Exception as e:
        return {"error": f"FETCH_FAIL: {str(e)}", "url": url}


async def fetch_batch_urls(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in urls]
        return await asyncio.gather(*tasks)


@celery_app.task(bind=True)
def run_etl_job(self, db_config, query_config):
    self.update_state(
        state="PROGRESS", meta={"status": "Initializing...", "progress": 0}
    )
    print(f"[{self.request.id}] JOB STARTED.")

    cleaner = CleanerEngine()
    temp_dir = os.path.join(".temp_data", self.request.id)
    os.makedirs(temp_dir, exist_ok=True)

    try:
        print(f"[{self.request.id}] Connecting to DB...")
        engine = get_db_engine(db_config)
        target = query_config["target_col"]
        date_col = query_config.get("date_col")
        pan_col = query_config.get("pan_col")
        mob_col = query_config.get("mobile_col")
        db_name = db_config.get("dbname", "UnknownDB")

        # Schema
        p_cols = []
        try:
            p_cols = get_columns(engine, query_config["primary_table"])
        except:
            pass

        def get_alias(col_name):
            if not col_name:
                return None
            if col_name in p_cols:
                return f"t1.{col_name}"
            return f"t2.{col_name}"

        # SQL
        select_cols = (
            f"t1.{query_config['primary_col']} as customer_id, t2.{target} as raw_data"
        )
        date_sql = get_alias(date_col)
        select_cols += (
            f", {date_sql} as record_date" if date_sql else ", '' as record_date"
        )
        pan_sql = get_alias(pan_col)
        select_cols += f", {pan_sql} as db_pan" if pan_sql else ", '' as db_pan"
        mob_sql = get_alias(mob_col)
        select_cols += f", {mob_sql} as db_mobile" if mob_sql else ", '' as db_mobile"

        conds = []
        if query_config.get("filter_col") and query_config.get("filter_val"):
            f_col = query_config["filter_col"]
            f_alias = "t1" if f_col in p_cols else "t2"
            conds.append(f"{f_alias}.{f_col} = '{query_config['filter_val']}'")
        if date_col and query_config.get("start_date") and query_config.get("end_date"):
            d_alias = "t1" if date_col in p_cols else "t2"
            conds.append(
                f"{d_alias}.{date_col} BETWEEN '{query_config['start_date']} 00:00:00' AND '{query_config['end_date']} 23:59:59'"
            )

        where_clause = ("WHERE " + " AND ".join(conds)) if conds else ""

        if query_config["use_join"]:
            base_from = f"FROM {query_config['primary_table']} t1 JOIN {query_config['secondary_table']} t2 ON t1.{query_config['primary_col']} = t2.{query_config['secondary_col']}"
        else:
            base_from = f"FROM {query_config['single_table']} t1"

        # Count
        print(f"[{self.request.id}] Counting rows...")
        count_sql = f"SELECT COUNT(*) {base_from} {where_clause}"
        self.update_state(
            state="PROGRESS", meta={"status": "Calculating Workload...", "progress": 5}
        )
        try:
            total_records = pd.read_sql(count_sql, engine).iloc[0, 0]
        except:
            total_records = 0
        print(f"[{self.request.id}] Total Rows: {total_records}")

        BATCH_SIZE = 1000
        num_batches = math.ceil(total_records / BATCH_SIZE) if total_records > 0 else 1
        total_fetched = 0

        # LOOP
        for i in range(num_batches):
            batch_leads, batch_loans, batch_enqs, batch_analysis = [], [], [], []
            offset = i * BATCH_SIZE
            paginated_sql = f"SELECT {select_cols} {base_from} {where_clause} ORDER BY t1.{query_config['primary_col']} LIMIT {BATCH_SIZE} OFFSET {offset}"

            try:
                chunk = pd.read_sql(paginated_sql, engine)
            except Exception as e:
                print(f"Batch {i} error: {e}")
                continue

            if chunk.empty:
                break
            total_fetched += len(chunk)

            # --- 2. DOWNLOAD MODE LOGIC ---
            if query_config.get("is_url_mode"):
                base = query_config.get("base_url") or ""
                # Construct URLs safely
                urls = [base + str(x).strip() for x in chunk["raw_data"]]

                # Fetch Async
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    fetched_data = loop.run_until_complete(fetch_batch_urls(urls))
                    loop.close()
                    chunk["json_object"] = fetched_data
                except Exception as e:
                    print(f"Batch {i} Fetch Error: {e}")
                    chunk["json_object"] = [None] * len(
                        chunk
                    )  # Fallback to avoid crash
            else:
                chunk["json_object"] = chunk["raw_data"]

            # Parse
            for _, row in chunk.iterrows():
                cid = row["customer_id"]
                r_date = str(row.get("record_date", ""))

                l, lo, eq = ParserEngine.parse(row["json_object"], cid)

                if l:
                    if row.get("db_pan"):
                        l["PAN"] = str(row["db_pan"])
                    if row.get("db_mobile"):
                        l["Mobile"] = str(row["db_mobile"])
                    l["Record_Date"] = r_date
                    l["Source_DB"] = db_name
                    batch_leads.append(l)

                    common = {
                        "Record_Date": r_date,
                        "Source_DB": db_name,
                        "PAN": l["PAN"],
                        "Mobile": l["Mobile"],
                    }
                    for item in lo:
                        item.update(common)
                    batch_loans.extend(lo)
                    for item in eq:
                        item.update(common)
                    batch_enqs.extend(eq)
                    try:
                        an_row = InsightEngine.generate_analytics(l, lo)
                        an_row.update(common)
                        batch_analysis.append(an_row)
                    except:
                        pass

            if batch_leads:
                pd.DataFrame(batch_leads).to_pickle(f"{temp_dir}/leads_{i}.pkl")
            if batch_loans:
                pd.DataFrame(batch_loans).to_pickle(f"{temp_dir}/loans_{i}.pkl")
            if batch_enqs:
                pd.DataFrame(batch_enqs).to_pickle(f"{temp_dir}/enqs_{i}.pkl")
            if batch_analysis:
                pd.DataFrame(batch_analysis).to_pickle(f"{temp_dir}/analysis_{i}.pkl")

            prog = 10 + int((i / num_batches) * 60)
            self.update_state(
                state="PROGRESS",
                meta={
                    "status": f"Processing Batch {i+1}/{num_batches}...",
                    "progress": prog,
                },
            )

        # MERGE
        print(f"[{self.request.id}] Merging Checkpoints...")
        self.update_state(
            state="PROGRESS", meta={"status": "Merging & Cleaning...", "progress": 80}
        )

        def load_merged(prefix):
            files = glob.glob(f"{temp_dir}/{prefix}_*.pkl")
            if not files:
                return []
            return pd.concat(
                [pd.read_pickle(f) for f in files], ignore_index=True
            ).to_dict("records")

        all_leads = load_merged("leads")
        all_loans = load_merged("loans")
        all_enqs = load_merged("enqs")
        all_analysis = load_merged("analysis")

        try:
            shutil.rmtree(temp_dir)
        except:
            pass

        # CLEAN
        df_leads_clean, df_leads_emp, df_leads_dupe = cleaner.process_leads(all_leads)

        def add_meta(df):
            if df.empty:
                return df
            if "Record_Date" in df.columns:
                df["Record_Month"] = pd.to_datetime(
                    df["Record_Date"], errors="coerce"
                ).dt.strftime("%b-%Y")
            return df

        df_leads_clean = add_meta(df_leads_clean)
        if not df_leads_clean.empty:
            df_leads_clean.insert(0, "S.No", range(1, 1 + len(df_leads_clean)))

        valid_ids = (
            set(df_leads_clean["Customer_ID"]) if not df_leads_clean.empty else set()
        )

        df_loans_clean = cleaner.clean_sub_sheet(
            all_loans,
            valid_ids,
            unique_keys=["Customer_ID", "Account_Number", "Bank_Name"],
        )
        if not df_loans_clean.empty and "Date_Opened" in df_loans_clean.columns:
            df_loans_clean["Loan_Start_Month"] = pd.to_datetime(
                df_loans_clean["Date_Opened"], errors="coerce"
            ).dt.strftime("%b-%Y")

        df_enqs_clean = cleaner.clean_sub_sheet(
            all_enqs, valid_ids, unique_keys=["Customer_ID", "Date", "Lender", "Amount"]
        )
        df_an_clean = cleaner.clean_sub_sheet(
            all_analysis, valid_ids, unique_keys=["Customer_ID"]
        )

        cleaner.calculate_category_stats(df_loans_clean)
        df_tracker = cleaner.get_tracker_df()

        excluded_ids = set()
        if not df_leads_dupe.empty:
            excluded_ids.update(df_leads_dupe["Customer_ID"])
        if not df_leads_emp.empty:
            excluded_ids.update(df_leads_emp["Customer_ID"])

        df_an_dupe, df_loans_dupe, df_enqs_dupe = (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
        )
        if excluded_ids:
            df_loans_dupe = cleaner.clean_sub_sheet(
                all_loans,
                excluded_ids,
                unique_keys=["Customer_ID", "Account_Number", "Bank_Name"],
            )
            df_enqs_dupe = cleaner.clean_sub_sheet(
                all_enqs, excluded_ids, unique_keys=None
            )
            df_an_dupe = cleaner.clean_sub_sheet(
                all_analysis, excluded_ids, unique_keys=["Customer_ID"]
            )

        # WRITING
        print(f"[{self.request.id}] Writing Excel...")
        self.update_state(
            state="PROGRESS", meta={"status": "Writing Excel...", "progress": 90}
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        clean_filename = f"{db_name}_{timestamp}_Clean.xlsx"
        dupe_filename = f"{db_name}_{timestamp}_Excluded.xlsx"
        clean_path = os.path.join("output", clean_filename)
        dupe_path = os.path.join("output", dupe_filename)

        lead_order = [
            "S.No",
            "Customer_ID",
            "Record_Date",
            "Record_Month",
            "Customer_Name",
            "PAN",
            "Mobile",
            "City_Mapped",
            "State_Mapped",
            "Zone_Mapped",
            "CIBIL_Score",
            "CIBIL_Band",
            "Has_Home_Loan",
            "Has_Auto_Loan",
            "Has_Credit_Card",
            "Has_Business_Loan",
            "Has_Personal_Loan",
            "Active_Trade_Count",
            "Total_Accounts",
            "Active_Accounts",
            "Closed_Accounts",
            "Total_Outstanding",
            "Total_Sanctioned",
            "Total_EMI",
            "Total_Past_Due",
            "Income",
            "Employment_Type",
            "Source_DB",
        ]

        def strict_format(df, target_cols=None):
            if df.empty:
                return df
            cols = list(df.columns)
            if target_cols:
                existing = [c for c in target_cols if c in cols]
                return df[existing]

            start_c = [
                "Customer_ID",
                "Record_Date",
                "Record_Month",
                "Customer_Name",
                "PAN",
                "Mobile",
                "City_Mapped",
                "State_Mapped",
                "Zone_Mapped",
                "CIBIL_Score",
                "CIBIL_Band",
            ]
            actual_start = [c for c in start_c if c in cols]
            used = set(start_c)
            middle = [c for c in cols if c not in used and c != "Source_DB"]
            end = ["Source_DB"] if "Source_DB" in cols else []
            return df[actual_start + middle + end]

        LOAN_BUCKETS = [
            "Home_Loans",
            "Personal_Loans",
            "Business_Loans",
            "Auto_Loans",
            "Credit_Cards",
            "Education_Loans",
            "Gold_Loans",
        ]

        def write_file(path, d_l, d_a, d_lo, d_e, d_track=None, d_emp=None):
            with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
                if d_track is not None and not d_track.empty:
                    d_track.to_excel(
                        writer, sheet_name="Processing_Tracker", index=False
                    )

                d_l.pipe(strict_format, target_cols=lead_order).to_excel(
                    writer, sheet_name="Leads", index=False
                )

                if d_emp is not None and not d_emp.empty:
                    d_emp.pipe(strict_format, target_cols=lead_order).to_excel(
                        writer, sheet_name="Internal_Employees", index=False
                    )

                d_a.pipe(strict_format).to_excel(
                    writer, sheet_name="Lead_Analysis", index=False
                )

                if not d_lo.empty:
                    for bucket in LOAN_BUCKETS:
                        subset = d_lo[d_lo["Mapped_Category"] == bucket]
                        if not subset.empty:
                            subset.pipe(strict_format).to_excel(
                                writer, sheet_name=bucket, index=False
                            )

                    others = d_lo[~d_lo["Mapped_Category"].isin(LOAN_BUCKETS)]
                    if not others.empty:
                        others.pipe(strict_format).to_excel(
                            writer, sheet_name="Other_Loans", index=False
                        )

                d_lo.pipe(strict_format).to_excel(
                    writer, sheet_name="All_Tradelines", index=False
                )
                d_e.pipe(strict_format).to_excel(
                    writer, sheet_name="Enquiries", index=False
                )

        write_file(
            clean_path,
            df_leads_clean,
            df_an_clean,
            df_loans_clean,
            df_enqs_clean,
            d_track=df_tracker,
        )

        has_data = (not df_leads_dupe.empty) or (not df_leads_emp.empty)
        if has_data:
            df_emp = pd.DataFrame(df_leads_emp)
            if not df_emp.empty:
                df_emp = add_meta(df_emp)
                if "S.No" not in df_emp.columns:
                    df_emp.insert(0, "S.No", range(1, 1 + len(df_emp)))

            write_file(
                dupe_path,
                df_leads_dupe,
                df_an_dupe,
                df_loans_dupe,
                df_enqs_dupe,
                d_emp=df_emp,
            )
            return {
                "status": "Completed",
                "file_path": clean_path,
                "dupe_path": dupe_path,
                "total_rows": len(df_leads_clean),
                "total_fetched": total_fetched,
                "employees_found": len(df_leads_emp),
            }

        return {
            "status": "Completed",
            "file_path": clean_path,
            "total_rows": len(df_leads_clean),
            "total_fetched": total_fetched,
            "employees_found": 0,
        }

    except Exception as e:
        traceback.print_exc()
        return {"status": "Failed", "error": str(e)}
