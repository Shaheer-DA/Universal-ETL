import asyncio
import os
import traceback
from datetime import datetime

import aiohttp
import pandas as pd
from celery import Celery

from app.cleaner_engine import CleanerEngine  # <--- IMPORT NEW ENGINE
from app.database import get_db_engine
from app.enrichment_engine import EnrichmentEngine
from app.insight_engine import InsightEngine
from app.parser_engine import ParserEngine

celery_app = Celery(
    "etl_worker", broker="redis://localhost:6379/0", backend="redis://localhost:6379/0"
)


def cancel_task(task_id):
    celery_app.control.revoke(task_id, terminate=True)
    return True


async def fetch_url(session, url):
    try:
        async with session.get(url, timeout=15, ssl=False) as response:
            if response.status == 200:
                try:
                    return await response.json()
                except:
                    return {"error": "INVALID_JSON"}
            else:
                return {"error": f"HTTP_{response.status}"}
    except:
        return {"error": "FETCH_FAIL"}


async def fetch_batch_urls(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in urls]
        return await asyncio.gather(*tasks)


@celery_app.task(bind=True)
def run_etl_job(self, db_config, query_config):
    self.update_state(
        state="PROGRESS", meta={"status": "Connecting to DB...", "progress": 0}
    )

    # Initialize Engines
    cleaner = CleanerEngine()
    EnrichmentEngine.load_masters()  # Ensure masters are loaded

    try:
        engine = get_db_engine(db_config)
        target = query_config["target_col"]
        date_col = query_config.get("date_col")
        db_name = db_config.get("dbname", "UnknownDB")

        # 1. SQL Construction
        select_clause = (
            f"t1.{query_config['primary_col']} as customer_id, t2.{target} as raw_data"
        )
        if date_col:
            select_clause += f", t1.{date_col} as record_date"
        else:
            select_clause += ", '' as record_date"

        pan_col = query_config.get("pan_col")
        mob_col = query_config.get("mobile_col")
        if pan_col:
            select_clause += f", t1.{pan_col} as db_pan"
        else:
            select_clause += ", '' as db_pan"
        if mob_col:
            select_clause += f", t1.{mob_col} as db_mobile"
        else:
            select_clause += ", '' as db_mobile"

        conds = []
        if query_config.get("filter_col") and query_config.get("filter_val"):
            alias = "t2" if query_config["use_join"] else "t1"
            conds.append(
                f"{alias}.{query_config['filter_col']} = '{query_config['filter_val']}'"
            )
        if date_col and query_config.get("start_date") and query_config.get("end_date"):
            alias = "t1"
            conds.append(
                f"{alias}.{date_col} BETWEEN '{query_config['start_date']} 00:00:00' AND '{query_config['end_date']} 23:59:59'"
            )
        where_sql = ("WHERE " + " AND ".join(conds)) if conds else ""

        if query_config["use_join"]:
            sql = f"SELECT {select_clause} FROM {query_config['primary_table']} t1 JOIN {query_config['secondary_table']} t2 ON t1.{query_config['primary_col']} = t2.{query_config['secondary_col']} {where_sql}"
        else:
            sql = f"SELECT t1.{query_config['single_id']} as customer_id, t1.{target} as raw_data {date_select.replace('t1.','')} FROM {query_config['single_table']} t1 {where_sql}"

        all_leads, all_loans, all_enqs, all_analysis = [], [], [], []
        total_fetched = 0

        # 2. Fetch & Parse Loop
        for chunk in pd.read_sql(sql, engine, chunksize=50):
            total_fetched += len(chunk)
            if query_config.get("is_url_mode"):
                base = query_config.get("base_url") or ""
                urls = [base + str(x) for x in chunk["raw_data"]]
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                chunk["json_object"] = loop.run_until_complete(fetch_batch_urls(urls))
                loop.close()
            else:
                chunk["json_object"] = chunk["raw_data"]

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
                    all_leads.append(l)

                    common = {
                        "Record_Date": r_date,
                        "Source_DB": db_name,
                        "PAN": l["PAN"],
                        "Mobile": l["Mobile"],
                    }
                    for item in lo:
                        item.update(common)
                    all_loans.extend(lo)
                    for item in eq:
                        item.update(common)
                    all_enqs.extend(eq)
                    try:
                        an_row = InsightEngine.generate_analytics(l, lo)
                        an_row.update(common)
                        all_analysis.append(an_row)
                    except:
                        pass
            self.update_state(
                state="PROGRESS",
                meta={
                    "status": f"Fetched: {total_fetched} | Valid: {len(all_leads)}",
                    "progress": 50,
                },
            )

        # 3. CLEANER ENGINE EXECUTION
        self.update_state(
            state="PROGRESS",
            meta={"status": "Running Cleaner Engine...", "progress": 80},
        )

        # A. Process Leads (Sort -> Filter Emp -> Dedupe)
        df_leads_clean, df_leads_emp, df_leads_dupe = cleaner.process_leads(all_leads)

        # Helper to extract month
        def add_month(df, col="Record_Date", target="Record_Month"):
            if not df.empty and col in df.columns:
                df[target] = pd.to_datetime(df[col], errors="coerce").dt.strftime(
                    "%b-%Y"
                )
            return df

        # Add months to leads
        df_leads_clean = add_month(df_leads_clean)
        df_leads_emp = add_month(df_leads_emp)

        # B. Process Sub-Sheets (Strict Deduplication on Loans)
        valid_ids = (
            set(df_leads_clean["Customer_ID"]) if not df_leads_clean.empty else set()
        )

        # Deduplicate Loans: Customer + Account + Bank (Fixes identical duplicates)
        df_loans_clean = cleaner.clean_sub_sheet(
            all_loans,
            valid_ids,
            unique_keys=["Customer_ID", "Account_Number", "Bank_Name"],
        )
        df_loans_clean = add_month(df_loans_clean, "Date_Opened", "Loan_Start_Month")

        # Deduplicate Enquiries: Customer + Date + Lender + Amount
        df_enqs_clean = cleaner.clean_sub_sheet(
            all_enqs, valid_ids, unique_keys=["Customer_ID", "Date", "Lender", "Amount"]
        )

        # Deduplicate Analysis: Customer ID only
        df_an_clean = cleaner.clean_sub_sheet(
            all_analysis, valid_ids, unique_keys=["Customer_ID"]
        )

        # C. Update Tracker with Category stats
        cleaner.calculate_category_stats(df_loans_clean)
        df_tracker = cleaner.get_tracker_df()

        # D. Process Duplicates/Excluded for the second file
        # (We dump the 'garbage' there)
        # Note: We don't spend too much time cleaning the garbage, just dumping it.
        # But we must define the variables to avoid NameError
        df_an_dupe = pd.DataFrame()  # Placeholder or implement if strictly needed
        df_loans_dupe = pd.DataFrame()
        df_enqs_dupe = pd.DataFrame()

        if not df_leads_dupe.empty:
            dupe_ids = set(df_leads_dupe["Customer_ID"])
            df_loans_dupe = cleaner.clean_sub_sheet(
                all_loans,
                dupe_ids,
                unique_keys=["Customer_ID", "Account_Number", "Bank_Name"],
            )
            df_enqs_dupe = cleaner.clean_sub_sheet(all_enqs, dupe_ids, unique_keys=None)
            df_an_dupe = cleaner.clean_sub_sheet(
                all_analysis, dupe_ids, unique_keys=["Customer_ID"]
            )

        # 4. EXCEL WRITING
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        clean_filename = f"{db_name}_{timestamp}_Clean.xlsx"
        dupe_filename = f"{db_name}_{timestamp}_Excluded.xlsx"
        clean_path = os.path.join("output", clean_filename)
        dupe_path = os.path.join("output", dupe_filename)

        # Strict Columns
        lead_order = [
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
                return df[[c for c in target_cols if c in cols]]

            # General ordering for other sheets
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

        SHEET_ORDER = [
            "Processing_Tracker",
            "Leads",
            "Lead_Analysis",
            "Home_Loans",
            "Personal_Loans",
            "Business_Loans",
            "Auto_Loans",
            "Credit_Cards",
            "Education_Loans",
            "Gold_Loans",
            "Other_Loans",
            "All_Tradelines",
            "Enquiries",
        ]

        def write_file(path, d_leads, d_an, d_loans, d_enqs, d_track=None, d_emp=None):
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                # 1. Tracker (First Sheet if exists)
                if d_track is not None and not d_track.empty:
                    d_track.to_excel(
                        writer, sheet_name="Processing_Tracker", index=False
                    )

                # 2. Leads
                d_leads.pipe(strict_format, target_cols=lead_order).to_excel(
                    writer, sheet_name="Leads", index=False
                )

                # 3. Employees (Only in Excluded file)
                if d_emp is not None and not d_emp.empty:
                    d_emp.pipe(strict_format, target_cols=lead_order).to_excel(
                        writer, sheet_name="Internal_Employees", index=False
                    )

                # 4. Analysis
                d_an.pipe(strict_format).to_excel(
                    writer, sheet_name="Lead_Analysis", index=False
                )

                # 5. Loan Buckets
                buckets = {k: [] for k in SHEET_ORDER if "_Loans" in k or "_Cards" in k}
                if not d_loans.empty:
                    for _, row in d_loans.iterrows():
                        cat = row.get("Mapped_Category", "Other_Loans")
                        if cat in buckets:
                            buckets[cat].append(row)
                        else:
                            buckets["Other_Loans"].append(row)

                for cat in buckets:
                    if buckets[cat]:
                        pd.DataFrame(buckets[cat]).pipe(strict_format).to_excel(
                            writer, sheet_name=cat, index=False
                        )

                d_loans.pipe(strict_format).to_excel(
                    writer, sheet_name="All_Tradelines", index=False
                )
                d_enqs.pipe(strict_format).to_excel(
                    writer, sheet_name="Enquiries", index=False
                )

        # WRITE CLEAN
        write_file(
            clean_path,
            df_leads_clean,
            df_an_clean,
            df_loans_clean,
            df_enqs_clean,
            d_track=df_tracker,
        )

        # WRITE EXCLUDED (If needed)
        has_dupes = not df_leads_dupe.empty
        has_emps = not df_leads_emp.empty

        if has_dupes or has_emps:
            write_file(
                dupe_path,
                df_leads_dupe,
                df_an_dupe,
                df_loans_dupe,
                df_enqs_dupe,
                d_emp=df_leads_emp,
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
