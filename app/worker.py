import asyncio
import os
import traceback
from datetime import datetime

import aiohttp
import pandas as pd
from celery import Celery

from app.database import get_db_engine
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
    try:
        engine = get_db_engine(db_config)
        target = query_config["target_col"]
        date_col = query_config.get("date_col")
        db_name = db_config.get("dbname", "UnknownDB")

        # 1. SQL Construction
        date_select = (
            f", t1.{date_col} as record_date" if date_col else ", '' as record_date"
        )
        conds = []
        if query_config.get("filter_col") and query_config.get("filter_val"):
            alias = "t2" if query_config["use_join"] else "t1"
            conds.append(
                f"{alias}.{query_config['filter_col']} = '{query_config['filter_val']}'"
            )
        if date_col and query_config.get("start_date") and query_config.get("end_date"):
            alias = "t2" if query_config["use_join"] else "t1"
            date_select = f", {alias}.{date_col} as record_date"
            conds.append(
                f"{alias}.{date_col} BETWEEN '{query_config['start_date']} 00:00:00' AND '{query_config['end_date']} 23:59:59'"
            )
        where_sql = ("WHERE " + " AND ".join(conds)) if conds else ""

        if query_config["use_join"]:
            sql = f"SELECT t1.{query_config['primary_col']} as customer_id {date_select}, t2.{target} as raw_data FROM {query_config['primary_table']} t1 JOIN {query_config['secondary_table']} t2 ON t1.{query_config['primary_col']} = t2.{query_config['secondary_col']} {where_sql}"
        else:
            sql = f"SELECT t1.{query_config['single_id']} as customer_id {date_select}, t1.{target} as raw_data FROM {query_config['single_table']} t1 {where_sql}"

        all_leads, all_loans, all_enqs, all_analysis = [], [], [], []
        total_fetched = 0

        # 2. Fetch & Parse
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
                    l["Record_Date"] = r_date
                    l["Source_DB"] = db_name  # Add Source DB
                    all_leads.append(l)

                    for item in lo:
                        item["Record_Date"] = r_date
                        item["Source_DB"] = db_name
                    all_loans.extend(lo)

                    for item in eq:
                        item["Record_Date"] = r_date
                        item["Source_DB"] = db_name
                    all_enqs.extend(eq)

                    try:
                        an_row = InsightEngine.generate_analytics(l, lo)
                        an_row["Record_Date"] = r_date
                        an_row["Source_DB"] = db_name
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

        # 3. PROCESSING LAYER (Date Extraction & Deduplication)
        self.update_state(
            state="PROGRESS", meta={"status": "Cleaning & Splitting...", "progress": 80}
        )

        # Helper to process a dataframe
        def process_df(data_list, sort_keys, unique_keys, date_field_for_month=None):
            if not data_list:
                return pd.DataFrame(), pd.DataFrame()
            df = pd.DataFrame(data_list)

            # Date Extraction
            if "Record_Date" in df.columns:
                df["Record_Month"] = pd.to_datetime(
                    df["Record_Date"], errors="coerce"
                ).dt.strftime("%b-%Y")

            if date_field_for_month and date_field_for_month in df.columns:
                df["Loan_Start_Month"] = pd.to_datetime(
                    df[date_field_for_month], errors="coerce"
                ).dt.strftime("%b-%Y")

            # Identify Duplicates
            # We explicitly want to KEEP the first and mark others as dupes
            if unique_keys:
                duplicates = df[df.duplicated(subset=unique_keys, keep="first")]
                clean = df.drop_duplicates(subset=unique_keys, keep="first")
                return clean, duplicates
            return df, pd.DataFrame()

        # A. Leads
        df_leads_clean, df_leads_dupe = process_df(
            all_leads, ["Record_Date"], ["Customer_ID"]
        )

        # B. Analysis
        df_an_clean, df_an_dupe = process_df(
            all_analysis, ["Record_Date"], ["Customer_ID"]
        )

        # C. Loans (Unique by Customer + Account Number + Bank)
        df_loans_clean, df_loans_dupe = process_df(
            all_loans,
            ["Record_Date"],
            ["Customer_ID", "Account_Number", "Bank_Name"],
            date_field_for_month="Date_Opened",
        )

        # D. Enquiries (Unique by Customer + Date + Lender + Amount)
        df_enqs_clean, df_enqs_dupe = process_df(
            all_enqs, ["Record_Date"], ["Customer_ID", "Date", "Lender", "Amount"]
        )

        # 4. EXCEL GENERATION
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        clean_filename = f"{db_name}_{timestamp}_Clean.xlsx"
        dupe_filename = f"{db_name}_{timestamp}_Duplicates.xlsx"

        clean_path = os.path.join("output", clean_filename)
        dupe_path = os.path.join("output", dupe_filename)

        # Column Ordering Definitions
        lead_cols_start = [
            "Customer_ID",
            "Record_Date",
            "Record_Month",
            "Customer_Name",
            "PAN",
            "Mobile",
        ]
        lead_cols_end = ["Source_DB"]  # Source DB at the end

        # Helper for Ordering
        def strict_order(df, is_lead=False):
            if df.empty:
                return df
            cols = list(df.columns)

            # Start
            start_c = [c for c in lead_cols_start if c in cols]

            # End (Source_DB)
            end_c = [c for c in lead_cols_end if c in cols]

            # Middle (Profile Glance first, then others)
            glance_cols = [
                "Has_Home_Loan",
                "Has_Auto_Loan",
                "Has_Credit_Card",
                "Has_Business_Loan",
                "Has_Personal_Loan",
                "Active_Trade_Count",
            ]
            glance_c = [c for c in glance_cols if c in cols]

            # The rest
            used = set(start_c + end_c + glance_c)
            middle_c = [c for c in cols if c not in used]

            final_order = start_c + glance_c + middle_c + end_c
            return df[final_order]

        SHEET_ORDER = [
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

        # WRITE FUNCTION
        def write_excel(path, d_leads, d_an, d_loans, d_enqs):
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                # Leads
                d_leads.pipe(strict_order).to_excel(
                    writer, sheet_name="Leads", index=False
                )
                # Analysis
                d_an.pipe(strict_order).to_excel(
                    writer, sheet_name="Lead_Analysis", index=False
                )

                # Buckets
                buckets = {k: [] for k in SHEET_ORDER if "_Loans" in k or "_Cards" in k}
                if not d_loans.empty:
                    for _, loan in d_loans.iterrows():
                        cat = loan.get("Mapped_Category", "Other_Loans")
                        if cat in buckets:
                            buckets[cat].append(loan)
                        else:
                            buckets["Other_Loans"].append(loan)

                for cat in buckets:
                    if buckets[cat]:
                        pd.DataFrame(buckets[cat]).pipe(strict_order).to_excel(
                            writer, sheet_name=cat, index=False
                        )

                # All Tradelines
                d_loans.pipe(strict_order).to_excel(
                    writer, sheet_name="All_Tradelines", index=False
                )
                # Enquiries
                d_enqs.pipe(strict_order).to_excel(
                    writer, sheet_name="Enquiries", index=False
                )

        # Generate BOTH files
        write_excel(
            clean_path, df_leads_clean, df_an_clean, df_loans_clean, df_enqs_clean
        )
        if not df_leads_dupe.empty or not df_loans_dupe.empty:
            write_excel(
                dupe_path, df_leads_dupe, df_an_dupe, df_loans_dupe, df_enqs_dupe
            )

        return {
            "status": "Completed",
            "file_path": clean_path,
            "dupe_path": dupe_path if os.path.exists(dupe_path) else None,
            "total_rows": len(df_leads_clean),
            "total_fetched": total_fetched,
        }
    except Exception as e:
        traceback.print_exc()
        return {"status": "Failed", "error": str(e)}
