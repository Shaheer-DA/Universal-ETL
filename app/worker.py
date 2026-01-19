import asyncio
import os
import traceback

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

        for chunk in pd.read_sql(sql, engine, chunksize=50):
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
                    all_leads.append(l)
                    for item in lo:
                        item["Record_Date"] = r_date
                    for item in eq:
                        item["Record_Date"] = r_date
                    if eq:
                        all_enqs.extend(eq)
                    if lo:
                        all_loans.extend(lo)
                    try:
                        an_row = InsightEngine.generate_analytics(l, lo)
                        an_row["Record_Date"] = r_date
                        all_analysis.append(an_row)
                    except:
                        pass

            self.update_state(
                state="PROGRESS", meta={"status": f"Processed rows...", "progress": 50}
            )

        self.update_state(
            state="PROGRESS", meta={"status": "Saving Excel...", "progress": 90}
        )
        filename = f"Report_{self.request.id}.xlsx"
        file_path = os.path.join("output", filename)

        # --- Helper to Reorder Columns ---
        def reorder(df):
            if df.empty:
                return df
            cols = list(df.columns)
            # FORCE these columns to be first if they exist
            start = ["Customer_ID", "Record_Date", "Customer_Name", "PAN", "Mobile"]
            actual_start = [c for c in start if c in cols]
            remaining = [c for c in cols if c not in actual_start]
            return df[actual_start + remaining]

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

        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            # 1. Leads (Use reorder to keep ALL cols)
            pd.DataFrame(all_leads if all_leads else []).pipe(reorder).to_excel(
                writer, sheet_name="Leads", index=False
            )

            # 2. Analysis
            pd.DataFrame(all_analysis if all_analysis else []).pipe(reorder).to_excel(
                writer, sheet_name="Lead_Analysis", index=False
            )

            # 3. Loan Buckets
            buckets = {k: [] for k in SHEET_ORDER if "_Loans" in k or "_Cards" in k}
            for loan in all_loans:
                cat = loan.get("Mapped_Category", "Other_Loans")
                if cat in buckets:
                    buckets[cat].append(loan)
                else:
                    buckets["Other_Loans"].append(loan)

            for cat in buckets:
                if buckets[cat]:
                    pd.DataFrame(buckets[cat]).pipe(reorder).to_excel(
                        writer, sheet_name=cat, index=False
                    )

            pd.DataFrame(all_loans if all_loans else []).pipe(reorder).to_excel(
                writer, sheet_name="All_Tradelines", index=False
            )
            pd.DataFrame(all_enqs if all_enqs else []).pipe(reorder).to_excel(
                writer, sheet_name="Enquiries", index=False
            )

        return {
            "status": "Completed",
            "file_path": file_path,
            "total_rows": len(all_leads),
        }
    except Exception as e:
        traceback.print_exc()
        return {"status": "Failed", "error": str(e)}
