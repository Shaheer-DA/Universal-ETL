import asyncio
import os
import traceback
from datetime import datetime

import aiohttp
import pandas as pd
from celery import Celery

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

    # NOTE: No broad try/except here so failures propagate to Celery state correctly

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

    # 3. PROCESSING
    self.update_state(
        state="PROGRESS",
        meta={"status": "Filtering Employees & Dupes...", "progress": 80},
    )

    # A. Segregate Employees
    customers_leads = []
    employees_leads = []
    emp_ids = set()

    for lead in all_leads:
        if EnrichmentEngine.is_employee(lead.get("Mobile")):
            employees_leads.append(lead)
            emp_ids.add(lead["Customer_ID"])
        else:
            customers_leads.append(lead)

    def split_list(full_list, target_ids):
        in_set = [x for x in full_list if x["Customer_ID"] in target_ids]
        out_set = [x for x in full_list if x["Customer_ID"] not in target_ids]
        return in_set, out_set

    # Separate Emp Data from Customer Data
    emp_loans, cust_loans = split_list(all_loans, emp_ids)
    emp_enqs, cust_enqs = split_list(all_enqs, emp_ids)
    emp_an, cust_an = split_list(all_analysis, emp_ids)

    # B. Deduplicate Customers
    def process_df(data_list, unique_keys, date_field_for_month=None):
        if not data_list:
            return pd.DataFrame(), pd.DataFrame()
        df = pd.DataFrame(data_list)
        if "Record_Date" in df.columns:
            df["Record_Month"] = pd.to_datetime(
                df["Record_Date"], errors="coerce"
            ).dt.strftime("%b-%Y")
        if date_field_for_month and date_field_for_month in df.columns:
            df["Loan_Start_Month"] = pd.to_datetime(
                df[date_field_for_month], errors="coerce"
            ).dt.strftime("%b-%Y")

        if unique_keys:
            valid_keys = [k for k in unique_keys if k in df.columns]
            if valid_keys:
                duplicates = df[df.duplicated(subset=valid_keys, keep="first")]
                clean = df.drop_duplicates(subset=valid_keys, keep="first")
                return clean, duplicates
        return df, pd.DataFrame()

    # Clean Customers
    df_leads_clean, df_leads_dupe = process_df(customers_leads, ["PAN", "Mobile"])

    # Sub-sheets for CLEAN customers
    valid_cust_ids = (
        set(df_leads_clean["Customer_ID"]) if not df_leads_clean.empty else set()
    )

    loans_clean_list = [x for x in cust_loans if x["Customer_ID"] in valid_cust_ids]
    enqs_clean_list = [x for x in cust_enqs if x["Customer_ID"] in valid_cust_ids]
    an_clean_list = [x for x in cust_an if x["Customer_ID"] in valid_cust_ids]

    df_loans_clean, _ = process_df(
        loans_clean_list, [], date_field_for_month="Date_Opened"
    )
    df_enqs_clean, _ = process_df(enqs_clean_list, [])
    df_an_clean, _ = process_df(an_clean_list, [])

    # C. PREPARE DUPLICATES / EXCLUDED DATA (FIXED LOGIC)
    # Get IDs of duplicate leads
    dupe_ids = set(df_leads_dupe["Customer_ID"]) if not df_leads_dupe.empty else set()

    # Filter lists for Duplicate leads (from the non-employee bucket)
    loans_dupe_list = [x for x in cust_loans if x["Customer_ID"] in dupe_ids]
    enqs_dupe_list = [x for x in cust_enqs if x["Customer_ID"] in dupe_ids]
    an_dupe_list = [x for x in cust_an if x["Customer_ID"] in dupe_ids]

    # Create DFs for Duplicates (FIX: These were missing before)
    df_loans_dupe, _ = process_df(
        loans_dupe_list, [], date_field_for_month="Date_Opened"
    )
    df_enqs_dupe, _ = process_df(enqs_dupe_list, [])
    df_an_dupe, _ = process_df(an_dupe_list, [])

    # 4. EXCEL WRITING
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    clean_filename = f"{db_name}_{timestamp}_Clean.xlsx"
    dupe_filename = f"{db_name}_{timestamp}_Excluded.xlsx"
    clean_path = os.path.join("output", clean_filename)
    dupe_path = os.path.join("output", dupe_filename)

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

        if target_cols:
            return df[[c for c in target_cols if c in cols]]

        used = set(start_c)
        middle = [c for c in cols if c not in used and c != "Source_DB"]
        end = ["Source_DB"] if "Source_DB" in cols else []
        return df[actual_start + middle + end]

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

    def write_file(path, d_l, d_a, d_lo, d_e, emp_df=None):
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            d_l.pipe(strict_format, target_cols=lead_order).to_excel(
                writer, sheet_name="Leads", index=False
            )

            if emp_df is not None and not emp_df.empty:
                emp_df.pipe(strict_format, target_cols=lead_order).to_excel(
                    writer, sheet_name="Internal_Employees", index=False
                )

            d_a.pipe(strict_format).to_excel(
                writer, sheet_name="Lead_Analysis", index=False
            )

            buckets = {k: [] for k in SHEET_ORDER if "_Loans" in k or "_Cards" in k}
            if not d_lo.empty:
                for _, row in d_lo.iterrows():
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

            d_lo.pipe(strict_format).to_excel(
                writer, sheet_name="All_Tradelines", index=False
            )
            d_e.pipe(strict_format).to_excel(
                writer, sheet_name="Enquiries", index=False
            )

    write_file(clean_path, df_leads_clean, df_an_clean, df_loans_clean, df_enqs_clean)

    # Prepare Employee DF for Excluded file
    df_emp = pd.DataFrame(employees_leads)
    if not df_emp.empty and "Record_Date" in df_emp.columns:
        df_emp["Record_Month"] = pd.to_datetime(
            df_emp["Record_Date"], errors="coerce"
        ).dt.strftime("%b-%Y")

    has_dupes = not df_leads_dupe.empty
    has_emps = not df_emp.empty

    if has_dupes or has_emps:
        write_file(
            dupe_path,
            df_leads_dupe,
            df_an_dupe,
            df_loans_dupe,
            df_enqs_dupe,
            emp_df=df_emp,
        )
        return {
            "status": "Completed",
            "file_path": clean_path,
            "dupe_path": dupe_path,
            "total_rows": len(df_leads_clean),
            "total_fetched": total_fetched,
            "employees_found": len(employees_leads),
        }

    return {
        "status": "Completed",
        "file_path": clean_path,
        "total_rows": len(df_leads_clean),
        "total_fetched": total_fetched,
        "employees_found": 0,
    }
