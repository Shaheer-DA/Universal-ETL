import os

import pandas as pd


class EnrichmentEngine:
    # 1. ACCOUNT MAP (Same as before)
    ACCOUNT_TYPE_MAP = {
        "housing loan": "Home_Loans",
        "overdraft": "Home_Loans",
        "property loan": "Home_Loans",
        "microfinance housing loan": "Home_Loans",
        "pradhan mantri awas yojana - credit link subsidy scheme may clss": "Home_Loans",
        "pm awas yojana - clss": "Home_Loans",
        "pradhan mantri awas yojana - clss": "Home_Loans",
        "auto loan": "Auto_Loans",
        "used car loan": "Auto_Loans",
        "commercial vehicle loan": "Auto_Loans",
        "two-wheeler loan": "Auto_Loans",
        "tractor loan": "Auto_Loans",
        "education loan": "Education_Loans",
        "educational loan": "Education_Loans",
        "gold loan": "Gold_Loans",
        "priity sect- gold loan [secured]": "Gold_Loans",
        "priority sector gold loan": "Gold_Loans",
        "personal loan": "Personal_Loans",
        "sht term personal loan [unsecured]": "Personal_Loans",
        "short term personal loan": "Personal_Loans",
        "loan on credit card": "Personal_Loans",
        "p2p personal loan": "Personal_Loans",
        "microfinance personal loan": "Personal_Loans",
        "loan to professional": "Personal_Loans",
        "consumer loan": "Personal_Loans",
        "credit card": "Credit_Cards",
        "kisan credit card": "Credit_Cards",
        "secured credit card": "Credit_Cards",
        "cpate credit card": "Credit_Cards",
        "corporate credit card": "Credit_Cards",
        "fleet card": "Credit_Cards",
        "business loan": "Business_Loans",
        "business loan - priity sect- others": "Business_Loans",
        "loan against bank deposits": "Business_Loans",
        "business loan - priity sect- small business": "Business_Loans",
        "business loan - unsecured": "Business_Loans",
        "business loan - priity sect- agriculture": "Business_Loans",
        "microfinance business loan": "Business_Loans",
        "loan against shares/securities": "Business_Loans",
        "business loan - secured": "Business_Loans",
        "mudra loans - shishu / kishor / tarun": "Business_Loans",
        "gecl loan secured": "Business_Loans",
        "gecl loan unsecured": "Business_Loans",
        "other account types": "Other_Loans",
        "other": "Other_Loans",
        "others": "Other_Loans",
    }

    ZONE_MAP = {
        "Delhi": "North",
        "Haryana": "North",
        "Punjab": "North",
        "Himachal Pradesh": "North",
        "Jammu & Kashmir": "North",
        "Uttarakhand": "North",
        "Uttar Pradesh": "North",
        "Rajasthan": "North",
        "Chandigarh": "North",
        "Ladakh": "North",
        "Maharashtra": "West",
        "Gujarat": "West",
        "Goa": "West",
        "Dadra and Nagar Haveli": "West",
        "Daman and Diu": "West",
        "Karnataka": "South",
        "Tamil Nadu": "South",
        "Kerala": "South",
        "Telangana": "South",
        "Andhra Pradesh": "South",
        "Puducherry": "South",
        "Lakshadweep": "South",
        "West Bengal": "East",
        "Odisha": "East",
        "Bihar": "East",
        "Jharkhand": "East",
        "Sikkim": "East",
        "Assam": "North-East",
        "Arunachal Pradesh": "North-East",
        "Manipur": "North-East",
        "Meghalaya": "North-East",
        "Mizoram": "North-East",
        "Nagaland": "North-East",
        "Tripura": "North-East",
        "Madhya Pradesh": "Central",
        "Chhattisgarh": "Central",
    }

    _pincode_cache = {}
    _employee_cache = set()
    _is_loaded = False

    @classmethod
    def load_masters(cls):
        """Loads Pincode and Employee Data"""
        if cls._is_loaded:
            return
        try:
            # 1. PINCODES
            csv_path = os.path.join("data", "pincode_master.csv")
            xlsx_path = os.path.join("data", "pincode_master.xlsx")
            df = None
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path, dtype={"Pincode": str})
            elif os.path.exists(xlsx_path):
                df = pd.read_excel(xlsx_path, dtype={"Pincode": str})

            if df is not None:
                df.columns = [c.strip() for c in df.columns]
                for _, row in df.iterrows():
                    pc = str(row.get("Pincode", "")).split(".")[0].strip()
                    cls._pincode_cache[pc] = {
                        "City": row.get("City", ""),
                        "State": row.get("State", ""),
                    }
                print(f"✅ Loaded {len(cls._pincode_cache)} pincodes.")

            # 2. EMPLOYEES
            emp_path = os.path.join(
                "data", "employee_master.xlsx"
            )  # Expecting this file
            if os.path.exists(emp_path):
                df_emp = pd.read_excel(emp_path, dtype=str)
                # Assume column is 'Mobile'
                if "Mobile" in df_emp.columns:
                    cls._employee_cache = set(
                        df_emp["Mobile"]
                        .dropna()
                        .str.strip()
                        .str.replace(r"\.0$", "", regex=True)
                    )
                    print(f"✅ Loaded {len(cls._employee_cache)} employees.")
                else:
                    print("⚠️ 'Mobile' column not found in employee_master.xlsx")

            cls._is_loaded = True
        except Exception as e:
            print(f"Master Load Error: {e}")

    @classmethod
    def get_location(cls, pincode):
        if not cls._is_loaded:
            cls.load_masters()
        pc = str(pincode).split(".")[0].strip()
        data = cls._pincode_cache.get(pc, {})
        city = data.get("City", "")
        state = data.get("State", "")
        zone = cls.ZONE_MAP.get(state, "Unknown")
        return city, state, zone

    @classmethod
    def is_employee(cls, mobile):
        if not cls._is_loaded:
            cls.load_masters()
        if not mobile:
            return False
        mob = str(mobile).strip().replace(".0", "")
        return mob in cls._employee_cache

    @classmethod
    def get_standard_category(cls, raw_type):
        if not raw_type:
            return "Other_Loans"
        key = str(raw_type).lower().strip()
        if key in cls.ACCOUNT_TYPE_MAP:
            return cls.ACCOUNT_TYPE_MAP[key]
        if "housing" in key or "home" in key or "property" in key or "awas" in key:
            return "Home_Loans"
        if "education" in key:
            return "Education_Loans"
        if "credit card" in key or "card" in key:
            return "Credit_Cards"
        if (
            "auto" in key
            or "car" in key
            or "vehicle" in key
            or "tractor" in key
            or "wheeler" in key
        ):
            return "Auto_Loans"
        if "business" in key or "mudra" in key or "gecl" in key:
            return "Business_Loans"
        if "personal" in key or "consumer" in key:
            return "Personal_Loans"
        if "gold" in key:
            return "Gold_Loans"
        return "Other_Loans"

    @staticmethod
    def get_score_band(score):
        try:
            s = float(score)
        except:
            return "Zero/No Score"
        if s >= 750:
            return "750+"
        if s >= 700:
            return "700-749"
        if s >= 650:
            return "650-699"
        if s >= 300:
            return "300-649"
        return "Zero/No Score"
