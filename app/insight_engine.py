class InsightEngine:
    @staticmethod
    def generate_analytics(lead_info, loans):
        if not lead_info:
            return {}

        categories = {
            "Home_Loans": {"data": []},
            "Auto_Loans": {"data": []},
            "Business_Loans": {"data": []},
            "Personal_Loans": {"data": []},
            "Credit_Cards": {"data": []},
            "Education_Loans": {"data": []},
            "Gold_Loans": {"data": []},
            "Other_Loans": {"data": []},
        }

        for loan in loans:
            cat = loan.get("Mapped_Category", "Other_Loans")
            if cat in categories:
                categories[cat]["data"].append(loan)
            else:
                categories["Other_Loans"]["data"].append(loan)

        row = {
            "Customer_ID": lead_info.get("Customer_ID"),
            "City": lead_info.get("City_Mapped"),
            "State": lead_info.get("State_Mapped"),
            "Zone": lead_info.get("Zone_Mapped"),
            "Total_Active_Loans": 0,
            "Total_EMI_Obligation": 0.0,
        }

        for cat_name, val in categories.items():
            data = val["data"]
            prefix = cat_name.replace("_Loans", "").replace("_Cards", "")
            row[f"{prefix}_Count"] = len(data)

            lenders = []
            sanc = []
            bal = 0.0

            for item in data:
                lenders.append(item.get("Bank_Name", ""))
                sanc.append(str(item.get("Sanctioned_Amount", 0)))
                bal += float(item.get("Current_Balance", 0))
                row["Total_EMI_Obligation"] += float(item.get("EMI_Amount", 0))
                st = str(item.get("Status", "")).lower()
                if "open" in st or "active" in st or "live" in st:
                    row["Total_Active_Loans"] += 1

            row[f"{prefix}_Lenders"] = ", ".join(list(set(lenders)))
            row[f"{prefix}_Sanctioned"] = ", ".join(sanc)
            row[f"{prefix}_Balance"] = bal

        return row
