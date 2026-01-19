import json
import re

from app.enrichment_engine import EnrichmentEngine


class ParserEngine:
    @staticmethod
    def identify_format(json_data):
        if isinstance(json_data, str):
            try:
                json_data = json.loads(json_data)
            except:
                return "INVALID_JSON"
        if isinstance(json_data, list) and len(json_data) > 0:
            return ParserEngine.identify_format(json_data[0])
        data_str = str(json_data).lower()
        if "xmljsonresponse" in data_str:
            return "EXPERIAN_RAW"
        if "cibildata" in data_str and "getcustomerassetsresponse" in data_str:
            return "TRUSTELL_CIBIL_RAW"
        if "reportdata" in data_str and "reportsummary" in data_str:
            return "CPL_TRUSTELL_CIBIL"
        if "creditanalysis" in data_str and "personalloans" in data_str:
            return "EXPERIAN_INTERNAL"
        return "UNKNOWN_FALLBACK"

    @staticmethod
    def parse(json_data, customer_id):
        try:
            if isinstance(json_data, str):
                try:
                    json_data = json.loads(json_data)
                except:
                    return None, None, None
            if not json_data:
                return None, None, None
            if isinstance(json_data, list):
                all_l, all_lo, all_e = [], [], []
                for item in json_data:
                    l, lo, e = ParserEngine.parse(item, customer_id)
                    if l:
                        all_l.append(l)
                        all_lo.extend(lo)
                        all_e.extend(e)
                return (all_l[0] if all_l else None), all_lo, all_e

            fmt = ParserEngine.identify_format(json_data)
            if fmt == "EXPERIAN_RAW":
                lead, loans, enqs = ParserEngine._parse_experian_raw(
                    json_data, customer_id
                )
            elif fmt == "TRUSTELL_CIBIL_RAW":
                lead, loans, enqs = ParserEngine._parse_trustell_cibil_raw(
                    json_data, customer_id
                )
            elif fmt == "CPL_TRUSTELL_CIBIL":
                lead, loans, enqs = ParserEngine._parse_cpl_trustell(
                    json_data, customer_id
                )
            elif fmt == "EXPERIAN_INTERNAL":
                lead, loans, enqs = ParserEngine._parse_experian_internal(
                    json_data, customer_id
                )
            else:
                lead, loans, enqs = ParserEngine._parse_universal_fallback(
                    json_data, customer_id
                )

            if lead:
                pc = lead.get("Pincode")
                city, state, zone = EnrichmentEngine.get_location(pc)
                lead["City_Mapped"] = city
                lead["State_Mapped"] = state
                lead["Zone_Mapped"] = zone
                score = lead.get("CIBIL_Score", 0)
                lead["CIBIL_Band"] = EnrichmentEngine.get_score_band(score)
                common = {
                    "Customer_Name": lead.get("Full_Name"),
                    "PAN": lead.get("PAN"),
                    "Mobile": lead.get("Mobile"),
                }
                for l in loans:
                    l.update(common)
                    l["Mapped_Category"] = EnrichmentEngine.get_standard_category(
                        l.get("Account_Type_Category")
                    )
                for e in enqs:
                    e.update(common)
            return lead, loans, enqs
        except Exception as e:
            print(f"PARSER ERROR on {customer_id}: {str(e)}")
            return None, None, None

    @staticmethod
    def _parse_experian_raw(data, cid):
        def safe(obj, *keys):
            for k in keys:
                if isinstance(obj, dict):
                    obj = obj.get(k)
                else:
                    return None
            return obj

        root = data.get("xmlJsonResponse", {})
        app_details = (
            safe(root, "currentApplication", "currentApplicationDetails") or {}
        )
        applicant = safe(app_details, "currentApplicantdetails") or {}
        other = safe(app_details, "currentOtherDetails") or {}
        addr = safe(app_details, "currentApplicantAddressDetails")
        if isinstance(addr, list) and len(addr) > 0:
            addr = addr[0]
        if not isinstance(addr, dict):
            addr = {}
        cais = safe(root, "caisAccount", "caisSummary") or {}

        lead_info = {
            "Customer_ID": cid,
            "Full_Name": f"{applicant.get('firstName', '')} {applicant.get('lastName', '')}".strip(),
            "PAN": applicant.get("incomeTaxPan"),
            "Mobile": applicant.get("mobilePhoneNumber"),
            "Email": applicant.get("emailId"),
            "Gender": applicant.get("genderCode"),
            "DOB": applicant.get("dateOfBirthApplicant"),
            "Employment_Type": other.get("employmentStatus"),
            "Income": _clean_numeric(other.get("income")),
            "Address": _format_address(addr),
            "Pincode": addr.get("pinCode"),
            "CIBIL_Score": _clean_numeric(safe(root, "score", "bureauScore")),
            # STATS
            "Total_Accounts": _clean_numeric(
                safe(cais, "creditAccount", "creditAccountTotal")
            ),
            "Active_Accounts": _clean_numeric(
                safe(cais, "creditAccount", "creditAccountActive")
            ),
            "Closed_Accounts": _clean_numeric(
                safe(cais, "creditAccount", "creditAccountClosed")
            ),
            "Total_Outstanding": _clean_numeric(
                safe(cais, "totalOutStandingBalance", "outstandingBalanceAll")
            ),
            "Total_Sanctioned": 0.0,
            "Total_EMI": 0.0,
            "Total_Past_Due": 0.0,  # Calculated via loop
            "Recent_Enquiries_30_Days": _clean_numeric(
                safe(root, "caps", "capsSummary", "capsLast30Days")
            ),
            "Source": "Experian_Raw",
        }

        loans_list = []
        cais_list = safe(root, "caisAccount", "caisAccountDetails") or []
        if isinstance(cais_list, dict):
            cais_list = [cais_list]
        for item in cais_list:
            if not isinstance(item, dict):
                continue
            sanc = _clean_numeric(item.get("highestCreditOrOrignalLoanAmount"))
            emi = _clean_numeric(item.get("scheduledMonthlyPaymentAmount"))
            pdue = _clean_numeric(item.get("amountPastDue"))
            lead_info["Total_Sanctioned"] += sanc
            lead_info["Total_EMI"] += emi
            lead_info["Total_Past_Due"] += pdue

            loans_list.append(
                {
                    "Customer_ID": cid,
                    "Account_Type_Category": item.get("accountType"),
                    "Bank_Name": item.get("subscriberName"),
                    "Account_Number": item.get("accountNumber"),
                    "Status": item.get("accountStatus"),
                    "Sanctioned_Amount": sanc,
                    "Current_Balance": _clean_numeric(item.get("currentBalance")),
                    "EMI_Amount": emi,
                    "Date_Opened": item.get("openDate"),
                    "Date_Closed": item.get("dateClosed"),
                    "Date_Reported": item.get("dateReported"),
                    "Rate_Of_Interest": item.get("rateOfInterest"),
                    "Repayment_Tenure": item.get("repaymentTenure"),
                    "Overdue_Amount": pdue,
                    "Write_Off_Amount": _clean_numeric(item.get("writtenOffAmtTotal")),
                    "Payment_History_String": item.get("paymentHistoryProfile"),
                }
            )

        enquiries_list = []
        caps_list = safe(root, "caps", "capsApplicationDetailList") or []
        if isinstance(caps_list, dict):
            caps_list = [caps_list]
        for item in caps_list:
            if not isinstance(item, dict):
                continue
            enquiries_list.append(
                {
                    "Customer_ID": cid,
                    "Date": item.get("dateOfRequest"),
                    "Lender": item.get("subscriberName"),
                    "Amount": _clean_numeric(item.get("amountFinanced")),
                    "Purpose": item.get("financePurpose"),
                }
            )
        return lead_info, loans_list, enquiries_list

    @staticmethod
    def _parse_cpl_trustell(data, cid):
        root = data.get("data", {}).get("reportData")
        if not root:
            root = data.get("reportData")
        if not root:
            root = data
        if "ccrResponse" in root:
            root = root.get("ccrResponse", {})
        lead_info = {
            "Customer_ID": cid,
            "Full_Name": _get_path(root, "reportSummary.personalDetails.fullName"),
            "PAN": _get_path(root, "reportSummary.personalDetails.pan"),
            "Mobile": _get_path(root, "reportSummary.personalDetails.mobile"),
            "Email": _get_path(root, "reportSummary.personalDetails.email"),
            "Gender": _get_path(root, "reportSummary.personalDetails.gender"),
            "DOB": _get_path(root, "reportSummary.personalDetails.dateOfBirth"),
            "Address": _get_path(root, "reportSummary.personalDetails.address"),
            "Pincode": _get_path(root, "reportSummary.personalDetails.pincode"),
            "Employment_Type": _get_path(
                root, "reportSummary.personalDetails.occupation"
            ),
            "Income": _clean_numeric(
                _get_path(root, "reportSummary.personalDetails.totalIncome")
            ),
            "CIBIL_Score": _clean_numeric(
                _get_path(root, "reportSummary.creditScore.score")
            ),
            "Total_Accounts": _clean_numeric(
                _get_path(root, "reportSummary.accountSummary.totalAccounts")
            ),
            "Active_Accounts": _clean_numeric(
                _get_path(root, "reportSummary.accountSummary.openAccounts")
            ),
            "Closed_Accounts": _clean_numeric(
                _get_path(root, "reportSummary.accountSummary.zeroBalanceAccounts")
            ),
            "Total_Outstanding": _clean_numeric(
                _get_path(root, "reportSummary.accountSummary.totalBalanceAmount")
            ),
            "Total_Sanctioned": _clean_numeric(
                _get_path(root, "reportSummary.accountSummary.totalSanctionedAmount")
            ),
            "Total_EMI": _clean_numeric(
                _get_path(
                    root, "reportSummary.accountSummary.totalMonthlyPaymentAmount"
                )
            ),
            "Total_Past_Due": _clean_numeric(
                _get_path(root, "reportSummary.accountSummary.totalPastDueAmount")
            ),
            "Recent_Enquiries_30_Days": _get_path(
                root, "creditAnalysis.enquiries.summary.last30Days"
            )
            or 0,
            "Source": "CPL_Trustell_Mapped",
        }
        loans_list = []
        ca = root.get("creditAnalysis", {})

        def extract(n):
            return {
                "Customer_ID": cid,
                "Account_Type_Category": n.get("accountType", "Other"),
                "Bank_Name": n.get("provider"),
                "Account_Number": n.get("accountNumber"),
                "Status": n.get("accountStatus"),
                "Sanctioned_Amount": _clean_numeric(n.get("sanctionedAmount")),
                "Current_Balance": _clean_numeric(n.get("outstanding")),
                "EMI_Amount": _clean_numeric(n.get("emi")),
                "Date_Opened": n.get("accountOpenDate"),
                "Date_Closed": n.get("accountCloseDate"),
                "Date_Reported": n.get("dateReported"),
                "Rate_Of_Interest": str(n.get("rateOfInterest", "")),
                "Repayment_Tenure": str(n.get("repaymentTenure", "")),
                "Overdue_Amount": _clean_numeric(n.get("accountPastDueAmount")),
                "Write_Off_Amount": _clean_numeric(n.get("writtenOffAmtTotal")),
                "Payment_History_String": _get_payment_history(n),
            }

        for cc in ca.get("creditCards", []) or []:
            loans_list.append(extract(cc))
        ld = ca.get("loans", {})
        if isinstance(ld, dict):
            for k, items in ld.items():
                if isinstance(items, list):
                    for i in items:
                        loans_list.append(extract(i))
        for ol in ca.get("otherLoans", []) or []:
            loans_list.append(extract(ol))
        enq_list = []
        for e in _get_nested(ca, ["enquiries", "recent"]) or []:
            enq_list.append(
                {
                    "Customer_ID": cid,
                    "Date": e.get("date"),
                    "Lender": e.get("lender"),
                    "Amount": _clean_numeric(e.get("amount")),
                    "Purpose": e.get("purpose"),
                }
            )
        return lead_info, loans_list, enq_list

    @staticmethod
    def _parse_trustell_cibil_raw(data, cid):
        base = _get_path(
            data,
            "data.cibilData.GetCustomerAssetsResponse.GetCustomerAssetsSuccess.Asset.TrueLinkCreditReport",
        )
        if not base:
            return {"Customer_ID": cid, "Source": "Trustell_Error"}, [], []
        borrower = base.get("Borrower", {})
        addr = _get_nested(borrower, ["BorrowerAddress"])
        if isinstance(addr, list) and len(addr) > 0:
            addr = addr[0].get("CreditAddress", {})
        lead_info = {
            "Customer_ID": cid,
            "Full_Name": _get_nested(borrower, ["BorrowerName", "Name", "Forename"]),
            "PAN": (
                _get_nested(borrower, ["IdentifierPartition", 0, "ID", "Id"])
                if "IdentifierPartition" in borrower
                else ""
            ),
            "Mobile": _get_nested(
                borrower, ["BorrowerTelephone", 0, "PhoneNumber", "Number"]
            ),
            "Pincode": addr.get("PostalCode"),
            "Income": _clean_numeric(borrower.get("TotalIncome")),
            "CIBIL_Score": _clean_numeric(
                _get_nested(borrower, ["CreditScore", "riskScore"])
            ),
            "Source": "Trustell_CIBIL_Raw_Mapped",
        }
        loans_list = []
        for item in base.get("TradeLinePartition", []) or []:
            tl = item.get("TradeLine", {})
            granted = tl.get("GrantedTrade", {})
            loans_list.append(
                {
                    "Customer_ID": cid,
                    "Account_Type_Category": str(tl.get("accountTypeDescription", "")),
                    "Bank_Name": tl.get("creditorName"),
                    "Account_Number": tl.get("accountNumber"),
                    "Status": _get_nested(tl, ["OpenClosed", "symbol"]),
                    "Sanctioned_Amount": _clean_numeric(tl.get("highBalance")),
                    "Current_Balance": _clean_numeric(tl.get("currentBalance")),
                    "EMI_Amount": _clean_numeric(granted.get("EMIAmount")),
                    "Date_Opened": tl.get("dateOpened"),
                    "Date_Closed": tl.get("dateClosed"),
                    "Date_Reported": tl.get("dateReported"),
                    "Payment_History_String": _get_nested(
                        granted, ["PayStatusHistory", "status"]
                    ),
                }
            )
        enq_list = []
        for item in base.get("InquiryPartition", []) or []:
            inq = item.get("Inquiry", {})
            enq_list.append(
                {
                    "Customer_ID": cid,
                    "Date": inq.get("inquiryDate"),
                    "Lender": inq.get("subscriberName"),
                    "Amount": _clean_numeric(inq.get("amount")),
                    "Purpose": inq.get("inquiryType"),
                }
            )
        return lead_info, loans_list, enq_list

    @staticmethod
    def _parse_experian_internal(data, cid):
        lead_info = {
            "Customer_ID": cid,
            "Full_Name": _get_path(
                data, "data.reportData.reportSummary.personalDetails.fullName"
            ),
            "PAN": _get_path(data, "data.reportData.reportSummary.personalDetails.pan"),
            "Pincode": _get_path(
                data, "data.reportData.reportSummary.personalDetails.pincode"
            ),
            "CIBIL_Score": _clean_numeric(
                _get_path(data, "data.reportData.reportSummary.creditScore.score")
            ),
            "Source": "Experian_Internal_Mapped",
        }
        loans_list = []

        def extract(n):
            return {
                "Customer_ID": cid,
                "Account_Type_Category": n.get("accountType", "Other"),
                "Bank_Name": n.get("provider"),
                "Sanctioned_Amount": _clean_numeric(n.get("sanctionedAmount")),
                "Current_Balance": _clean_numeric(n.get("outstanding")),
                "EMI_Amount": _clean_numeric(n.get("emi")),
                "Status": n.get("accountStatus"),
            }

        for cc in _get_path(data, "data.reportData.creditAnalysis.creditCards") or []:
            loans_list.append(extract(cc))
        ld = _get_path(data, "data.reportData.creditAnalysis.loans") or {}
        if isinstance(ld, dict):
            for k, items in ld.items():
                if isinstance(items, list):
                    for i in items:
                        loans_list.append(extract(i))
        return lead_info, loans_list, []

    @staticmethod
    def _parse_universal_fallback(data, cid):
        return {"Customer_ID": cid, "Source": "Unknown"}, [], []


def _clean_numeric(value):
    if value is None:
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    clean = re.sub(r"[^\d.]", "", s)
    try:
        return float(clean)
    except:
        return 0.0


def _format_address(addr_obj):
    if not isinstance(addr_obj, dict):
        return str(addr_obj)
    parts = [
        addr_obj.get("flatNoPlotNoHouseNo", ""),
        addr_obj.get("bldgNumberSocietyName", ""),
        addr_obj.get("roadNumberNameAreaLocality", ""),
        addr_obj.get("city", ""),
        addr_obj.get("state", ""),
        addr_obj.get("pinCode", ""),
    ]
    return ", ".join([str(p) for p in parts if p])


def _get_path(data, path):
    if not path:
        return None
    path = path.replace("x.", "").replace("xmlJsonResponse.", "")
    keys = path.split(".")
    ref = data
    if keys[0] == "xmlJsonResponse" and "xmlJsonResponse" in data:
        ref = data
    elif keys[0] == "xmlJsonResponse":
        keys.pop(0)
    try:
        for k in keys:
            if "[" in k and "]" in k:
                key_part = k.split("[")[0]
                index = int(k.split("[")[1].replace("]", ""))
                ref = ref.get(key_part, [])
                if isinstance(ref, list) and len(ref) > index:
                    ref = ref[index]
                else:
                    return None
            else:
                ref = ref.get(k)
            if ref is None:
                return None
        return ref
    except:
        return None


def _get_nested(d, keys):
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k)
        elif isinstance(d, list) and isinstance(k, int) and len(d) > k:
            d = d[k]
        else:
            return None
    return d


def _get_payment_history(node):
    ph = node.get("paymentHistory")
    if isinstance(ph, str):
        return ph
    if isinstance(ph, list):
        return ", ".join([str(x.get("status", "")) for x in ph])
    return str(ph)
