""" 
s2c_ppc_optimizer_v2_5.py — Final Production Version (Aslam + GPT-5)
Includes:
 - Mutual exclusivity (Promote/Bid vs Negatives)
 - ROAS ceiling cap for Watch Negatives
 - Hardened NEGATIVES_TO_APPLY logic (Promoted → Exact → NEG_EXACT)
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime

CONFIG = {
    "CLICK_THRESHOLD": 10,
    "SPEND_MULTIPLIER": 3,
    "IMPRESSION_THRESHOLD": 250,
    "ALPHA_EXACT_PT": 0.20,
    "ALPHA_BROAD_PHRASE": 0.15,
    "MAX_BID_CHANGE": 0.15,
    "OUTPUT_DIR": "./outputs",
    "DEBUG_MODE": False,
    "MIN_VALIDATION_CLICKS": 5,
    "NEGATIVE_DELAY_CYCLES": 1,
    "ROAS_WATCH_CEILING": 3.0,
    "PROTECT_ROAS_HIGH": 2.0,
}

def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(r"[^\d\.\-]", "", regex=True),
                         errors="coerce").fillna(0)

def extract_asin(text: str):
    if not isinstance(text, str): return None
    m = re.search(r"\\b(B0[A-Z0-9]{8})\\b", text.upper())
    return m.group(1) if m else None

def compute_ctr(impr, clicks): return (clicks / impr) if (impr and impr > 0) else 0.0
def compute_cvr(clicks, orders): return (orders / clicks) if (clicks and clicks > 0) else 0.0

def run_phase1(df: pd.DataFrame):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # --- Discovery Filter ---
    auto_pattern = r'close-match|loose-match|substitutes|complements|category=|asin|b0[a-z0-9]{8}'
    discovery_mask = (~df["Match Type"].astype(str).str.contains("exact", case=False, na=False)) | \
                     (df["Targeting"].astype(str).str.contains(auto_pattern, case=False, na=False))
    discovery_df = df[discovery_mask].copy()

    avg_cpc = discovery_df.get("Cost Per Click (CPC)", pd.Series()).replace(0, np.nan).mean()
    avg_cpc = 0.1 if np.isnan(avg_cpc) or avg_cpc <= 0 else avg_cpc

    readable_mask = (
        (discovery_df["Clicks"] >= CONFIG["CLICK_THRESHOLD"]) &
        (discovery_df["Spend"] >= CONFIG["SPEND_MULTIPLIER"] * avg_cpc) &
        (discovery_df["Impressions"] >= CONFIG["IMPRESSION_THRESHOLD"])
    )
    readable_df = discovery_df[readable_mask].copy()

    readable_df["CTR"] = np.where(readable_df["Impressions"] > 0, readable_df["Clicks"] / readable_df["Impressions"], 0)
    readable_df["CVR"] = np.where(readable_df["Clicks"] > 0, readable_df["7 Day Total Orders (#)"] / readable_df["Clicks"], 0)
    readable_df["ROAS"] = np.where(readable_df["Spend"] > 0, readable_df["7 Day Total Sales"] / readable_df["Spend"], 0)
    readable_df["Campaign Median ROAS"] = readable_df.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.1)

    def classify(row):
        median = row["Campaign Median ROAS"]
        if row["ROAS"] >= 1.2 * median:
            return "Promote"
        if row["ROAS"] < 0.8 * median:
            return "Bid Down / Review"
        return "Stable"

    readable_df["Action"] = readable_df.apply(classify, axis=1)
    promote_df = readable_df[readable_df["Action"] == "Promote"].copy()
    non_promote = readable_df[readable_df["Action"] != "Promote"].copy()

    # --- NEGATIVES: confirm/watch classification ---
    camp_stats = non_promote.groupby("Campaign Name").agg({
        "ROAS": ["median", "std"], "CTR": ["median", "std"], "CVR": ["median", "std"]
    }).reset_index()
    camp_stats.columns = ["Campaign Name", "ROAS_med", "ROAS_std", "CTR_med", "CTR_std", "CVR_med", "CVR_std"]
    non_promote = non_promote.merge(camp_stats, on="Campaign Name", how="left")

    def neg_decision(row):
        roas, ctr, cvr = row["ROAS"], row["CTR"], row["CVR"]
        med_r, med_ctr, med_cvr = row["ROAS_med"], row["CTR_med"], row["CVR_med"]
        std_ctr, std_cvr = row["CTR_std"], row["CVR_std"]
        spend, orders = row["Spend"], row["7 Day Total Orders (#)"]
        if roas >= CONFIG["PROTECT_ROAS_HIGH"] and orders >= 1:
            return "Keep"
        if spend >= 3 * avg_cpc and ((roas < 0.3 * med_r) or (roas < 0.5)) and (ctr < (med_ctr - std_ctr) or cvr < (med_cvr - std_cvr)):
            return "Confirm Negative"
        if spend >= 3 * avg_cpc and ((roas < 0.8 * med_r) or (ctr < med_ctr) or (cvr < med_cvr)):
            if roas >= CONFIG["ROAS_WATCH_CEILING"]:
                return "Keep"
            return "Watch Negative"
        return "Keep"

    non_promote["Neg_Action"] = non_promote.apply(neg_decision, axis=1)

    watch_df = non_promote[non_promote["Neg_Action"] == "Watch Negative"].copy()
    confirm_df = non_promote[non_promote["Neg_Action"] == "Confirm Negative"].copy()

    # --- Negatives_To_Apply (Promote→Exact→NEG_EXACT) ---
    neg_rows = []
    for _, r in promote_df.iterrows():
        term = str(r["Customer Search Term"]).strip()
        srcs = discovery_df[discovery_df["Customer Search Term"].astype(str).str.strip().str.lower() == term.lower()]["Campaign Name"].unique().tolist()
        if not srcs: srcs = [r["Campaign Name"]]
        for sc in srcs:
            neg_rows.append({
                "SourceCampaign": sc,
                "NegativeType": "NEG_EXACT",
                "TermOrASIN": term,
                "Reason": "Promoted->Exact",
                "Status": "Pending",
                "ValidationClicksRequired": CONFIG["MIN_VALIDATION_CLICKS"],
                "DelayCycles": CONFIG["NEGATIVE_DELAY_CYCLES"]
            })
    negatives_df = pd.DataFrame(neg_rows).drop_duplicates(subset=["SourceCampaign", "TermOrASIN"])

    promote_df.attrs["Negatives_To_Apply"] = negatives_df
    promote_df.attrs["Watch_Negatives"] = watch_df
    promote_df.attrs["Confirm_Negatives"] = confirm_df

    return promote_df, readable_df

if __name__ == "__main__":
    file_path = "Sponsored_Products_Search_term_report.xlsx"
    df_input = pd.read_excel(file_path)
    promote, readable = run_phase1(df_input)

    negs = promote.attrs["Negatives_To_Apply"]
    watch = promote.attrs["Watch_Negatives"]
    confirm = promote.attrs["Confirm_Negatives"]

    Path(CONFIG["OUTPUT_DIR"]).mkdir(exist_ok=True)
    out_file = f"{CONFIG['OUTPUT_DIR']}/s2c_ppc_optimizer_v2_5_output_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
        promote.to_excel(writer, sheet_name="Promote", index=False)
        readable.to_excel(writer, sheet_name="Readable", index=False)
        negs.to_excel(writer, sheet_name="Negatives_To_Apply", index=False)
        watch.to_excel(writer, sheet_name="Watch_Negatives", index=False)
        confirm.to_excel(writer, sheet_name="Confirm_Negatives", index=False)

    print(f"✅ Run completed. Output saved to {out_file}")
