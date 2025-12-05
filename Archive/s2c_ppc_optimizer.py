"""
s2c_ppc_optimizer.py — S2C PPC Optimizer v2.4
Minor improvements on v2.3:
 - Adds CURRENT BID column in Phase1.5 & Phase2 outputs
 - Adds Phase_Summary sheet with quick run metrics
 - Adds DEBUG_MODE flag for extra console tracing
Author: Aslam + GPT-5 (updated v2.4)
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime

# ==========================================================
# CONFIGURATION
# ==========================================================
CONFIG = {
    "CLICK_THRESHOLD": 10,
    "SPEND_MULTIPLIER": 3,           # Spend >= 3× Avg CPC
    "IMPRESSION_THRESHOLD": 250,     # Relaxed for balanced harvest
    "ALPHA_EXACT_PT": 0.20,
    "ALPHA_BROAD_PHRASE": 0.15,
    "MAX_BID_CHANGE": 0.15,          # ±15% per cycle
    "AUTO_CLUSTER_MIN_ROWS": 10,     # sensitivity for Stage5
    "OUTPUT_DIR": "./outputs",
    "DEBUG_MODE": False,             # set True to print extra diagnostics
    "MIN_VALIDATION_CLICKS": 5,
    "NEGATIVE_DELAY_CYCLES": 1,
}

# ==========================================================
# UTILITIES
# ==========================================================
def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(r"[^\d\.\-]", "", regex=True),
                         errors="coerce").fillna(0)

def extract_asin(text: str) -> str:
    if not isinstance(text, str):
        return None
    m = re.search(r"\b(B0[A-Z0-9]{8})\b", text.upper())
    return m.group(1) if m else None

def normalize_text(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.lower().strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def compute_ctr(impr, clicks):
    return (clicks / impr) if (impr and impr > 0) else 0.0

def compute_cvr(clicks, orders):
    return (orders / clicks) if (clicks and clicks > 0) else 0.0

def flatten_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns.tolist():
        val = df[col]
        if isinstance(val, pd.DataFrame):
            df[col] = val.astype(str).apply(lambda row: " ".join([str(x) for x in row.values if str(x).strip()]), axis=1)
        elif hasattr(val, "dtype") and val.dtype == "O":
            df[col] = val.apply(lambda x: " ".join(x) if isinstance(x, (list, tuple)) else ("" if pd.isna(x) else str(x)))
    return df

# ==========================================================
# PHASE 1 — UNIFIED HARVEST ENGINE
# ==========================================================
def run_phase1(df: pd.DataFrame):
    if CONFIG.get("DEBUG_MODE", False):
        print("▶ Running Phase 1: Unified Harvest Engine...")
    df = df.copy()
    df.columns = df.columns.str.strip()

    # merge duplicate "7 Day Total Sales" columns if present
    sales_cols = [c for c in df.columns if c.strip().lower() == "7 day total sales"]
    if len(sales_cols) > 1:
        df["7 Day Total Sales"] = df[sales_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
        df = df.drop(columns=sales_cols[1:], errors="ignore")

    # Diagnostics + auto-fix for duplicate headers / non-scalars
    Path(CONFIG["OUTPUT_DIR"]).mkdir(exist_ok=True)
    import json, itertools
    diag_rows = []
    problem_cols = []
    for col in df.columns.tolist():
        val = df[col]
        try:
            sample_vals = list(itertools.islice((x for x in val.values if pd.notna(x)), 10))
        except Exception:
            sample_vals = ["<unreadable>"]
        is_dataframe = isinstance(val, pd.DataFrame)
        non_scalar_count = 0
        scalar_count = 0
        if not is_dataframe:
            if hasattr(val, "dtype") and val.dtype == "O":
                for v in sample_vals:
                    if isinstance(v, (list, tuple, dict, set)):
                        non_scalar_count += 1
                    else:
                        scalar_count += 1
        else:
            non_scalar_count = len(sample_vals)
        suggested = "ok"
        if is_dataframe:
            suggested = "duplicate-header -> flatten"
            problem_cols.append(col)
        elif non_scalar_count > 0 and non_scalar_count > scalar_count:
            suggested = "object-cells -> join list/tuple to string"
            problem_cols.append(col)
        diag_rows.append({
            "column": col,
            "is_dataframe": bool(is_dataframe),
            "dtype": str(val.dtype) if not is_dataframe else "DataFrame",
            "sample_values": json.dumps([str(x) for x in sample_vals], ensure_ascii=False),
            "suggested_action": suggested
        })
    diag_df = pd.DataFrame(diag_rows)
    diag_path = Path(CONFIG["OUTPUT_DIR"]) / "phase1_diagnostics.xlsx"
    try:
        with pd.ExcelWriter(diag_path, engine="openpyxl") as w:
            diag_df.to_excel(w, sheet_name="Column_Diagnostics", index=False)
    except Exception:
        diag_df.to_csv(str(diag_path.with_suffix(".csv")), index=False)

    # Auto-fix flattening
    for col in df.columns.tolist():
        val = df[col]
        if isinstance(val, pd.DataFrame):
            df[col] = val.astype(str).apply(lambda row: " ".join([str(x) for x in row.values if str(x).strip()]), axis=1)
        elif hasattr(val, "dtype") and val.dtype == "O":
            df[col] = val.apply(lambda x: " ".join(x) if isinstance(x, (list, tuple)) else (json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else ("" if pd.isna(x) else str(x))))
    df = flatten_dataframe_columns(df)

    still_problems = [c for c in df.columns.tolist() if isinstance(df[c], pd.DataFrame)]
    if still_problems:
        raise Exception(f"After auto-fix, these columns are still DataFrame objects: {still_problems}. See diagnostics: {diag_path}")
    if problem_cols and CONFIG.get("DEBUG_MODE", False):
        print(f"⚠️ Phase1: Detected problematic columns: {problem_cols}. Diagnostics saved to: {diag_path}")

    # canonical columns
    string_cols = ["Campaign Name", "Match Type", "Targeting", "Customer Search Term"]
    numeric_cols = ["Impressions", "Clicks", "Spend", "Cost Per Click (CPC)",
                    "7 Day Total Sales", "7 Day Total Orders (#)"]

    for c in string_cols:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].astype(str)

    for c in numeric_cols:
        if c not in df.columns:
            df[c] = 0
        df[c] = safe_numeric(df[c])

    # Derived metrics
    df["CTR"] = np.where(df["Impressions"] > 0, df["Clicks"] / df["Impressions"], 0)
    df["CVR"] = np.where(df["Clicks"] > 0, df["7 Day Total Orders (#)"] / df["Clicks"], 0)
    df["ROAS"] = np.where(df["Spend"] > 0, df["7 Day Total Sales"] / df["Spend"], 0)
    df["Campaign Median ROAS"] = df.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)

    # Discovery mask
    auto_pattern = r'close-match|loose-match|substitutes|complements|category=|asin|b0[a-z0-9]{8}'
    discovery_mask = (~df["Match Type"].str.contains("exact", case=False, na=False)) | \
                     (df["Targeting"].str.contains(auto_pattern, case=False, na=False))
    discovery_df = df[discovery_mask].copy()

    avg_cpc = discovery_df.get("Cost Per Click (CPC)", pd.Series()).replace(0, np.nan).mean()
    if np.isnan(avg_cpc) or avg_cpc <= 0:
        avg_cpc = df.get("Cost Per Click (CPC)", pd.Series()).replace(0, np.nan).mean()
    if np.isnan(avg_cpc) or avg_cpc <= 0:
        avg_cpc = 0.1

    readable_mask = (
        (discovery_df["Clicks"] >= CONFIG["CLICK_THRESHOLD"]) &
        (discovery_df["Spend"] >= CONFIG["SPEND_MULTIPLIER"] * avg_cpc) &
        (discovery_df["Impressions"] >= CONFIG["IMPRESSION_THRESHOLD"])
    )
    readable_df = discovery_df[readable_mask].copy()

    discovery_global_median = discovery_df["ROAS"].replace([np.inf, -np.inf], np.nan).median()
    if np.isnan(discovery_global_median) or discovery_global_median <= 0:
        discovery_global_median = 0.1

    def _classify(row):
        median = row["Campaign Median ROAS"] if row["Campaign Median ROAS"] > 0 else discovery_global_median
        if row["7 Day Total Sales"] <= 0:
            if row["Clicks"] >= CONFIG["CLICK_THRESHOLD"] and row["Impressions"] >= CONFIG["IMPRESSION_THRESHOLD"] and row["ROAS"] > 0:
                if row["ROAS"] >= 1.5 * discovery_global_median:
                    return "Promote"
                return "Stable"
            return "Stable"
        if row["ROAS"] >= 1.2 * median:
            return "Promote"
        if row["ROAS"] < 0.8 * median:
            return "Bid Down / Review"
        return "Stable"

    readable_df["Action"] = readable_df.apply(_classify, axis=1)
    promote_df = readable_df[readable_df["Action"] == "Promote"].copy()

    promote_df["ASIN_in_term"] = promote_df["Customer Search Term"].apply(extract_asin)
    promote_df["ASIN_in_targeting"] = promote_df["Targeting"].apply(extract_asin)
    promote_df["Category_in_targeting"] = promote_df["Targeting"].str.contains(r'category\s*=\s*"?b0', case=False, na=False)
    promote_df["Is_PT_Candidate"] = promote_df["ASIN_in_term"].notna() | promote_df["ASIN_in_targeting"].notna() | promote_df["Category_in_targeting"]
    promote_df["Promotion_Type"] = np.where(promote_df["Is_PT_Candidate"], "Move→PT", "Move→Exact")

    # ---- PT evaluation extension ----
    pt_df = promote_df[promote_df["Is_PT_Candidate"]].copy()
    if not pt_df.empty:
        # Normalize metrics for scoring
        for col in ["ROAS", "CTR", "CVR", "7 Day Total Orders (#)"]:
            median = pt_df[col].median() if col in pt_df.columns else 0.0
            pt_df[col + "_norm"] = np.where(median > 0, pt_df[col] / median, 0)

        # Compute composite PT score
        pt_df["PT_score"] = (
            0.4 * pt_df.get("ROAS_norm", 0) +
            0.3 * pt_df.get("CTR_norm", 0) +
            0.2 * pt_df.get("CVR_norm", 0) +
            0.1 * pt_df.get("7 Day Total Orders (#)_norm", 0)
        )

        # Apply thresholds
        pt_df["PT_recommendation"] = np.select(
            [
                (pt_df["PT_score"] >= 0.6) & (pt_df["7 Day Total Orders (#)"] >= 1),
                (pt_df["ROAS"] < 0.5 * pt_df["Campaign Median ROAS"])
            ],
            ["Promote→PT", "Reject (Low ROAS)"],
            default="Watch"
        )

        # Add category-level handling
        pt_df["Category_Action"] = np.where(
            pt_df["Category_in_targeting"] & (pt_df["ROAS"] < 0.5 * pt_df["Campaign Median ROAS"]),
            "Confirm Negative Category",
            np.where(pt_df["Category_in_targeting"], "Watch", "None")
        )

        # Merge back into promote_df
        promote_df = promote_df.merge(
            pt_df[["Customer Search Term","PT_score","PT_recommendation","Category_Action"]],
            on="Customer Search Term", how="left"
        )

    # Routing / exact-like campaign identification
    campaigns = df["Campaign Name"].dropna().unique().tolist()
    exact_like = []
    for c in campaigns:
        if re.search(r'\bexact\b', c, flags=re.I) or re.search(r'\bexac', c, flags=re.I) or re.search(r'_ex', c, flags=re.I):
            exact_like.append(c)
    exact_campaigns_from_rows = df[df["Match Type"].astype(str).str.contains("exact", case=False, na=False)]["Campaign Name"].dropna().unique().tolist()
    for c in exact_campaigns_from_rows:
        if c not in exact_like:
            exact_like.append(c)
    exact_like = list(dict.fromkeys(exact_like))

    def tokens_of(s: str):
        s = re.sub(r"[^a-z0-9 ]", " ", str(s).lower())
        return {t for t in s.split() if len(t) > 2}

    def route_campaign(row):
        term = str(row["Customer Search Term"])
        src = str(row["Campaign Name"])
        asin = extract_asin(term) or extract_asin(row["Targeting"])
        if asin:
            for c in campaigns:
                if asin in str(c).upper():
                    return c, "ASIN-match"
        tokens_term = tokens_of(term)
        tokens_src = tokens_of(src)
        best, best_score = None, 0
        for cand in exact_like:
            cand_tokens = tokens_of(cand)
            score = len(tokens_term & cand_tokens) * 2 + len(tokens_src & cand_tokens)
            if score > best_score:
                best, best_score = cand, score
        if best_score > 0:
            return best, "Fuzzy-match"
        tokens = re.split(r'[_\-| ]+', src)
        prefix = "_".join(tokens[:2]).strip() if len(tokens) > 1 else tokens[0]
        suggested = f"{prefix}_Exact_Harvest_{datetime.today().strftime('%b%y')}"
        return suggested, "Suggested-New"

    if not promote_df.empty:
        promote_df["Suggested Exact Campaign"], promote_df["Routing Method"] = zip(*promote_df.apply(route_campaign, axis=1))
    else:
        promote_df["Suggested Exact Campaign"] = []
        promote_df["Routing Method"] = []

    unmatched_df = promote_df[promote_df["Routing Method"] == "Suggested-New"].copy()

    # ======================================================
    # NEGATIVE EVALUATION BLOCK (Neg_Action)
    # ======================================================
    # Work off readable_df (non-promote candidates) to decide negatives
    neg_df = readable_df[readable_df["Action"] != "Promote"].copy()
    if neg_df.empty:
        # ensure attrs exist even if empty
        promote_df.attrs["Watch_Negatives"] = pd.DataFrame()
        promote_df.attrs["Confirm_Negatives"] = pd.DataFrame()
        promote_df.attrs["Negatives_To_Apply"] = pd.DataFrame()
    else:
        # Ensure campaign medians present
        neg_df["Campaign Median ROAS"] = neg_df.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)

        def classify_negative(row):
            median = row["Campaign Median ROAS"] if row["Campaign Median ROAS"] > 0 else discovery_global_median
            roas_ratio = (row["ROAS"] / median) if median > 0 else 0
            # High spend, no conversions -> manual review
            if row["Clicks"] >= 15 and row["Spend"] >= 4 * avg_cpc and row.get("7 Day Total Orders (#)", 0) == 0:
                return "Review"
            # Clear poor performer (ready to negate)
            if roas_ratio < 0.3:
                return "Confirm Negative"
            # Borderline (watch for next cycle)
            if roas_ratio < 0.5:
                return "Watch Negative"
            return "Keep"

        neg_df["Neg_Action"] = neg_df.apply(classify_negative, axis=1)

        # build watch & confirm frames
        watch_neg = neg_df[neg_df["Neg_Action"] == "Watch Negative"].copy().reset_index(drop=True)
        confirm_neg = neg_df[neg_df["Neg_Action"] == "Confirm Negative"].copy().reset_index(drop=True)

        # Build Negatives_To_Apply from confirm_neg (and later dedupe against promoted terms)
        neg_rows = []
        for _, r in confirm_neg.iterrows():
            term = str(r["Customer Search Term"]).strip()
            sc = r["Campaign Name"]
            neg_rows.append({
                "SourceCampaign": sc,
                "NegativeType": "NEG_EXACT",
                "TermOrASIN": term,
                "Reason": "Confirm Negative (Low ROAS/CTR/CVR)",
                "Status": "Pending",
                "ValidationClicksRequired": CONFIG.get("MIN_VALIDATION_CLICKS", 5),
                "DelayCycles": CONFIG.get("NEGATIVE_DELAY_CYCLES", 1)
            })
        negatives_df = pd.DataFrame(neg_rows).drop_duplicates(subset=["SourceCampaign", "TermOrASIN"])

        # Also add promote-derived negatives (to prevent cannibalization) but mark pending
        prom_neg_rows = []
        for _, r in promote_df[promote_df["Promotion_Type"] == "Move→Exact"].iterrows():
            term = str(r["Customer Search Term"]).strip()
            srcs = discovery_df[discovery_df["Customer Search Term"].astype(str).str.strip().str.lower() == term.lower()]["Campaign Name"].unique().tolist()
            if not srcs:
                srcs = [r["Campaign Name"]]
            for sc in srcs:
                prom_neg_rows.append({
                    "SourceCampaign": sc,
                    "NegativeType": "NEG_EXACT",
                    "TermOrASIN": term,
                    "Reason": "Promoted->Exact",
                    "Status": "Pending",
                    "ValidationClicksRequired": CONFIG.get("MIN_VALIDATION_CLICKS", 5),
                    "DelayCycles": CONFIG.get("NEGATIVE_DELAY_CYCLES", 1)
                })
        prom_neg_df = pd.DataFrame(prom_neg_rows).drop_duplicates(subset=["SourceCampaign", "TermOrASIN"])

        # merge and dedupe: prioritize confirm-negatives (set Status "Review-Confirm" for manual final check)
        if not negatives_df.empty and not prom_neg_df.empty:
            combined_neg = pd.concat([negatives_df, prom_neg_df], ignore_index=True, sort=False).drop_duplicates(subset=["SourceCampaign", "TermOrASIN"])
        elif not negatives_df.empty:
            combined_neg = negatives_df
        else:
            combined_neg = prom_neg_df

        # final sanity columns
        if combined_neg is None or combined_neg.empty:
            combined_neg = pd.DataFrame(columns=["SourceCampaign","NegativeType","TermOrASIN","Reason","Status","ValidationClicksRequired","DelayCycles"])

        promote_df.attrs["Watch_Negatives"] = watch_neg
        promote_df.attrs["Confirm_Negatives"] = confirm_neg
        promote_df.attrs["Negatives_To_Apply"] = combined_neg.reset_index(drop=True)

    # ---------- NEGATIVE DISCOVERY: Watch vs Confirm (campaign stats based) ----------
    # compute campaign-level medians/std for CTR & CVR & ROAS using readable_df (used earlier for readable_with_stats)
    camp_stats = readable_df.groupby("Campaign Name").agg({
        "ROAS": ["median","std"],
        "CTR": ["median","std"],
        "CVR": ["median","std"]
    })
    # flatten multiindex columns
    camp_stats.columns = ["_".join(col).strip() for col in camp_stats.columns.values]
    camp_stats = camp_stats.reset_index().rename(columns={
        "ROAS_median":"ROAS_med","ROAS_std":"ROAS_std",
        "CTR_median":"CTR_med","CTR_std":"CTR_std",
        "CVR_median":"CVR_med","CVR_std":"CVR_std"
    })
    # merge back
    readable_with_stats = readable_df.merge(camp_stats, how="left", on="Campaign Name")

    def negative_decision(row):
        roas_med = row.get("ROAS_med", 1e-9) or 1e-9
        ctr_med = row.get("CTR_med", 0) or 0
        cvr_med = row.get("CVR_med", 0) or 0
        ctr_std = row.get("CTR_std", 0) or 0
        cvr_std = row.get("CVR_std", 0) or 0

        # Guard conditions
        roas_ok = row["ROAS"] >= 0.8 * roas_med
        ctr_ok = row["CTR"] >= (ctr_med - 0.5 * ctr_std)
        cvr_ok = row["CVR"] >= (cvr_med - 0.5 * cvr_std)

        # Confirm negative: very poor across all three
        if (row["ROAS"] < 0.5 * roas_med) and \
           (row["CTR"] < (ctr_med - ctr_std)) and \
           (row["CVR"] < (cvr_med - cvr_std)):
            return "Confirm Negative"

    # Watch only if two metrics underperform
        if sum([not roas_ok, not ctr_ok, not cvr_ok]) >= 2:
            return "Watch Negative"

        return "Keep"

    readable_with_stats = readable_with_stats.drop(columns=["Action"], errors="ignore")
    readable_with_stats["Negative_Label"] = readable_with_stats.apply(negative_decision, axis=1)
    watch_df = readable_with_stats[readable_with_stats["Negative_Label"] == "Watch Negative"].copy().reset_index(drop=True)

    # For confirm negatives (actionable), create rows (this is supplemental to confirm_neg already created)
    confirm_df = readable_with_stats[readable_with_stats["Negative_Label"] == "Confirm Negative"].copy()
    confirm_rows = []
    for _, r in confirm_df.iterrows():
        term = str(r["Customer Search Term"]).strip()
        sc = r["Campaign Name"]
        confirm_rows.append({
            "SourceCampaign": sc,
            "NegativeType": "NEG_EXACT",
            "TermOrASIN": term,
            "Reason": "Confirm Negative (Low ROAS/CTR/CVR)",
            "Status": "Review-Confirm"
        })
    confirm_negatives_df = pd.DataFrame(confirm_rows).drop_duplicates(subset=["SourceCampaign", "TermOrASIN"])

    # ensure promote_df.attrs reflect final watch/confirm sets (merge if necessary)
    if "Watch_Negatives" not in promote_df.attrs or promote_df.attrs["Watch_Negatives"].empty:
        promote_df.attrs["Watch_Negatives"] = watch_df
    else:
        promote_df.attrs["Watch_Negatives"] = pd.concat([promote_df.attrs["Watch_Negatives"], watch_df], ignore_index=True).drop_duplicates()

    if "Confirm_Negatives" not in promote_df.attrs or promote_df.attrs["Confirm_Negatives"].empty:
        promote_df.attrs["Confirm_Negatives"] = confirm_negatives_df
    else:
        promote_df.attrs["Confirm_Negatives"] = pd.concat([promote_df.attrs["Confirm_Negatives"], confirm_negatives_df], ignore_index=True).drop_duplicates()

    # Summary
    summary = pd.DataFrame([
        {"Metric": "Total Discovery Rows", "Value": len(discovery_df)},
        {"Metric": "Rows Passing Readability Filter", "Value": len(readable_df)},
        {"Metric": "Promote Candidates", "Value": len(promote_df)},
        {"Metric": "Move->Exact", "Value": int((promote_df['Promotion_Type'] == 'Move→Exact').sum())},
        {"Metric": "Move->PT", "Value": int((promote_df['Promotion_Type'] == 'Move→PT').sum())},
        {"Metric": "Matched Existing Exact", "Value": int((promote_df['Routing Method'] != 'Suggested-New').sum())},
        {"Metric": "Suggested New Exact", "Value": int(len(unmatched_df))},
        {"Metric": "Average Discovery CPC", "Value": round(avg_cpc, 4)}
    ])

    if CONFIG.get("DEBUG_MODE", False):
        print("Phase1 summary:", summary.to_dict(orient="records"))

    return promote_df.reset_index(drop=True), summary, unmatched_df.reset_index(drop=True)


# ==========================================================
# PHASE 1.5 — EXACT & PT OPTIMIZATION
# ==========================================================
def run_phase1_5(df: pd.DataFrame) -> pd.DataFrame:
    if CONFIG["DEBUG_MODE"]:
        print("▶ Running Phase 1.5: Exact & PT Optimization...")
    d = df.copy()

    needed = ["Match Type", "Targeting", "Impressions", "Clicks", "Spend", "Cost Per Click (CPC)",
              "7 Day Total Sales", "7 Day Total Orders (#)", "Campaign Name", "Customer Search Term"]
    for c in needed:
        if c not in d.columns:
            d[c] = "" if c in ["Match Type","Targeting","Campaign Name","Customer Search Term"] else 0

    mask = d["Match Type"].astype(str).str.contains("exact", case=False, na=False) | \
           d["Targeting"].astype(str).str.contains(r"asin|b0[a-z0-9]{8}", case=False, na=False)
    exact_df = d[mask].copy()
    if exact_df.empty:
        if CONFIG["DEBUG_MODE"]:
            print("⚠️ No Exact/PT rows found for optimization.")
        return pd.DataFrame()

    exact_df["Impressions"] = safe_numeric(exact_df["Impressions"])
    exact_df["Clicks"] = safe_numeric(exact_df["Clicks"])
    exact_df["Spend"] = safe_numeric(exact_df["Spend"])
    exact_df["Cost Per Click (CPC)"] = safe_numeric(exact_df.get("Cost Per Click (CPC)", pd.Series(0)))
    exact_df["7 Day Total Sales"] = safe_numeric(exact_df["7 Day Total Sales"])
    exact_df["7 Day Total Orders (#)"] = safe_numeric(exact_df["7 Day Total Orders (#)"])

    exact_df["CTR"] = exact_df.apply(lambda r: compute_ctr(r["Impressions"], r["Clicks"]), axis=1)
    exact_df["CVR"] = exact_df.apply(lambda r: compute_cvr(r["Clicks"], r["7 Day Total Orders (#)"]), axis=1)
    exact_df["ROAS"] = np.where(exact_df["Spend"] > 0, exact_df["7 Day Total Sales"] / exact_df["Spend"], 0)
    exact_df["Campaign Median ROAS"] = exact_df.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)

    def bid_action(row):
        median = row["Campaign Median ROAS"] if row["Campaign Median ROAS"] > 0 else 1e-9
        if row["Clicks"] < CONFIG["CLICK_THRESHOLD"] or row["Spend"] < CONFIG["SPEND_MULTIPLIER"] * (row.get("Cost Per Click (CPC)", 1) or 1):
            return "Stay Put"
        if row["ROAS"] >= 1.2 * median:
            return "Bid Up"
        if row["ROAS"] <= 0.8 * median:
            return "Bid Down"
        return "Hold"

    exact_df["Action"] = exact_df.apply(bid_action, axis=1)

    def calc_new_bid(row):
        bid = float(row.get("Cost Per Click (CPC)", 0) or 0)
        target = row["Campaign Median ROAS"]
        actual = row["ROAS"]
        if bid <= 0 or target <= 0 or actual <= 0:
            return bid
        delta = (actual - target) / target
        alpha = CONFIG["ALPHA_EXACT_PT"]
        new = bid * (1 + alpha * delta)
        lower = bid * (1 - CONFIG["MAX_BID_CHANGE"])
        upper = bid * (1 + CONFIG["MAX_BID_CHANGE"])
        return max(lower, min(upper, new))

    exact_df["New Bid"] = exact_df.apply(calc_new_bid, axis=1)
    exact_df["Current Bid"] = exact_df["Cost Per Click (CPC)"]

    cols = ["Campaign Name", "Match Type", "Targeting", "Customer Search Term",
            "Impressions", "Clicks", "Spend", "Current Bid", "Cost Per Click (CPC)",
            "CTR", "CVR", "7 Day Total Sales", "ROAS",
            "Action", "New Bid"]
    if CONFIG["DEBUG_MODE"]:
        print(f"✅ Phase 1.5 complete — {len(exact_df)} rows optimized.")
    return exact_df[cols].reset_index(drop=True)

# ==========================================================
# PHASE 2 — BROAD / PHRASE OPTIMIZATION (AGGREGATED)
# ==========================================================
def run_phase2(df: pd.DataFrame) -> pd.DataFrame:
    if CONFIG["DEBUG_MODE"]:
        print("▶ Running Phase 2: Broad & Phrase Optimization (aggregated)...")
    d = df.copy()

    for c in ["Match Type", "Campaign Name", "Customer Search Term", "Impressions", "Clicks", "Spend", "Cost Per Click (CPC)", "7 Day Total Sales", "7 Day Total Orders (#)"]:
        if c not in d.columns:
            d[c] = "" if isinstance(d.get(c, ""), str) else 0

    mask = d["Match Type"].astype(str).str.contains("broad|phrase", case=False, na=False)
    pb = d[mask].copy()
    if pb.empty:
        if CONFIG["DEBUG_MODE"]:
            print("⚠️ No Broad/Phrase rows found.")
        return pd.DataFrame()

    def kw_theme(s: str, n_tokens: int = 2) -> str:
        t = normalize_text(s)
        tokens = [tok for tok in t.split() if len(tok) > 2]
        if not tokens:
            tokens = t.split()
        return " ".join(tokens[:n_tokens])

    pb["Normalized_KW"] = pb["Customer Search Term"].apply(lambda x: kw_theme(str(x), n_tokens=2))
    pb["Impressions"] = safe_numeric(pb["Impressions"])
    pb["Clicks"] = safe_numeric(pb["Clicks"])
    pb["Spend"] = safe_numeric(pb["Spend"])
    pb["Cost Per Click (CPC)"] = safe_numeric(pb.get("Cost Per Click (CPC)", pd.Series(0)))
    pb["7 Day Total Sales"] = safe_numeric(pb["7 Day Total Sales"])
    pb["7 Day Total Orders (#)"] = safe_numeric(pb["7 Day Total Orders (#)"])

    agg = pb.groupby(["Campaign Name", "Normalized_KW"], as_index=False).agg({
        "Impressions": "sum",
        "Clicks": "sum",
        "Spend": "sum",
        "Cost Per Click (CPC)": "mean",
        "7 Day Total Sales": "sum",
        "7 Day Total Orders (#)": "sum"
    })

    agg["CTR"] = np.where(agg["Impressions"] > 0, agg["Clicks"] / agg["Impressions"], 0)
    agg["CVR"] = np.where(agg["Clicks"] > 0, agg["7 Day Total Orders (#)"] / agg["Clicks"], 0)
    agg["ROAS"] = np.where(agg["Spend"] > 0, agg["7 Day Total Sales"] / agg["Spend"], 0)
    agg["Campaign Median ROAS"] = agg.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)

    def agg_action(row):
        median = row["Campaign Median ROAS"] if row["Campaign Median ROAS"] > 0 else 1e-9
        if row["Clicks"] < CONFIG["CLICK_THRESHOLD"] or row["Spend"] < CONFIG["SPEND_MULTIPLIER"] * (row.get("Cost Per Click (CPC)", 1) or 1):
            return "Stay Put"
        if row["ROAS"] >= 1.2 * median:
            return "Bid Up"
        if row["ROAS"] <= 0.8 * median:
            return "Bid Down"
        return "Hold"

    agg["Action"] = agg.apply(agg_action, axis=1)

    def agg_new_bid(row):
        bid = float(row.get("Cost Per Click (CPC)", 0) or 0)
        target = row["Campaign Median ROAS"]
        actual = row["ROAS"]
        if bid <= 0 or target <= 0 or actual <= 0:
            return bid
        delta = (actual - target) / target
        alpha = CONFIG["ALPHA_BROAD_PHRASE"]
        new = bid * (1 + alpha * delta)
        lower = bid * (1 - CONFIG["MAX_BID_CHANGE"])
        upper = bid * (1 + CONFIG["MAX_BID_CHANGE"])
        return max(lower, min(upper, new))

    agg["New Bid"] = agg.apply(agg_new_bid, axis=1)
    agg["Current Bid"] = agg["Cost Per Click (CPC)"]

    cols = ["Campaign Name", "Normalized_KW", "Impressions", "Clicks", "Spend",
            "Current Bid", "Cost Per Click (CPC)", "CTR", "CVR", "7 Day Total Sales", "ROAS",
            "Campaign Median ROAS", "Action", "New Bid"]
    if CONFIG["DEBUG_MODE"]:
        print(f"✅ Phase 2 complete — {len(agg)} aggregated keyword rows produced.")
    return agg[cols].reset_index(drop=True)

# ==========================================================
# STAGE 5 — AUTO TERM GROUPING & PROMOTION
# ==========================================================
def run_auto_cluster(df: pd.DataFrame,
                     min_clicks: int = 10,
                     min_impr: int = 250,
                     min_spend_mult: float = 3.0,
                     jaccard_threshold: float = 0.3) -> pd.DataFrame:
    if CONFIG["DEBUG_MODE"]:
        print("▶ Running Stage 5: Auto Term Grouping...")
    d = df.copy()
    if "Targeting" not in d.columns:
        d["Targeting"] = ""
    d["Targeting"] = d["Targeting"].astype(str)
    auto_mask = d["Targeting"].str.contains(r"close-match|loose-match|substitutes|complements|category=", case=False, na=False)
    auto_df = d[auto_mask].copy()
    if len(auto_df) < CONFIG["AUTO_CLUSTER_MIN_ROWS"]:
        if CONFIG["DEBUG_MODE"]:
            print(f"⚠️ Not enough auto rows ({len(auto_df)}) to cluster — threshold {CONFIG['AUTO_CLUSTER_MIN_ROWS']}.")
        return pd.DataFrame()

    def toks(s): return {t for t in re.sub(r"[^a-z0-9 ]"," ",str(s).lower()).split() if len(t)>2}
    terms = auto_df["Customer Search Term"].fillna("").tolist()
    used, groups = set(), []
    for i, t in enumerate(terms):
        if i in used: continue
        base = toks(t)
        if not base: continue
        group = [i]; used.add(i)
        for j in range(i+1, len(terms)):
            if j in used: continue
            other = toks(terms[j])
            if not other: continue
            jacc = len(base & other) / len(base | other)
            if jacc >= jaccard_threshold:
                group.append(j); used.add(j)
        groups.append(group)

    clusters = []
    avg_cpc = auto_df["Cost Per Click (CPC)"].replace(0, np.nan).mean() or 0.1
    for gid, idxs in enumerate(groups):
        grp = auto_df.iloc[idxs]
        clicks = int(grp["Clicks"].sum()) if "Clicks" in grp.columns else 0
        impr = int(grp["Impressions"].sum()) if "Impressions" in grp.columns else 0
        spend = float(grp["Spend"].sum()) if "Spend" in grp.columns else 0.0
        sales = float(grp["7 Day Total Sales"].sum()) if "7 Day Total Sales" in grp.columns else 0.0
        rep = grp["Customer Search Term"].iloc[0] if not grp.empty else ""
        ready = (clicks >= min_clicks) and (spend >= min_spend_mult * avg_cpc) and (impr >= min_impr)
        clusters.append({"ClusterID": gid, "RepresentativeTerm": rep, "TermCount": len(grp),
                         "Clicks": clicks, "Impressions": impr, "Spend": spend, "Sales": sales, "Ready": ready})
    clusters_df = pd.DataFrame(clusters)
    if CONFIG["DEBUG_MODE"]:
        print(f"✅ Stage 5 complete — {len(clusters_df)} clusters formed, {clusters_df['Ready'].sum() if not clusters_df.empty else 0} ready.")
    return clusters_df

# ==========================================================
# MAIN EXECUTION — robust sequential run and outputs
# ==========================================================
if __name__ == "__main__":
    file_path = "Sponsored_Products_Search_term_report.xlsx"
    try:
        print("🚀 Starting Full PPC Optimization Workflow...\n")
        df_input = pd.read_excel(file_path)

        # ---- START: INPUT SANITIZATION ----
        df_input.columns = df_input.columns.map(lambda c: "" if pd.isna(c) else str(c).strip())
        empty_cols = [c for c in df_input.columns if c == ""]
        if empty_cols:
            print(f"⚠️ Dropping unnamed columns: {empty_cols}")
            df_input = df_input.drop(columns=empty_cols)

        sales_cols = [c for c in df_input.columns if c.strip().lower() == "7 day total sales"]
        if len(sales_cols) > 1:
            print(f"⚠️ Merging duplicate sales columns: {sales_cols}")
            df_input["7 Day Total Sales"] = df_input[sales_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
            df_input = df_input.drop(columns=sales_cols[1:], errors="ignore")

        expected_core = [
            "Campaign Name", "Match Type", "Customer Search Term", "Targeting",
            "Impressions", "Clicks", "Cost Per Click (CPC)", "Spend",
            "7 Day Total Sales", "7 Day Total Orders (#)"
        ]
        for c in expected_core:
            if c not in df_input.columns:
                df_input[c] = 0 if c in ["Impressions","Clicks","Cost Per Click (CPC)","Spend","7 Day Total Sales","7 Day Total Orders (#)"] else ""
                print(f"⚠️ Adding missing column (empty): {c}")

        for c in ["Impressions","Clicks","Cost Per Click (CPC)","Spend","7 Day Total Sales","7 Day Total Orders (#)"]:
            df_input[c] = pd.to_numeric(df_input[c].astype(str).str.replace(r"[^\d\.\-]", "", regex=True), errors="coerce").fillna(0)

        if CONFIG["DEBUG_MODE"]:
            print("Sanitized columns:", list(df_input.columns))
            print("Sample dtypes:", df_input.dtypes.to_dict())
        # ---- END: INPUT SANITIZATION ----

        df_input = df_input.reset_index(drop=True)
        outputs = {}
        errors = {}
        phase_stats = []

        # ================
        # Phase 1
        # ================
        try:
            promote, summary, unmatched = run_phase1(df_input)

            # ---- Attach additional Phase 1 outputs ----
            if isinstance(promote, pd.DataFrame):
                negs = promote.attrs.get("Negatives_To_Apply")
                watch = promote.attrs.get("Watch_Negatives")
                confirm = promote.attrs.get("Confirm_Negatives")

                if isinstance(negs, pd.DataFrame) and not negs.empty:
                    outputs["Negatives_To_Apply"] = negs
                if isinstance(watch, pd.DataFrame) and not watch.empty:
                    outputs["Watch_Negatives"] = watch
                if isinstance(confirm, pd.DataFrame) and not confirm.empty:
                    outputs["Confirm_Negatives"] = confirm

            outputs["Phase1_Promote"] = promote
            outputs["Phase1_Summary"] = summary
            outputs["Phase1_Unmatched"] = unmatched
            readable_val = int(summary.loc[summary['Metric'] == 'Rows Passing Readability Filter', 'Value'].values[0]) \
                if not summary.loc[summary['Metric'] == 'Rows Passing Readability Filter'].empty else 0
            phase_stats.append({
                "Phase": "Phase1",
                "PromoteRows": len(promote),
                "ReadableRows": readable_val
            })
            print("Phase 1 completed ✅")

        except Exception as e:
            errors["Phase1"] = str(e)
            print(f"Phase 1 failed: {e}")

        # ================
        # Stage 5
        # ================
        try:
            auto_mask = df_input["Targeting"].astype(str).str.contains(
                r"close-match|loose-match|substitutes|complements|category=",
                case=False, na=False)
            if df_input[auto_mask].shape[0] >= CONFIG["AUTO_CLUSTER_MIN_ROWS"]:
                auto_clusters = run_auto_cluster(df_input)
            else:
                auto_clusters = pd.DataFrame()
                print("Stage 5 skipped (not enough auto rows) ⚠️")
            outputs["Stage5_Auto_Clusters"] = auto_clusters
            phase_stats.append({"Phase": "Stage5", "ClusterCount": len(auto_clusters)})
        except Exception as e:
            errors["Stage5"] = str(e)
            outputs["Stage5_Auto_Clusters"] = pd.DataFrame()
            print(f"Stage 5 failed: {e}")

        # ================
        # Phase 1.5
        # ================
        try:
            p15 = run_phase1_5(df_input)
            outputs["Phase1.5_ExactBids"] = p15
            phase_stats.append({"Phase": "Phase1.5", "Rows": len(p15)})
            print("Phase 1.5 completed ✅")
        except Exception as e:
            errors["Phase1.5"] = str(e)
            outputs["Phase1.5_ExactBids"] = pd.DataFrame()
            print(f"Phase 1.5 failed: {e}")

        # ================
        # Phase 2
        # ================
        try:
            p2 = run_phase2(df_input)
            outputs["Phase2_PB_Bids"] = p2
            phase_stats.append({"Phase": "Phase2", "Rows": len(p2)})
            print("Phase 2 completed ✅")
        except Exception as e:
            errors["Phase2"] = str(e)
            outputs["Phase2_PB_Bids"] = pd.DataFrame()
            print(f"Phase 2 failed: {e}")

        # ================
        # Phase Summary
        # ================
        try:
            total_discovery = summary.loc[summary['Metric'] == 'Total Discovery Rows', 'Value'].values[0] \
                if ("Phase1_Summary" in outputs and not outputs["Phase1_Summary"].empty) else len(df_input)
            promote_rows = len(outputs.get("Phase1_Promote", pd.DataFrame()))
            readable_rows = int(outputs.get("Phase1_Summary", pd.DataFrame()).loc[
                outputs["Phase1_Summary"]['Metric'] == 'Rows Passing Readability Filter', 'Value'].values[0]) \
                if ("Phase1_Summary" in outputs and not outputs["Phase1_Summary"].empty) else 0
            phase_summary = {
                "Run Timestamp": datetime.now().isoformat(),
                "Total Input Rows": len(df_input),
                "Total Discovery Rows": int(total_discovery if not np.isnan(total_discovery) else 0),
                "Readable Rows": int(readable_rows),
                "Promote Candidates": int(promote_rows),
                "Phase1.5 Optimized Rows": int(len(outputs.get("Phase1.5_ExactBids", pd.DataFrame()))),
                "Phase2 Aggregated Rows": int(len(outputs.get("Phase2_PB_Bids", pd.DataFrame()))),
                "Stage5 Clusters": int(len(outputs.get("Stage5_Auto_Clusters", pd.DataFrame())))
            }
        except Exception as e:
            phase_summary = {"Run Timestamp": datetime.now().isoformat(), "ErrorBuildingSummary": str(e)}

        # ================
        # Write workbook
        # ================
        Path(CONFIG["OUTPUT_DIR"]).mkdir(exist_ok=True)
        out_file = f"{CONFIG['OUTPUT_DIR']}/s2c_full_run_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
            for name, df_out in outputs.items():
                try:
                    if isinstance(df_out, pd.DataFrame) and not df_out.empty:
                        df_out.to_excel(writer, sheet_name=name[:31], index=False)
                    else:
                        pd.DataFrame([{"Info": f"No rows for {name}"}]).to_excel(writer, sheet_name=name[:31], index=False)
                except Exception as e:
                    print(f"⚠️ Could not write sheet {name}: {e}")

            pd.DataFrame(phase_stats).to_excel(writer, sheet_name="Phase_Stats", index=False)
            pd.DataFrame([phase_summary]).to_excel(writer, sheet_name="Phase_Summary", index=False)
            if "Phase1_Summary" in outputs and isinstance(outputs["Phase1_Summary"], pd.DataFrame):
                outputs["Phase1_Summary"].to_excel(writer, sheet_name="Phase1_Summary_Details", index=False)
            if errors:
                pd.DataFrame([{"Phase": k, "Error": v} for k, v in errors.items()]).to_excel(writer, sheet_name="Errors", index=False)

        print(f"\n✅ Full workflow attempted. Output saved to: {out_file}")
        if errors:
            print("Some phases failed — check the 'Errors' tab in the output workbook for details.")
        else:
            print("All phases completed successfully.")

    # <-- CLOSE OUTER TRY HERE!
    except FileNotFoundError:
        print(f"⚠️ File not found: {file_path}. Please check your input filename or path.")
    except Exception as e:
        print(f"❌ Unexpected error during run: {e}")