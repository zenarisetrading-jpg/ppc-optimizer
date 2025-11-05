#!/usr/bin/env python3
"""
s2c_ppc_optimizer_v2_5_1.py — S2C PPC Optimizer (clean, runnable)
Version: 2.5.1
Author: Aslam + GPT-5
Features:
 - Phase1 harvest + PT eval
 - Phase1.5 Exact/PT bid suggestions
 - Phase2 Broad/Phrase aggregated bid suggestions
 - Stage5 Auto clustering (light)
 - Mutual exclusivity: Protect promote + bid-change terms from negatives
 - ROAS ceiling: exclude ROAS > 3 from negative consideration
 - Robust input sanitization and diagnostics
Save as: s2c_ppc_optimizer_v2_5_1.py
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
CONFIG = {
    "CLICK_THRESHOLD": 10,
    "SPEND_MULTIPLIER": 3,
    "IMPRESSION_THRESHOLD": 250,
    "ALPHA_EXACT_PT": 0.20,
    "ALPHA_BROAD_PHRASE": 0.15,
    "MAX_BID_CHANGE": 0.15,
    "AUTO_CLUSTER_MIN_ROWS": 10,
    "OUTPUT_DIR": "./outputs",
    "DEBUG_MODE": False,
    "MIN_VALIDATION_CLICKS": 5,
    "NEGATIVE_DELAY_CYCLES": 1,
    "NEGATIVE_ROAS_CEILING": 3.0,   # ROAS > 3 excluded from negatives
}

# -----------------------------
# UTILITIES
# -----------------------------
def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(r"[^\d\.\-]", "", regex=True),
                         errors="coerce").fillna(0)

def extract_asin(text: str):
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

# -----------------------------
# PHASE 1 — UNIFIED HARVEST ENGINE
# -----------------------------
def run_phase1(df: pd.DataFrame):
    if CONFIG.get("DEBUG_MODE"):
        print("▶ Running Phase 1: Unified Harvest Engine...")

    df = df.copy()
    # sanitize column names
    df.columns = df.columns.map(lambda c: "" if pd.isna(c) else str(c).strip())

    # Basic required columns: create safe fallbacks
    for c in ["Campaign Name", "Match Type", "Targeting", "Customer Search Term"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].astype(str)

    # merge duplicate sales columns if present (case-insensitive)
    sales_cols = [c for c in df.columns if c.strip().lower() == "7 day total sales"]
    if len(sales_cols) > 1:
        df["7 Day Total Sales"] = df[sales_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
        for extra in sales_cols[1:]:
            df.drop(columns=extra, inplace=True, errors="ignore")

    # Ensure numeric columns exist and sanitized
    num_cols = ["Impressions","Clicks","Spend","Cost Per Click (CPC)","7 Day Total Sales","7 Day Total Orders (#)"]
    for nc in num_cols:
        if nc not in df.columns:
            df[nc] = 0
        df[nc] = safe_numeric(df[nc])

    # Flatten any weird column objects
    df = flatten_dataframe_columns(df)

    # Derived metrics on full df (for reference)
    df["CTR"] = np.where(df["Impressions"] > 0, df["Clicks"] / df["Impressions"], 0.0)
    df["CVR"] = np.where(df["Clicks"] > 0, df["7 Day Total Orders (#)"] / df["Clicks"], 0.0)
    df["ROAS"] = np.where(df["Spend"] > 0, df["7 Day Total Sales"] / df["Spend"], 0.0)
    df["Campaign Median ROAS"] = df.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)

    # Discovery mask: include non-exact or special targeting (Auto/PT/Category)
    auto_pattern = r'close-match|loose-match|substitutes|complements|category=|asin|b0[a-z0-9]{8}'
    discovery_mask = (~df["Match Type"].str.contains("exact", case=False, na=False)) | (df["Targeting"].str.contains(auto_pattern, case=False, na=False))
    discovery_df = df[discovery_mask].copy()

    # SAFEGUARD numeric columns for discovery_df
    for nc in num_cols:
        if nc not in discovery_df.columns:
            discovery_df[nc] = 0
        discovery_df[nc] = safe_numeric(discovery_df[nc])

    discovery_df["CTR"] = np.where(discovery_df["Impressions"] > 0, discovery_df["Clicks"] / discovery_df["Impressions"], 0.0)
    discovery_df["CVR"] = np.where(discovery_df["Clicks"] > 0, discovery_df["7 Day Total Orders (#)"] / discovery_df["Clicks"], 0.0)
    discovery_df["ROAS"] = np.where(discovery_df["Spend"] > 0, discovery_df["7 Day Total Sales"] / discovery_df["Spend"], 0.0)
    discovery_df["ROAS"].replace([np.inf, -np.inf], np.nan, inplace=True)
    discovery_df["ROAS"] = discovery_df["ROAS"].fillna(0.0)
    discovery_df["Campaign Median ROAS"] = discovery_df.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)

    # Average discovery CPC (for spend threshold)
    avg_cpc = discovery_df["Cost Per Click (CPC)"].replace(0, np.nan).mean()
    if np.isnan(avg_cpc) or avg_cpc <= 0:
        avg_cpc = df["Cost Per Click (CPC)"].replace(0, np.nan).mean()
    if np.isnan(avg_cpc) or avg_cpc <= 0:
        avg_cpc = 0.1

    # Readability filter
    readable_mask = (
        (discovery_df["Clicks"] >= CONFIG["CLICK_THRESHOLD"]) &
        (discovery_df["Spend"] >= CONFIG["SPEND_MULTIPLIER"] * avg_cpc) &
        (discovery_df["Impressions"] >= CONFIG["IMPRESSION_THRESHOLD"])
    )
    readable_df = discovery_df[readable_mask].copy()

    # discovery global median to fallback
    discovery_global_median = discovery_df["ROAS"].median()
    if np.isnan(discovery_global_median) or discovery_global_median <= 0:
        discovery_global_median = 0.1

    # Classification logic to tag Promote / Stable / Bid Down
    def classify_action(row):
        median = row["Campaign Median ROAS"] if row["Campaign Median ROAS"] > 0 else discovery_global_median
        # special handling if zero sales but sufficient clicks/impr
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

    readable_df["Action"] = readable_df.apply(classify_action, axis=1)
    promote_df = readable_df[readable_df["Action"] == "Promote"].copy()

    # PT candidate detection & scoring
    promote_df["ASIN_in_term"] = promote_df["Customer Search Term"].apply(extract_asin)
    promote_df["ASIN_in_targeting"] = promote_df["Targeting"].apply(extract_asin)
    promote_df["Category_in_targeting"] = promote_df["Targeting"].str.contains(r'category\s*=\s*"?b0', case=False, na=False)
    promote_df["Is_PT_Candidate"] = promote_df["ASIN_in_term"].notna() | promote_df["ASIN_in_targeting"].notna() | promote_df["Category_in_targeting"]
    promote_df["Promotion_Type"] = np.where(promote_df["Is_PT_Candidate"], "Move→PT", "Move→Exact")

    # Compute simple PT score (if PT candidates exist)
    pt_df = promote_df[promote_df["Is_PT_Candidate"]].copy()
    if not pt_df.empty:
        for col in ["ROAS", "CTR", "CVR", "7 Day Total Orders (#)"]:
            if col in pt_df.columns:
                med = pt_df[col].median() if pt_df[col].notna().any() else 0.0
                pt_df[col + "_norm"] = np.where(med > 0, pt_df[col] / med, 0.0)
            else:
                pt_df[col + "_norm"] = 0.0
        pt_df["PT_score"] = (
            0.4 * pt_df.get("ROAS_norm", 0.0) +
            0.3 * pt_df.get("CTR_norm", 0.0) +
            0.2 * pt_df.get("CVR_norm", 0.0) +
            0.1 * pt_df.get("7 Day Total Orders (#)_norm", 0.0)
        )
        pt_df["PT_recommendation"] = np.select(
            [(pt_df["PT_score"] >= 0.6) & (pt_df["7 Day Total Orders (#)"] >= 1),
             (pt_df["ROAS"] < 0.5 * pt_df["Campaign Median ROAS"])],
            ["Promote→PT", "Reject (Low ROAS)"],
            default="Watch"
        )
        pt_df["Category_Action"] = np.where(
            pt_df["Category_in_targeting"] & (pt_df["ROAS"] < 0.5 * pt_df["Campaign Median ROAS"]),
            "Confirm Negative Category",
            np.where(pt_df["Category_in_targeting"], "Watch", "None")
        )
        promote_df = promote_df.merge(pt_df[["Customer Search Term","PT_score","PT_recommendation","Category_Action"]], on="Customer Search Term", how="left")

    # ROUTING to existing exact-like campaigns
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
        prefix = "_".join(tokens[:2]).strip() if len(tokens) > 1 else (tokens[0] if tokens else "New")
        suggested = f"{prefix}_Exact_Harvest_{datetime.today().strftime('%b%y')}"
        return suggested, "Suggested-New"

    if not promote_df.empty:
        promote_df["Suggested Exact Campaign"], promote_df["Routing Method"] = zip(*promote_df.apply(route_campaign, axis=1))
    else:
        promote_df["Suggested Exact Campaign"] = []
        promote_df["Routing Method"] = []

    unmatched_df = promote_df[promote_df["Routing Method"] == "Suggested-New"].copy()

    # --------------------------
    # NEGATIVE ENGINE (mutual exclusivity + ROAS cap)
    # --------------------------
    # Build protected terms: terms that were promoted (exact) and terms that will get bid updates later (we'll allow caller to merge)
    protected_terms = set(promote_df["Customer Search Term"].astype(str).str.strip().str.lower().unique())

    # readable_df (source for negatives) is earlier readable_df (non-promote discovery rows)
    neg_source = readable_df.copy()
    # Exclude any rows that are in promote set (mutual exclusivity)
    neg_source = neg_source[~neg_source["Customer Search Term"].astype(str).str.strip().str.lower().isin(protected_terms)].copy()

    # classify negatives
    def classify_negative(row):
        # ROAS ceiling: do not consider very high performing as negatives
        if row.get("ROAS", 0.0) > CONFIG.get("NEGATIVE_ROAS_CEILING", 3.0):
            return "Keep"
        # protected terms: keep
        if str(row["Customer Search Term"]).strip().lower() in protected_terms:
            return "Keep"
        median = row["Campaign Median ROAS"] if row["Campaign Median ROAS"] > 0 else discovery_global_median
        roas_ratio = (row["ROAS"] / median) if median > 0 else 0
        if row["Clicks"] >= 15 and row["Spend"] >= 4 * avg_cpc and row.get("7 Day Total Orders (#)", 0) == 0:
            return "Review"
        if roas_ratio < 0.3:
            return "Confirm Negative"
        if roas_ratio < 0.5:
            return "Watch Negative"
        return "Keep"

    neg_source["Neg_Action"] = neg_source.apply(classify_negative, axis=1)
    watch_neg = neg_source[neg_source["Neg_Action"] == "Watch Negative"].copy().reset_index(drop=True)
    confirm_neg = neg_source[neg_source["Neg_Action"] == "Confirm Negative"].copy().reset_index(drop=True)

    # Build Negatives_To_Apply from confirm_neg (NEG_EXACT rows)
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
    # Also add promote-derived NEG_EXACT candidates (to avoid cannibalization) but mark Pending
    for _, r in promote_df[promote_df["Promotion_Type"] == "Move→Exact"].iterrows():
        term = str(r["Customer Search Term"]).strip()
        # find source campaigns where term occurred
        srcs = discovery_df[discovery_df["Customer Search Term"].astype(str).str.strip().str.lower() == term.lower()]["Campaign Name"].unique().tolist()
        if not srcs:
            srcs = [r["Campaign Name"]]
        for sc in srcs:
            neg_rows.append({
                "SourceCampaign": sc,
                "NegativeType": "NEG_EXACT",
                "TermOrASIN": term,
                "Reason": "Promoted->Exact",
                "Status": "Pending",
                "ValidationClicksRequired": CONFIG.get("MIN_VALIDATION_CLICKS", 5),
                "DelayCycles": CONFIG.get("NEGATIVE_DELAY_CYCLES", 1)
            })

    negatives_df = pd.DataFrame(neg_rows).drop_duplicates(subset=["SourceCampaign","TermOrASIN"]).reset_index(drop=True)

    # Attach negative sets to promote_df.attrs for caller extraction
    promote_df.attrs["Watch_Negatives"] = watch_neg
    promote_df.attrs["Confirm_Negatives"] = confirm_neg
    promote_df.attrs["Negatives_To_Apply"] = negatives_df

    # Summary
    summary = pd.DataFrame([
        {"Metric":"Total Discovery Rows","Value":len(discovery_df)},
        {"Metric":"Rows Passing Readability Filter","Value":len(readable_df)},
        {"Metric":"Promote Candidates","Value":len(promote_df)},
        {"Metric":"Move->Exact","Value":int((promote_df['Promotion_Type']=='Move→Exact').sum()) if 'Promotion_Type' in promote_df.columns else 0},
        {"Metric":"Move->PT","Value":int((promote_df['Promotion_Type']=='Move→PT').sum()) if 'Promotion_Type' in promote_df.columns else 0},
        {"Metric":"Matched Existing Exact","Value":int((promote_df['Routing Method']!='Suggested-New').sum()) if 'Routing Method' in promote_df.columns else 0},
        {"Metric":"Suggested New Exact","Value":int(len(unmatched_df))}
    ])

    if CONFIG.get("DEBUG_MODE"):
        print("Phase1 summary:", summary.to_dict(orient="records"))

    return promote_df.reset_index(drop=True), summary, unmatched_df.reset_index(drop=True)

# -----------------------------
# PHASE 1.5 — Exact & PT Optimization
# -----------------------------
def run_phase1_5(df: pd.DataFrame):
    if CONFIG.get("DEBUG_MODE"):
        print("▶ Running Phase 1.5: Exact & PT Optimization...")
    d = df.copy()
    # ensure required columns exist
    needed = ["Match Type","Targeting","Impressions","Clicks","Spend","Cost Per Click (CPC)",
              "7 Day Total Sales","7 Day Total Orders (#)","Campaign Name","Customer Search Term"]
    for c in needed:
        if c not in d.columns:
            d[c] = 0 if c not in ["Match Type","Targeting","Campaign Name","Customer Search Term"] else ""
    mask = d["Match Type"].astype(str).str.contains("exact", case=False, na=False) | d["Targeting"].astype(str).str.contains(r"asin|b0[a-z0-9]{8}", case=False, na=False)
    exact_df = d[mask].copy()
    if exact_df.empty:
        return pd.DataFrame()
    # numeric safety
    for c in ["Impressions","Clicks","Spend","Cost Per Click (CPC)","7 Day Total Sales","7 Day Total Orders (#)"]:
        exact_df[c] = safe_numeric(exact_df[c])
    exact_df["CTR"] = np.where(exact_df["Impressions"]>0, exact_df["Clicks"]/exact_df["Impressions"],0)
    exact_df["CVR"] = np.where(exact_df["Clicks"]>0, exact_df["7 Day Total Orders (#)"]/exact_df["Clicks"],0)
    exact_df["ROAS"] = np.where(exact_df["Spend"]>0, exact_df["7 Day Total Sales"]/exact_df["Spend"],0)
    exact_df["Campaign Median ROAS"] = exact_df.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)

    def bid_action(row):
        median = row["Campaign Median ROAS"] if row["Campaign Median ROAS"]>0 else 1e-9
        if row["Clicks"] < CONFIG["CLICK_THRESHOLD"] or row["Spend"] < CONFIG["SPEND_MULTIPLIER"] * (row.get("Cost Per Click (CPC)",1) or 1):
            return "Stay Put"
        if row["ROAS"] >= 1.2 * median:
            return "Bid Up"
        if row["ROAS"] <= 0.8 * median:
            return "Bid Down"
        return "Hold"

    exact_df["Action"] = exact_df.apply(bid_action, axis=1)

    def calc_new_bid(row):
        bid = float(row.get("Cost Per Click (CPC)",0) or 0)
        target = row["Campaign Median ROAS"]
        actual = row["ROAS"]
        if bid <= 0 or target <= 0 or actual <= 0:
            return bid
        delta = (actual - target)/target
        alpha = CONFIG["ALPHA_EXACT_PT"]
        new = bid * (1 + alpha * delta)
        lower = bid * (1 - CONFIG["MAX_BID_CHANGE"])
        upper = bid * (1 + CONFIG["MAX_BID_CHANGE"])
        return max(lower, min(upper, new))

    exact_df["New Bid"] = exact_df.apply(calc_new_bid, axis=1)
    exact_df["Current Bid"] = exact_df["Cost Per Click (CPC)"]

    cols = ["Campaign Name","Match Type","Targeting","Customer Search Term",
            "Impressions","Clicks","Spend","Current Bid","Cost Per Click (CPC)",
            "CTR","CVR","7 Day Total Sales","ROAS","Action","New Bid"]
    return exact_df[cols].reset_index(drop=True)

# -----------------------------
# PHASE 2 — Broad/Phrase aggregation
# -----------------------------
def run_phase2(df: pd.DataFrame):
    if CONFIG.get("DEBUG_MODE"):
        print("▶ Running Phase 2: Broad & Phrase Optimization...")
    d = df.copy()
    for c in ["Match Type","Campaign Name","Customer Search Term","Impressions","Clicks","Spend","Cost Per Click (CPC)","7 Day Total Sales","7 Day Total Orders (#)"]:
        if c not in d.columns:
            d[c] = 0 if c not in ["Match Type","Campaign Name","Customer Search Term"] else ""
    mask = d["Match Type"].astype(str).str.contains("broad|phrase", case=False, na=False)
    pb = d[mask].copy()
    if pb.empty:
        return pd.DataFrame()
    # normalize kw (2-token theme)
    def kw_theme(s, n_tokens=2):
        t = normalize_text(str(s))
        tokens = [tok for tok in t.split() if len(tok)>2]
        if not tokens:
            tokens = t.split()
        return " ".join(tokens[:n_tokens])
    pb["Normalized_KW"] = pb["Customer Search Term"].apply(lambda x: kw_theme(x, n_tokens=2))
    for c in ["Impressions","Clicks","Spend","Cost Per Click (CPC)","7 Day Total Sales","7 Day Total Orders (#)"]:
        pb[c] = safe_numeric(pb[c])
    agg = pb.groupby(["Campaign Name","Normalized_KW"], as_index=False).agg({
        "Impressions":"sum","Clicks":"sum","Spend":"sum",
        "Cost Per Click (CPC)":"mean","7 Day Total Sales":"sum","7 Day Total Orders (#)":"sum"
    })
    agg["CTR"] = np.where(agg["Impressions"]>0, agg["Clicks"]/agg["Impressions"],0)
    agg["CVR"] = np.where(agg["Clicks"]>0, agg["7 Day Total Orders (#)"]/agg["Clicks"],0)
    agg["ROAS"] = np.where(agg["Spend"]>0, agg["7 Day Total Sales"]/agg["Spend"],0)
    agg["Campaign Median ROAS"] = agg.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)

    def agg_action(row):
        median = row["Campaign Median ROAS"] if row["Campaign Median ROAS"]>0 else 1e-9
        if row["Clicks"] < CONFIG["CLICK_THRESHOLD"] or row["Spend"] < CONFIG["SPEND_MULTIPLIER"] * (row.get("Cost Per Click (CPC)",1) or 1):
            return "Stay Put"
        if row["ROAS"] >= 1.2 * median:
            return "Bid Up"
        if row["ROAS"] <= 0.8 * median:
            return "Bid Down"
        return "Hold"

    agg["Action"] = agg.apply(agg_action, axis=1)

    def agg_new_bid(row):
        bid = float(row.get("Cost Per Click (CPC)",0) or 0)
        target = row["Campaign Median ROAS"]
        actual = row["ROAS"]
        if bid <= 0 or target <= 0 or actual <= 0:
            return bid
        delta = (actual - target)/target
        alpha = CONFIG["ALPHA_BROAD_PHRASE"]
        new = bid * (1 + alpha * delta)
        lower = bid * (1 - CONFIG["MAX_BID_CHANGE"])
        upper = bid * (1 + CONFIG["MAX_BID_CHANGE"])
        return max(lower, min(upper, new))

    agg["New Bid"] = agg.apply(agg_new_bid, axis=1)
    agg["Current Bid"] = agg["Cost Per Click (CPC)"]

    cols = ["Campaign Name","Normalized_KW","Impressions","Clicks","Spend","Current Bid","Cost Per Click (CPC)","CTR","CVR","7 Day Total Sales","ROAS","Campaign Median ROAS","Action","New Bid"]
    return agg[cols].reset_index(drop=True)

# -----------------------------
# STAGE 5 — Auto clustering (patched with routing)
# -----------------------------
def run_auto_cluster(df: pd.DataFrame, min_clicks=10, min_impr=250, min_spend_mult=3.0, jaccard_threshold=0.3):
    if CONFIG.get("DEBUG_MODE"):
        print("▶ Running Stage 5: Auto Term Grouping...")
    d = df.copy()
    if "Targeting" not in d.columns:
        d["Targeting"] = ""
    d["Targeting"] = d["Targeting"].astype(str)
    auto_mask = d["Targeting"].str.contains(r"close-match|loose-match|substitutes|complements|category=", case=False, na=False)
    auto_df = d[auto_mask].copy()
    if len(auto_df) < CONFIG["AUTO_CLUSTER_MIN_ROWS"]:
        if CONFIG.get("DEBUG_MODE"):
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
        clusters.append({
            "ClusterID": gid,
            "RepresentativeTerm": rep,
            "TermCount": len(grp),
            "Clicks": clicks,
            "Impressions": impr,
            "Spend": spend,
            "Sales": sales,
            "Ready": ready
        })

    clusters_df = pd.DataFrame(clusters)

    # -------------------------
    # Routing logic for clusters
    # -------------------------
    campaigns = d["Campaign Name"].dropna().unique().tolist() if "Campaign Name" in d.columns else []
    exact_like = []
    for c in campaigns:
        if re.search(r'\bexact\b', str(c), flags=re.I) or re.search(r'\bexac', str(c), flags=re.I) or re.search(r'_ex', str(c), flags=re.I):
            exact_like.append(c)
    exact_campaigns_from_rows = d[d.get("Match Type", "").astype(str).str.contains("exact", case=False, na=False)]["Campaign Name"].dropna().unique().tolist() if "Match Type" in d.columns else []
    for c in exact_campaigns_from_rows:
        if c not in exact_like:
            exact_like.append(c)
    exact_like = list(dict.fromkeys(exact_like))

    def tokens_of(s: str):
        s = re.sub(r"[^a-z0-9 ]", " ", str(s).lower())
        return {t for t in s.split() if len(t) > 2}

    def route_campaign_for_term(term: str, src_campaign_name: str = ""):
        term = str(term)
        src = str(src_campaign_name)
        asin = extract_asin(term) or extract_asin(src)
        # 1) ASIN direct match on campaign names
        if asin:
            for c in campaigns:
                if asin in str(c).upper():
                    return c, "ASIN-match"
        # 2) fuzzy token overlap against exact_like candidates
        tokens_term = tokens_of(term)
        tokens_src = tokens_of(src)
        best, best_score = None, 0
        for cand in exact_like:
            cand_tokens = tokens_of(cand)
            score = len(tokens_term & cand_tokens) * 2 + len(tokens_src & cand_tokens)
            if score > best_score:
                best, best_score = cand, score
        # you can increase threshold here if you want stricter matches (e.g. best_score >= 2)
        if best_score > 0:
            return best, "Fuzzy-match"
        # 3) suggested new campaign name based on source campaign prefix or fallback
        tokens = re.split(r'[_\-| ]+', src) if src else []
        prefix = "_".join(tokens[:2]).strip() if tokens and len(tokens) > 1 else (tokens[0] if tokens else "AutoCluster")
        suggested = f"{prefix}_Exact_Harvest_{datetime.today().strftime('%b%y')}"
        return suggested, "Suggested-New"

    # Apply routing for all clusters (use RepresentativeTerm and pick a likely source campaign by looking up the term)
    if not clusters_df.empty:
        routed = []
        for _, row in clusters_df.iterrows():
            term = row["RepresentativeTerm"] or ""
            # find a source campaign from auto_df where this term appears (best-effort)
            src_campaign = ""
            matches = auto_df[auto_df["Customer Search Term"].astype(str).str.strip().str.lower() == str(term).strip().lower()]["Campaign Name"].unique().tolist()
            if matches:
                src_campaign = matches[0]
            suggested, method = route_campaign_for_term(term, src_campaign)
            routed.append((suggested, method))
        if routed:
            clusters_df["Suggested Exact Campaign"], clusters_df["Routing Method"] = zip(*routed)
        else:
            clusters_df["Suggested Exact Campaign"] = ""
            clusters_df["Routing Method"] = ""

    if CONFIG.get("DEBUG_MODE"):
        print(f"✅ Stage 5 complete — {len(clusters_df)} clusters formed, {clusters_df['Ready'].sum() if not clusters_df.empty else 0} ready.")
    return clusters_df


def build_optimization_summary(outputs, df_input):
    summary_rows = []

    total_input = len(df_input)
    safe_len = lambda df: len(df) if isinstance(df, pd.DataFrame) else 0

    def safe_avg_roas(df):
        if isinstance(df, pd.DataFrame) and "ROAS" in df.columns and not df.empty:
            return round(df["ROAS"].replace([np.inf, -np.inf], np.nan).fillna(0).mean(), 2)
        return "—"

    # ---- Phase 1 ----
    p1 = outputs.get("Phase1_Promote", pd.DataFrame())
    p1_sum = outputs.get("Phase1_Summary", pd.DataFrame())
    if not p1.empty:
        total_disc = int(p1_sum.loc[p1_sum["Metric"] == "Total Discovery Rows", "Value"].values[0]) \
            if ("Metric" in p1_sum.columns and "Total Discovery Rows" in p1_sum["Metric"].values) else total_input
        promote = safe_len(p1)
        summary_rows.append({
            "Phase": "Phase 1 — Discovery Harvest",
            "Total Inputs": total_disc,
            "Actionable": f"{promote} Promote",
            "% of Total": f"{round(promote / total_input * 100, 2)}%",
            "Avg ROAS": safe_avg_roas(p1),
            "Key Insight": "Good harvest" if promote > 50 else "Limited harvest",
            "Recommended Next Step": "Campaign Creation (Exact/PT)"
        })

    # ---- Phase 1.5 ----
    p15 = outputs.get("Phase1.5_ExactBids", pd.DataFrame())
    if not p15.empty:
        bid_up = (p15["Action"].str.contains("Up", case=False, na=False)).sum() if "Action" in p15.columns else 0
        bid_down = (p15["Action"].str.contains("Down", case=False, na=False)).sum() if "Action" in p15.columns else 0
        actionable = f"{bid_up + bid_down} Bids"
        summary_rows.append({
            "Phase": "Phase 1.5 — Exact/PT Optimization",
            "Total Inputs": safe_len(p15),
            "Actionable": actionable,
            "% of Total": f"{round(safe_len(p15) / total_input * 100, 2)}%",
            "Avg ROAS": safe_avg_roas(p15),
            "Key Insight": "Tight targeting" if (bid_up + bid_down) > 30 else "Low data",
            "Recommended Next Step": "Bid Optimization"
        })

    # ---- Phase 2 ----
    p2 = outputs.get("Phase2_PB_Bids", pd.DataFrame())
    if not p2.empty:
        bid_chg = (p2["Action"].str.contains("Bid", case=False, na=False)).sum() if "Action" in p2.columns else 0
        summary_rows.append({
            "Phase": "Phase 2 — Broad/Phrase Optimization",
            "Total Inputs": safe_len(p2),
            "Actionable": f"{bid_chg} Bids",
            "% of Total": f"{round(bid_chg / total_input * 100, 2)}%",
            "Avg ROAS": safe_avg_roas(p2),
            "Key Insight": "Healthy coverage" if bid_chg > 100 else "Limited adjustments",
            "Recommended Next Step": "Bid Optimization (Broad/Phrase)"
        })

    # ---- Stage 5 ----
    auto = outputs.get("Stage5_Auto_Clusters", pd.DataFrame())
    if not auto.empty:
        ready = int(auto.get("Ready", False).sum())
        summary_rows.append({
            "Phase": "Stage 5 — Auto Clustering",
            "Total Inputs": safe_len(auto),
            "Actionable": f"{ready} Clusters",
            "% of Total": f"{round(ready / total_input * 100, 2)}%",
            "Avg ROAS": "—",
            "Key Insight": "Strong pattern" if ready > 5 else "Few ready clusters",
            "Recommended Next Step": "New Campaign Creation (Auto→Exact)"
        })

    # ---- Negatives ----
    negs = outputs.get("Negatives_To_Apply", pd.DataFrame())
    if not negs.empty:
        conf = (negs["Status"].str.contains("Confirm", case=False, na=False)).sum() if "Status" in negs.columns else 0
        summary_rows.append({
            "Phase": "Negatives Engine",
            "Total Inputs": safe_len(negs),
            "Actionable": f"{conf} Confirm",
            "% of Total": f"{round(safe_len(negs) / total_input * 100, 2)}%",
            "Avg ROAS": safe_avg_roas(df_input[df_input["Customer Search Term"].isin(negs["TermOrASIN"])]) if "Customer Search Term" in df_input.columns else "—",
            "Key Insight": "Waste reduction" if conf > 10 else "Monitor",
            "Recommended Next Step": "Apply Negatives in Source Campaigns"
        })

    df = pd.DataFrame(summary_rows)
    cols = ["Phase", "Total Inputs", "Actionable", "% of Total", "Avg ROAS", "Key Insight", "Recommended Next Step"]
    return df[cols]

# ==========================================================
# MAIN RUN — Clean, stable with Stage5 auto-negative patch
# ==========================================================
if __name__ == "__main__":
    file_path = "Sponsored_Products_Search_term_report.xlsx"
    Path(CONFIG["OUTPUT_DIR"]).mkdir(exist_ok=True)
    outputs, errors, phase_stats = {}, {}, []

    try:
        df_input = pd.read_excel(file_path)
    except FileNotFoundError:
        print(f"⚠️ File not found: {file_path}. Please check your input path or file name.")
        raise

    # -----------------------------
    # INPUT SANITIZATION
    # -----------------------------
    df_input.columns = df_input.columns.map(lambda c: "" if pd.isna(c) else str(c).strip())
    empty_cols = [c for c in df_input.columns if c == ""]
    if empty_cols:
        df_input = df_input.drop(columns=empty_cols)

    core_cols = [
        "Campaign Name", "Match Type", "Customer Search Term", "Targeting",
        "Impressions", "Clicks", "Cost Per Click (CPC)", "Spend",
        "7 Day Total Sales", "7 Day Total Orders (#)"
    ]
    for c in core_cols:
        if c not in df_input.columns:
            df_input[c] = 0 if c in [
                "Impressions", "Clicks", "Cost Per Click (CPC)",
                "Spend", "7 Day Total Sales", "7 Day Total Orders (#)"
            ] else ""

    for c in ["Impressions", "Clicks", "Cost Per Click (CPC)", "Spend", "7 Day Total Sales", "7 Day Total Orders (#)"]:
        df_input[c] = safe_numeric(df_input[c])

    df_input = flatten_dataframe_columns(df_input).reset_index(drop=True)

    # ==========================================================
    # PHASE 1 — Unified Harvest
    # ==========================================================
    try:
        promote, summary, unmatched = run_phase1(df_input)
        outputs["Phase1_Promote"] = promote
        outputs["Phase1_Summary"] = summary
        outputs["Phase1_Unmatched"] = unmatched
        readable_rows = 0
        if "Rows Passing Readability Filter" in summary["Metric"].values:
            readable_rows = int(summary.loc[summary["Metric"] == "Rows Passing Readability Filter", "Value"].values[0])
        phase_stats.append({"Phase": "Phase1", "PromoteRows": len(promote), "ReadableRows": readable_rows})
        print(f"Phase 1 completed ✅ — Promote rows: {len(promote)}")
    except Exception as e:
        errors["Phase1"] = str(e)
        print(f"❌ Phase 1 failed: {e}")

    # ==========================================================
    # STAGE 5 — Auto Clustering + Negatives Patch
    # ==========================================================
    try:
        auto_clusters = run_auto_cluster(df_input)
        outputs["Stage5_Auto_Clusters"] = auto_clusters

        # ---------- PATCH: Append Auto-Cluster Negatives ----------
        def append_auto_cluster_negatives(auto_clusters, df_input, promote_df):
            """Convert ready Stage5 clusters into NEG_EXACT negatives for source Auto campaigns."""
            if auto_clusters is None or auto_clusters.empty:
                return promote_df.attrs.get("Negatives_To_Apply", pd.DataFrame())

            ready = auto_clusters[auto_clusters.get("Ready", False)].copy()
            if ready.empty:
                return promote_df.attrs.get("Negatives_To_Apply", pd.DataFrame())

            neg_rows = []
            df_input["Customer Search Term"] = df_input["Customer Search Term"].astype(str)
            df_input["Targeting"] = df_input.get("Targeting", "").astype(str)
            df_input["Campaign Name"] = df_input["Campaign Name"].astype(str)

            for _, r in ready.iterrows():
                term = str(r.get("RepresentativeTerm", "")).strip()
                if not term:
                    continue
                term_lower = term.lower()

                # find source campaigns
                srcs = df_input[
                    df_input["Customer Search Term"].astype(str).str.strip().str.lower() == term_lower
                ]["Campaign Name"].unique().tolist()

                auto_mask = df_input["Targeting"].astype(str).str.contains(
                    r"close-match|loose-match|substitutes|complements|category=", case=False, na=False
                )
                if not srcs:
                    candidate_df = df_input[auto_mask]
                    srcs = candidate_df[
                        candidate_df["Customer Search Term"].astype(str).str.lower().str.contains(re.escape(term_lower))
                    ]["Campaign Name"].unique().tolist()

                if not srcs:
                    srcs = df_input[auto_mask]["Campaign Name"].unique().tolist()

                for sc in srcs:
                    neg_rows.append({
                        "SourceCampaign": sc,
                        "NegativeType": "NEG_EXACT",
                        "TermOrASIN": term,
                        "Reason": "AutoCluster→NewCampaign",
                        "Status": "Pending-Validate",
                        "ValidationClicksRequired": CONFIG.get("MIN_VALIDATION_CLICKS", 5),
                        "DelayCycles": CONFIG.get("NEGATIVE_DELAY_CYCLES", 1),
                        "Cluster_TermCount": int(r.get("TermCount", 0)),
                        "Cluster_Clicks": int(r.get("Clicks", 0)),
                        "Cluster_Impressions": int(r.get("Impressions", 0)),
                        "Cluster_Spend": float(r.get("Spend", 0.0))
                    })

            auto_neg_df = pd.DataFrame(neg_rows).drop_duplicates(subset=["SourceCampaign", "TermOrASIN"])

            existing = promote_df.attrs.get("Negatives_To_Apply", pd.DataFrame())
            if existing is None or existing.empty:
                combined = auto_neg_df
            else:
                combined = pd.concat([existing, auto_neg_df], ignore_index=True, sort=False)
                priority = {
                    "Review-Confirm": 4, "Confirm": 3, "Confirm Negative": 3,
                    "Pending-Validate": 2, "Pending": 1, "Pending-Manual": 1
                }
                combined["_status_pr"] = combined["Status"].map(priority).fillna(0).astype(int)
                combined = (
                    combined.sort_values("_status_pr", ascending=False)
                    .drop_duplicates(subset=["SourceCampaign", "TermOrASIN"], keep="first")
                    .drop(columns=["_status_pr"])
                )

            if combined is None or combined.empty:
                combined = pd.DataFrame(columns=[
                    "SourceCampaign", "NegativeType", "TermOrASIN", "Reason",
                    "Status", "ValidationClicksRequired", "DelayCycles"
                ])
            promote_df.attrs["Negatives_To_Apply"] = combined.reset_index(drop=True)
            return combined

        # integrate auto-negatives
        try:
            promote_df = outputs.get("Phase1_Promote", pd.DataFrame())
            if isinstance(promote_df, pd.DataFrame):
                combined_negatives = append_auto_cluster_negatives(auto_clusters, df_input, promote_df)
                if not combined_negatives.empty:
                    outputs["Negatives_To_Apply"] = combined_negatives
        except Exception as e:
            print(f"⚠️ Could not append auto-cluster negatives: {e}")

        phase_stats.append({"Phase": "Stage5", "ClusterCount": len(auto_clusters)})
        print("Stage 5 completed ✅")
    except Exception as e:
        errors["Stage5"] = str(e)
        outputs["Stage5_Auto_Clusters"] = pd.DataFrame()
        print(f"❌ Stage 5 failed: {e}")

    # ==========================================================
    # PHASE 1.5 — Exact/PT Optimization
    # ==========================================================
    try:
        p15 = run_phase1_5(df_input)
        outputs["Phase1.5_ExactBids"] = p15
        phase_stats.append({"Phase": "Phase1.5", "Rows": len(p15)})
        print("Phase 1.5 completed ✅")
    except Exception as e:
        errors["Phase1.5"] = str(e)
        outputs["Phase1.5_ExactBids"] = pd.DataFrame()
        print(f"❌ Phase 1.5 failed: {e}")

    # ==========================================================
    # PHASE 2 — Broad/Phrase Optimization
    # ==========================================================
    try:
        p2 = run_phase2(df_input)
        outputs["Phase2_PB_Bids"] = p2
        phase_stats.append({"Phase": "Phase2", "Rows": len(p2)})
        print("Phase 2 completed ✅")
    except Exception as e:
        errors["Phase2"] = str(e)
        outputs["Phase2_PB_Bids"] = pd.DataFrame()
        print(f"❌ Phase 2 failed: {e}")

    # ==========================================================
    # NEGATIVE SHEETS ATTACHMENT
    # ==========================================================
    try:
        if isinstance(promote, pd.DataFrame):
            outputs["Watch_Negatives"] = promote.attrs.get("Watch_Negatives", pd.DataFrame())
            outputs["Confirm_Negatives"] = promote.attrs.get("Confirm_Negatives", pd.DataFrame())
            outputs["Negatives_To_Apply"] = promote.attrs.get("Negatives_To_Apply", pd.DataFrame())
    except Exception as e:
        errors["NegativesAttach"] = str(e)

    # ==========================================================
    # PHASE SUMMARY
    # ==========================================================
    try:
        phase_summary = {
            "Run Timestamp": datetime.now().isoformat(),
            "Total Input Rows": len(df_input),
            "Total Discovery Rows": int(
                outputs.get("Phase1_Summary", pd.DataFrame())
                .loc[outputs.get("Phase1_Summary", pd.DataFrame())["Metric"] == "Total Discovery Rows", "Value"]
                .values[0]
            ) if ("Phase1_Summary" in outputs and not outputs["Phase1_Summary"].empty) else len(df_input),
            "Promote Candidates": len(outputs.get("Phase1_Promote", pd.DataFrame())),
            "Phase1.5 Optimized Rows": len(outputs.get("Phase1.5_ExactBids", pd.DataFrame())),
            "Phase2 Aggregated Rows": len(outputs.get("Phase2_PB_Bids", pd.DataFrame())),
            "Stage5 Clusters": len(outputs.get("Stage5_Auto_Clusters", pd.DataFrame())),
        }
    except Exception as e:
        phase_summary = {"Run Timestamp": datetime.now().isoformat(), "ErrorBuildingSummary": str(e)}

    # === Build Optimization Summary (human-friendly, table view) ===
    try:
        opt_summary = build_optimization_summary(outputs, df_input)
        # add to outputs so it gets written to the workbook
        outputs["Optimization_Summary"] = opt_summary
        if CONFIG.get("DEBUG_MODE"):
            print("Optimization_Summary built:", opt_summary.to_dict(orient="records"))
    except Exception as e:
        print(f"⚠️ Could not build optimization summary: {e}")
        outputs["Optimization_Summary"] = pd.DataFrame([{"Info": "Could not build optimization summary"}])


    # ==========================================================
    # OUTPUT WORKBOOK
    # ==========================================================
    out_file = f"{CONFIG['OUTPUT_DIR']}/s2c_full_run_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
        for name, df_out in outputs.items():
            try:
                if isinstance(df_out, pd.DataFrame) and not df_out.empty:
                    df_out.to_excel(writer, sheet_name=name[:31], index=False)
                else:
                    pd.DataFrame([{"Info": f"No rows for {name}"}]).to_excel(
                        writer, sheet_name=name[:31], index=False
                    )
            except Exception as e:
                print(f"⚠️ Could not write sheet {name}: {e}")

        pd.DataFrame(phase_stats).to_excel(writer, sheet_name="Phase_Stats", index=False)
        pd.DataFrame([phase_summary]).to_excel(writer, sheet_name="Phase_Summary", index=False)
        if errors:
            pd.DataFrame(
                [{"Phase": k, "Error": v} for k, v in errors.items()]
            ).to_excel(writer, sheet_name="Errors", index=False)

    print(f"\n✅ Full workflow complete. Output saved to: {out_file}")
    if errors:
        print("⚠️ Some phases failed — check 'Errors' tab.")
    else:
        print("🎯 All phases completed successfully.")
