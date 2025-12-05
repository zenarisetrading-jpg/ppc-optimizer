#!/usr/bin/env python3
"""s2c_ppc_optimizer_v2_5_3.py — S2C PPC Optimizer (v2.5.3)
- Survivor-only Phase1 (dedupe against existing exact keywords)
- Manual PT handling (flag only)
- No fuzzy routing in Phase1 or Stage5 (manual review required)
- Other phases preserved from v2.5.1
Author: Aslam + GPT-5 Thinking mini
Version: 2.5.3
"""

import pandas as pd
import numpy as np
import re
import difflib
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
    "OUTPUT_DIR": "./outputs_v2_5_3",
    "DEBUG_MODE": False,
    "MIN_VALIDATION_CLICKS": 5,
    "NEGATIVE_DELAY_CLICLES": 1,
    "NEGATIVE_ROAS_CEILING": 3.0,
    "DEDUPE_SIMILARITY": 0.90,  # 90% similarity threshold
}

# -----------------------------
# UTILITIES
# -----------------------------
def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(r"[^0-9\.\-]", "", regex=True),
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

def tokens_of(s: str):
    s = re.sub(r"[^a-z0-9 ]", " ", str(s).lower())
    return {t for t in s.split() if len(t) > 2}

def similarity(a: str, b: str) -> float:
    if not a or not b: return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()

def extract_ids_from_names(name: str):
    """Best-effort extraction of IDs from campaign/adgroup names.
    Returns (id1, id2) — if none found returns empty strings.
    """
    if not isinstance(name, str):
        return "", ""
    # Look for long numbers or asin
    num_match = re.search(r"(?:id[_:=-]?|_)(\d{3,})", name, flags=re.I)
    if num_match:
        return num_match.group(1), num_match.group(1)
    asin = extract_asin(name)
    if asin:
        return asin, asin
    return "", ""

def flatten_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns.tolist():
        val = df[col]
        if isinstance(val, pd.DataFrame):
            df[col] = val.astype(str).apply(lambda row: " ".join([str(x) for x in row.values if str(x).strip()]), axis=1)
        elif hasattr(val, "dtype") and val.dtype == "O":
            df[col] = val.apply(lambda x: " ".join(x) if isinstance(x, (list, tuple)) else ("" if pd.isna(x) else str(x)))
    return df

# -----------------------------
# PHASE 1 — Survivor-only: discover -> promote -> dedupe against existing exacts
# -----------------------------
def run_phase1_survivor(df: pd.DataFrame):
    df = df.copy()
    df.columns = df.columns.map(lambda c: "" if pd.isna(c) else str(c).strip())

    # Ensure required columns
    for c in ["Campaign Name", "Match Type", "Targeting", "Customer Search Term", "Ad Group Name"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].astype(str)

    # Numeric safeties
    num_cols = ["Impressions","Clicks","Spend","Cost Per Click (CPC)","7 Day Total Sales","7 Day Total Orders (#)"]
    for nc in num_cols:
        if nc not in df.columns:
            df[nc] = 0
        df[nc] = safe_numeric(df[nc])

    df = flatten_dataframe_columns(df)
    df["CTR"] = np.where(df["Impressions"]>0, df["Clicks"]/df["Impressions"], 0.0)
    df["CVR"] = np.where(df["Clicks"]>0, df["7 Day Total Orders (#)"]/df["Clicks"], 0.0)
    df["ROAS"] = np.where(df["Spend"]>0, df["7 Day Total Sales"]/df["Spend"], 0.0)
    df["Campaign Median ROAS"] = df.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)

    # Discovery mask
    auto_pattern = r'close-match|loose-match|substitutes|complements|category=|asin|b0[a-z0-9]{8}'
    discovery_mask = (~df["Match Type"].str.contains("exact", case=False, na=False)) | (df["Targeting"].str.contains(auto_pattern, case=False, na=False))
    discovery_df = df[discovery_mask].copy()

    discovery_df["CTR"] = np.where(discovery_df["Impressions"]>0, discovery_df["Clicks"]/discovery_df["Impressions"], 0.0)
    discovery_df["CVR"] = np.where(discovery_df["Clicks"]>0, discovery_df["7 Day Total Orders (#)"]/discovery_df["Clicks"], 0.0)
    discovery_df["ROAS"] = np.where(discovery_df["Spend"]>0, discovery_df["7 Day Total Sales"]/discovery_df["Spend"], 0.0)
    discovery_df["ROAS"].replace([np.inf, -np.inf], np.nan, inplace=True)
    discovery_df["ROAS"] = discovery_df["ROAS"].fillna(0.0)
    discovery_global_median = discovery_df["ROAS"].median() if not discovery_df.empty else 0.1
    if discovery_global_median <= 0:
        discovery_global_median = 0.1

    # Readability filter
    avg_cpc = discovery_df["Cost Per Click (CPC)"].replace(0, np.nan).mean()
    if np.isnan(avg_cpc) or avg_cpc <= 0:
        avg_cpc = df["Cost Per Click (CPC)"].replace(0, np.nan).mean()
    if np.isnan(avg_cpc) or avg_cpc <= 0:
        avg_cpc = 0.1

    readable_mask = (
        (discovery_df["Clicks"] >= CONFIG["CLICK_THRESHOLD"]) &
        (discovery_df["Spend"] >= CONFIG["SPEND_MULTIPLIER"] * avg_cpc) &
        (discovery_df["Impressions"] >= CONFIG["IMPRESSION_THRESHOLD"])
    )
    readable_df = discovery_df[readable_mask].copy()

    def classify_action(row):
        median = row["Campaign Median ROAS"] if row["Campaign Median ROAS"]>0 else discovery_global_median
        if row["7 Day Total Sales"] <= 0:
            if row["Clicks"] >= CONFIG["CLICK_THRESHOLD"] and row["Impressions"] >= CONFIG["IMPRESSION_THRESHOLD"] and row["ROAS"]>0:
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
    promote_df = readable_df[readable_df["Action"]=="Promote"].copy()

    # PT flags (only flag, source campaign/adgroup retained)
    promote_df["ASIN_in_term"] = promote_df["Customer Search Term"].apply(extract_asin)
    promote_df["ASIN_in_targeting"] = promote_df["Targeting"].apply(extract_asin)
    promote_df["Category_in_targeting"] = promote_df["Targeting"].str.contains(r'category\s*=\s*"?b0', case=False, na=False)
    promote_df["Is_PT_Candidate"] = promote_df["ASIN_in_term"].notna() | promote_df["ASIN_in_targeting"].notna() | promote_df["Category_in_targeting"]

    # Build list of exact keywords from input (Match Type == EXACT)
    exact_rows = df[df["Match Type"].astype(str).str.contains("exact", case=False, na=False)].copy()
    exact_keywords = exact_rows["Customer Search Term"].astype(str).apply(normalize_text).unique().tolist()

    # Dedupe promote_df against exact_keywords with similarity threshold
    def is_dupe_against_exact(term: str, exact_list: list, thresh=CONFIG["DEDUPE_SIMILARITY"]):
        t = normalize_text(str(term))
        for ek in exact_list:
            if not ek: continue
            if similarity(t, ek) >= thresh:
                return True, ek, similarity(t, ek)
        return False, None, 0.0

    survivors = []
    dedupe_meta = []
    for _, row in promote_df.iterrows():
        term = row.get("Customer Search Term", "")
        dupe, matched_exact, sim = is_dupe_against_exact(term, exact_keywords)
        if not dupe:
            survivors.append(row)
        else:
            dedupe_meta.append({
                "Customer Search Term": term,
                "MatchedExact": matched_exact,
                "Similarity": sim,
                "SourceCampaign": row.get("Campaign Name", ""),
                "Reason": "DuplicateWithExact"
            })

    survivors_df = pd.DataFrame(survivors).reset_index(drop=True)
    dedupe_df = pd.DataFrame(dedupe_meta)

    # Survivors: mark NewCampaignRequired, retain source campaign/adgroup and PT flags, extract IDs best-effort
    if not survivors_df.empty:
        survivors_df["Suggested Action"] = "NewCampaignRequired"
        survivors_df["Suggested Exact Campaign"] = ""
        survivors_df["Campaign ID"], _ = zip(*survivors_df["Campaign Name"].astype(str).apply(extract_ids_from_names))
        survivors_df["AdGroup ID"], _ = zip(*survivors_df.get("Ad Group Name", "").astype(str).apply(extract_ids_from_names))
    else:
        survivors_df = pd.DataFrame(columns=list(promote_df.columns) + ["Suggested Action","Suggested Exact Campaign","Campaign ID","AdGroup ID"])

    # Summary
    summary = pd.DataFrame([
        {"Metric":"Total Discovery Rows","Value":len(discovery_df)},
        {"Metric":"Rows Passing Readability Filter","Value":len(readable_df)},
        {"Metric":"Promote Candidates","Value":len(promote_df)},
        {"Metric":"Survivors after dedupe","Value":len(survivors_df)},
        {"Metric":"Duplicates matched to exact","Value":len(dedupe_df)}
    ])

    promote_df.attrs["Dedupe_Matches"] = dedupe_df
    promote_df.attrs["Survivors"] = survivors_df

    return promote_df.reset_index(drop=True), summary, survivors_df.reset_index(drop=True), dedupe_df.reset_index(drop=True)

# -----------------------------
# Phase1.5, Phase2, Stage5 (kept consistent with prior stable logic)
# For brevity they are implemented similarly to v2.5.1
# -----------------------------
def run_phase1_5(df: pd.DataFrame):
    d = df.copy()
    mask = d["Match Type"].astype(str).str.contains("exact", case=False, na=False) | d["Targeting"].astype(str).str.contains(r"asin|b0[a-z0-9]{8}", case=False, na=False)
    exact_df = d[mask].copy()
    if exact_df.empty:
        return pd.DataFrame()
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

    cols = ["Campaign Name","Ad Group Name","Match Type","Targeting","Customer Search Term","Impressions","Clicks","Spend","Current Bid","Cost Per Click (CPC)","CTR","CVR","7 Day Total Sales","ROAS","Action","New Bid"]
    return exact_df[cols].reset_index(drop=True)

def run_phase2(df: pd.DataFrame):
    d = df.copy()
    mask = d["Match Type"].astype(str).str.contains("broad|phrase", case=False, na=False)
    pb = d[mask].copy()
    if pb.empty:
        return pd.DataFrame()
    def kw_theme(s, n_tokens=2):
        t = normalize_text(str(s))
        tokens = [tok for tok in t.split() if len(tok)>2]
        if not tokens:
            tokens = t.split()
        return " ".join(tokens[:n_tokens])
    pb["Normalized_KW"] = pb["Customer Search Term"].apply(lambda x: kw_theme(x, n_tokens=2))
    for c in ["Impressions","Clicks","Spend","Cost Per Click (CPC)","7 Day Total Sales","7 Day Total Orders (#)"]:
        pb[c] = safe_numeric(pb[c])
    agg = pb.groupby(["Campaign Name","Normalized_KW"], as_index=False).agg({"Impressions":"sum","Clicks":"sum","Spend":"sum","Cost Per Click (CPC)":"mean","7 Day Total Sales":"sum","7 Day Total Orders (#)":"sum"})
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

def run_auto_cluster(df: pd.DataFrame, min_clicks=10, min_impr=250, min_spend_mult=3.0, jaccard_threshold=0.3):
    d = df.copy()
    if "Targeting" not in d.columns:
        d["Targeting"] = ""
    auto_mask = d["Targeting"].str.contains(r"close-match|loose-match|substitutes|complements|category=", case=False, na=False)
    auto_df = d[auto_mask].copy()
    if len(auto_df) < CONFIG["AUTO_CLUSTER_MIN_ROWS"]:
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
        sales = float(grp.get("7 Day Total Sales", 0).sum()) if not grp.empty else 0.0
        rep = grp["Customer Search Term"].iloc[0] if not grp.empty else ""
        ready = (clicks >= min_clicks) and (spend >= min_spend_mult * avg_cpc) and (impr >= min_impr)
        clusters.append({"ClusterID": gid, "RepresentativeTerm": rep, "TermCount": len(grp), "Clicks": clicks, "Impressions": impr, "Spend": spend, "Sales": sales, "Ready": ready})
    clusters_df = pd.DataFrame(clusters)
    # Routing intentionally disabled for v2.5.3 — clusters for manual review
    return clusters_df

# -----------------------------
# MAIN RUN
# -----------------------------
def run_all(file_path: str):
    Path(CONFIG["OUTPUT_DIR"]).mkdir(parents=True, exist_ok=True)
    try:
        df_input = pd.read_excel(file_path)
    except Exception as e:
        raise SystemExit(f"Could not read input file: {e}")

    # sanitize and ensure core columns
    df_input.columns = df_input.columns.map(lambda c: "" if pd.isna(c) else str(c).strip())
    core_cols = ["Campaign Name","Match Type","Customer Search Term","Targeting","Ad Group Name","Impressions","Clicks","Cost Per Click (CPC)","Spend","7 Day Total Sales","7 Day Total Orders (#)"]
    for c in core_cols:
        if c not in df_input.columns:
            df_input[c] = "" if c in ["Campaign Name","Match Type","Customer Search Term","Targeting","Ad Group Name"] else 0
    for c in ["Impressions","Clicks","Cost Per Click (CPC)","Spend","7 Day Total Sales","7 Day Total Orders (#)"]:
        df_input[c] = safe_numeric(df_input[c])
    df_input = flatten_dataframe_columns(df_input).reset_index(drop=True)

    outputs = {}
    # Phase 1 survivor-only
    promote_df, summary_df, survivors_df, dedupe_matches_df = run_phase1_survivor(df_input)
    outputs['Phase1_Promote_All'] = promote_df
    outputs['Phase1_Summary'] = summary_df
    outputs['Phase1_Survivors'] = survivors_df
    outputs['Phase1_DedupeMatches'] = dedupe_matches_df

    # Stage5 clusters (manual review)
    clusters = run_auto_cluster(df_input)
    outputs['Stage5_Auto_Clusters'] = clusters

    # Phase1.5 (exact/PT optimization) - runs on exact rows from input
    p15 = run_phase1_5(df_input)
    outputs['Phase1.5_ExactBids'] = p15

    # Phase2
    p2 = run_phase2(df_input)
    outputs['Phase2_PB_Bids'] = p2

    # Write output workbook
    out_file = f"{CONFIG['OUTPUT_DIR']}/s2c_full_run_v2_5_3_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    with pd.ExcelWriter(out_file, engine='openpyxl') as writer:
        for name, df_out in outputs.items():
            try:
                if isinstance(df_out, pd.DataFrame) and not df_out.empty:
                    df_out.to_excel(writer, sheet_name=name[:31], index=False)
                else:
                    pd.DataFrame([{"Info": f"No rows for {name}"}]).to_excel(writer, sheet_name=name[:31], index=False)
            except Exception as e:
                print(f"Could not write sheet {name}: {e}")
    return out_file, outputs

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print("Usage: python s2c_ppc_optimizer_v2_5_3.py <Sponsored_Products_Search_term_report.xlsx>")
        raise SystemExit(1)
    fp = sys.argv[1]
    out, outputs = run_all(fp)
    print("✅ v2.5.3 run complete. Output:", out)