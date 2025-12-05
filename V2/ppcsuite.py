import streamlit as st
import pandas as pd
import numpy as np
import re
import difflib
from io import BytesIO
from collections import defaultdict
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from openai import OpenAI

# ==========================================
# 0. PAGE CONFIG & STATE MANAGEMENT
# ==========================================
st.set_page_config(page_title="S2C LaunchPad Suite", layout="wide", page_icon="🚀")

# --- CUSTOM CSS FOR UI ---
st.markdown("""
<style>
    .main-header { text-align: center; margin-bottom: 30px; }
    .block-container { padding-top: 2rem; }
    
    /* Landing Page "Clickable" Cards via Buttons */
    div.stButton > button.landing-card {
        height: 240px;
        width: 100%;
        background-color: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
        color: #1f2937;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        transition: all 0.3s ease;
    }
    div.stButton > button.landing-card:hover {
        border-color: #6366f1;
        box-shadow: 0 10px 15px rgba(0, 0, 0, 0.1);
        transform: translateY(-3px);
        color: #6366f1;
    }
    
    /* Tab Blurb Styling */
    .tab-blurb {
        background-color: #f0f9ff;
        border-left: 4px solid #0ea5e9;
        padding: 15px;
        border-radius: 4px;
        margin-bottom: 20px;
        color: #0c4a6e;
    }
    .action-box {
        background-color: #f0fdf4;
        border: 1px solid #bbf7d0;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 20px;
    }
    .action-title {
        font-weight: bold;
        color: #166534;
        margin-bottom: 5px;
    }
</style>
""", unsafe_allow_html=True)

if 'current_module' not in st.session_state:
    st.session_state['current_module'] = 'home'

def navigate_to(module):
    st.session_state['current_module'] = module

# ==========================================
# 1. SHARED UTILITIES
# ==========================================
def safe_numeric(series: pd.Series) -> pd.Series:
    clean = series.astype(str).str.replace(r"[^0-9\.\-]", "", regex=True)
    clean = clean.replace("", "0")
    return pd.to_numeric(clean, errors="coerce").fillna(0)

def normalize_text(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.lower().strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def get_tokens(s: str) -> set:
    tokens = {t for t in normalize_text(str(s)).split() if len(t) > 2}
    return {t for t in tokens if not t.startswith('b0')}

def tokens_sorted_string(s: str, n_tokens=None) -> str:
    tokens = sorted(list(get_tokens(s)))
    if n_tokens: tokens = tokens[:n_tokens]
    return " ".join(tokens)

def find_col(df, keywords):
    for col in df.columns:
        for kw in keywords:
            if kw.lower() in col.lower(): return col
    return None

def is_asin(s: str) -> bool:
    return bool(re.search(r'\bb0[a-z0-9]{8}\b', str(s).lower()))

def extract_asin_from_text(text: str) -> str:
    """Extracts B0XXXXXXXX string from text if present."""
    if not isinstance(text, str): return None
    match = re.search(r'(?:_|\b)(B0[A-Z0-9]{8})(?:_|\b)', text, re.IGNORECASE)
    return match.group(1).upper() if match else None

def flatten_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns.tolist():
        val = df[col]
        if hasattr(val, "dtype") and val.dtype == "O":
            df[col] = val.apply(lambda x: " ".join(map(str, x)) if isinstance(x, (list, tuple, set)) else str(x) if not pd.isna(x) else "")
    return df

def to_excel_download(df_or_dict, filename_prefix="data"):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        if isinstance(df_or_dict, dict):
            for name, df in df_or_dict.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    df.to_excel(writer, sheet_name=name[:30], index=False)
        elif isinstance(df_or_dict, pd.DataFrame):
             df_or_dict.to_excel(writer, index=False)
    return output.getvalue()

# ==========================================
# 1.1 ID MAPPING UTILITIES (NEW)
# ==========================================

def parse_id_lookup(file_content):
    """
    Parses the specific CSV format:
    Campaign ID, Ad Group ID, Keyword ID, Campaign Name, Ad Group Name, Keyword Text, Match Type
    Handles suffixes like "(Informational only)" common in Amazon bulk downloads.
    """
    try:
        # Try read CSV first as per user snippet
        try:
            df = pd.read_csv(file_content)
        except:
            file_content.seek(0)
            df = pd.read_excel(file_content)
            
        # Normalize columns: Strip whitespace
        df.columns = [str(c).strip() for c in df.columns]
        
        # Helper to find column loosely
        def find_header(candidates, columns):
            for col in columns:
                for cand in candidates:
                    if cand.lower() in col.lower():
                        return col
            return None

        # Map required fields to actual columns found
        col_map = {
            "Campaign ID": find_header(["Campaign ID"], df.columns),
            "Keyword ID": find_header(["Keyword ID", "Ad ID", "Target Id"], df.columns),
            "Campaign Name": find_header(["Campaign Name"], df.columns),
            "Keyword Text": find_header(["Keyword Text", "Targeting Expression"], df.columns),
            "Ad Group ID": find_header(["Ad Group ID"], df.columns),
            "Match Type": find_header(["Match Type"], df.columns)
        }

        # Check for missing critical columns
        missing = [k for k, v in col_map.items() if v is None and k in ["Campaign ID", "Keyword ID", "Campaign Name"]]
        if missing:
            return None, None, f"Missing required columns: {missing}. Found headers: {list(df.columns)}"
            
        campaign_map = {} # Name -> ID
        keyword_map = {}  # (Campaign Name, Keyword Text) -> {data}
        
        for _, row in df.iterrows():
            c_name_col = col_map["Campaign Name"]
            c_id_col = col_map["Campaign ID"]
            
            c_name = str(row.get(c_name_col, "")).strip()
            c_id = row.get(c_id_col)
            
            if c_name and pd.notna(c_id):
                campaign_map[c_name] = c_id
                
            # Keyword Mapping
            kw_text_col = col_map["Keyword Text"]
            kw_id_col = col_map["Keyword ID"]
            ag_id_col = col_map["Ad Group ID"]
            match_col = col_map["Match Type"]

            kw_text = str(row.get(kw_text_col, "")).strip().lower()
            kw_id = row.get(kw_id_col)
            ag_id = row.get(ag_id_col) if ag_id_col else None
            match_type = str(row.get(match_col, "")).strip().lower() if match_col else ""
            
            if c_name and kw_text and pd.notna(kw_id):
                # We use a tuple key: (Campaign Name, Keyword Text)
                key = (c_name, kw_text)
                keyword_map[key] = {
                    "Keyword ID": kw_id,
                    "Ad Group ID": ag_id,
                    "Campaign ID": c_id,
                    "Match Type": match_type
                }
                
        return campaign_map, keyword_map, None
    except Exception as e:
        return None, None, f"Error parsing ID file: {str(e)}"

def generate_negatives_bulk(negatives_df, campaign_map):
    """
    Generates Bulk 2.0 for Campaign Negative Keywords.
    """
    rows = []
    unmapped_count = 0
    
    for _, row in negatives_df.iterrows():
        camp_name = str(row["Campaign"]).strip()
        term = str(row["Term"]).strip()
        
        camp_id = campaign_map.get(camp_name)
        
        if camp_id:
            rows.append({
                "Product": "Sponsored Products",
                "Entity": "Campaign Negative Keyword",
                "Operation": "Create",
                "Campaign ID": camp_id,
                "Keyword Text": term,
                "Match Type": "negativeExact",
                "State": "Enabled"
            })
        else:
            unmapped_count += 1
            
    df_out = pd.DataFrame(rows)
    # Ensure specific order/columns for Bulk 2.0
    cols = ["Product", "Entity", "Operation", "Campaign ID", "Keyword Text", "Match Type", "State"]
    if not df_out.empty:
        df_out = df_out[cols]
    
    return df_out, unmapped_count

def generate_bids_bulk(bids_df, keyword_map):
    """
    Generates Bulk 2.0 for Keyword/PT Bid Updates.
    Skips any bids where Reason indicates 'Hold'.
    """
    rows = []
    unmapped_count = 0
    skipped_hold_count = 0
    
    for _, row in bids_df.iterrows():
        # SKIP if Reason is Hold
        reason = str(row.get("Reason", "")).lower()
        if "hold" in reason:
            skipped_hold_count += 1
            continue

        camp_name = str(row["Campaign Name"]).strip()
        term = str(row["Customer Search Term"]).strip().lower()
        new_bid = row["New Bid"]
        
        # Lookup
        match = keyword_map.get((camp_name, term))
        
        if match:
            # Determine Entity Type based on content (ASIN or Keyword)
            # The lookup file calls it "Keyword ID" regardless, but Bulk sheet needs "Product Targeting ID" col for targets
            is_pt = "asin=" in term or "category=" in term or is_asin(term)
            
            row_data = {
                "Product": "Sponsored Products",
                "Entity": "Product Targeting" if is_pt else "Keyword",
                "Operation": "Update",
                "Campaign ID": match["Campaign ID"],
                "Ad Group ID": match["Ad Group ID"],
                "Bid": f"{new_bid:.2f}"
            }
            
            if is_pt:
                row_data["Product Targeting ID"] = match["Keyword ID"] # Map ID from file to PT ID column
            else:
                row_data["Keyword ID"] = match["Keyword ID"]
                
            rows.append(row_data)
        else:
            unmapped_count += 1

    df_out = pd.DataFrame(rows)
    cols = ["Product", "Entity", "Operation", "Campaign ID", "Ad Group ID", "Keyword ID", "Product Targeting ID", "Bid"]
    
    # Fill missing cols
    if not df_out.empty:
        for c in cols:
            if c not in df_out.columns: df_out[c] = ""
        df_out = df_out[cols]
        
    return df_out, unmapped_count, skipped_hold_count

# ==========================================
# 2. MODULE A: PPC OPTIMIZER LOGIC
# ==========================================

class ExactMatcher:
    def __init__(self, df: pd.DataFrame):
        match_types = df["Match Type"].astype(str).fillna("")
        exact_rows = df[match_types.str.contains("exact", case=False, na=False)]
        self.exact_keywords = set(exact_rows["Customer Search Term"].astype(str).apply(normalize_text).unique())
        self.token_index = defaultdict(set)
        for kw in self.exact_keywords:
            tokens = get_tokens(kw)
            for t in tokens: self.token_index[t].add(kw)

    def find_match(self, term: str, threshold: float = 0.90):
        norm_term = normalize_text(str(term))
        if not norm_term: return None, 0.0
        term_tokens = get_tokens(norm_term)
        if not term_tokens: return None, 0.0
        
        candidates = set()
        for t in term_tokens:
            if t in self.token_index: candidates.update(self.token_index[t])
        if not candidates: return None, 0.0
        if norm_term in candidates: return norm_term, 1.0

        best_match = None
        best_score = 0.0
        term_len = len(norm_term)
        for cand in candidates:
            if abs(len(cand) - term_len) > 5: continue
            score = difflib.SequenceMatcher(None, norm_term, cand).ratio()
            if score > best_score:
                best_score = score
                best_match = cand
        if best_score >= threshold: return best_match, best_score
        return None, 0.0

def calculate_optimal_bid(row, alpha, policy, max_change, low_vol_boost=0.0):
    current_cpc = float(row.get("Cost Per Click (CPC)", 0) or 0)
    target_roas = row.get("Campaign Median ROAS", 2.5)
    actual_roas = row.get("ROAS", 0)
    clicks = row.get("Clicks", 0)
    
    if current_cpc <= 0: return 0.5, "Default (No CPC Data)"
    if target_roas <= 0: target_roas = 2.5 
    
    if clicks < 5:
        if low_vol_boost > 0:
             new_bid = current_cpc * (1 + low_vol_boost)
             return new_bid, f"Exploration: Low Vol (+{low_vol_boost*100:.0f}%)"
        return current_cpc, "Hold: Insufficient Data"

    new_bid = current_cpc
    reason = "Hold"
    
    if actual_roas > 0:
        delta = (actual_roas - target_roas) / target_roas
        delta = max(-0.5, min(1.0, delta)) 
        if delta > 0:
            new_bid = current_cpc * (1 + alpha * delta)
            reason = f"Performant: ROAS {actual_roas:.2f} > Target"
        elif delta < 0:
            new_bid = current_cpc * (1 + alpha * delta)
            reason = f"Underperform: ROAS {actual_roas:.2f} < Target"
    else:
        if clicks > 10:
             new_bid = current_cpc * (1 - max_change)
             reason = f"Bleeder: {clicks} Clicks, 0 Sales"
        else:
            reason = "Hold: Low Clicks, 0 Sales"
                
    lower_bound = current_cpc * (1 - max_change)
    upper_bound = current_cpc * (1 + max_change)
    final_bid = max(lower_bound, min(upper_bound, new_bid))
    return final_bid, reason

def get_llm_analysis(df, outputs):
    try:
        api_key = st.secrets["OPENAI_API_KEY"]
    except Exception:
        return "⚠️ **OpenAI API Key Missing**. Please add it to `.streamlit/secrets.toml`."

    try:
        client = OpenAI(api_key=api_key)
        tot_spend = df["Spend"].sum()
        tot_sales = df["7 Day Total Sales"].sum()
        roas = tot_sales/tot_spend if tot_spend > 0 else 0
        
        top_winners = df[df["7 Day Total Sales"] > 0].sort_values("ROAS", ascending=False).head(5)
        top_bleeders = df[df["7 Day Total Sales"] == 0].sort_values("Spend", ascending=False).head(5)
        
        neg_spend_saved = 0
        if not outputs['Negatives'].empty:
            neg_terms = outputs['Negatives']['Term'].tolist()
            # Find spend of these terms in the full data
            neg_spend_saved = df[df['Customer Search Term'].isin(neg_terms)]['Spend'].sum()
            
        survivor_rev = 0
        if not outputs['Survivors'].empty:
            survivor_rev = outputs['Survivors']['7 Day Total Sales'].sum()

        opt_stats = {
            "potential_savings": neg_spend_saved,
            "revenue_optimized": survivor_rev,
            "new_harvests": len(outputs.get('Survivors', [])),
            "negatives_added": len(outputs.get('Negatives', [])),
            "bids_updated": len(outputs.get('Exact_PT_Bids', []))
        }

        prompt = f"""
        You are a Senior Amazon PPC Strategist. Analyze this data and write a high-impact Executive Report.
        
        **1. CURRENT ACCOUNT SNAPSHOT (Status Quo):**
        - Total Spend: ${tot_spend:,.2f} | Sales: ${tot_sales:,.2f} | ROAS: {roas:.2f}
        - **Top 5 Winners (High ROAS):** {', '.join(top_winners['Customer Search Term'].astype(str).tolist())}
        - **Top 5 Bleeders (High Spend, 0 Sales):** {', '.join(top_bleeders['Customer Search Term'].astype(str).tolist())}
        
        **2. OPTIMIZATION IMPACT (The "Why"):**
        The optimizer has identified these opportunities:
        - **Wasted Spend to Cut:** ${opt_stats['potential_savings']:,.2f} (Annualized: ${opt_stats['potential_savings']*12:,.2f}) from {opt_stats['negatives_added']} negative terms.
        - **Revenue to Isolate:** ${opt_stats['revenue_optimized']:,.2f} from {opt_stats['new_harvests']} new Exact terms.
        
        **TASK:**
        Write a report in Markdown.
        
        ### 📊 Part 1: Top 5 Performance Insights
        Identify the 5 most critical insights about user intent and account health based on the Winners vs. Bleeders list. Be specific about the terms.
        
        ### 💰 Part 2: Optimization Opportunities & Savings
        Focus purely on the financial impact.
        - Quantify the savings from the negatives found.
        - Explain the upside of harvesting the new Exact terms.
        - List 3 bulleted "Next Steps" for the user.
        """
        response = client.chat.completions.create(
            model="gpt-4o", messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"❌ Error calling OpenAI: {str(e)}"

def expand_dates(df):
    start_col = find_col(df, ["Start Date", "Start"])
    end_col = find_col(df, ["End Date", "End"])
    if not start_col or not end_col: return df
    df[start_col] = pd.to_datetime(df[start_col], errors='coerce')
    df[end_col] = pd.to_datetime(df[end_col], errors='coerce')
    df = df.dropna(subset=[start_col, end_col])
    
    expanded_rows = []
    metric_cols = ["Impressions", "Clicks", "Spend", "Sales", "Orders"]
    
    for _, row in df.iterrows():
        s = row[start_col]; e = row[end_col]
        if s == e:
            row["Date"] = s; expanded_rows.append(row)
        else:
            days = (e - s).days + 1
            if days <= 0: days = 1
            for i in range(days):
                new_row = row.copy()
                new_row["Date"] = s + timedelta(days=i)
                for m in metric_cols:
                    if m in new_row: new_row[m] = row[m] / days
                expanded_rows.append(new_row)
    return pd.DataFrame(expanded_rows)

def aggregate_for_optimization(df):
    group_keys = ["Campaign Name", "Ad Group Name", "Customer Search Term", "Match Type", "Targeting"]
    agg_rules = {"Impressions": "sum", "Clicks": "sum", "Spend": "sum", "7 Day Total Sales": "sum", "7 Day Total Orders": "sum", "Cost Per Click (CPC)": "mean"}
    valid_keys = [k for k in group_keys if k in df.columns]
    valid_aggs = {k: v for k, v in agg_rules.items() if k in df.columns}
    if not valid_keys: return df 
    df_agg = df.groupby(valid_keys, as_index=False).agg(valid_aggs)
    return df_agg

def map_ads_report(file_content):
    """
    Parses Advertised Product or Purchased Product report to map Campaign -> SKU/ASIN.
    """
    try:
        try:
            df = pd.read_excel(file_content)
        except:
            file_content.seek(0)
            df = pd.read_csv(file_content)
            
        camp_col = find_col(df, ["Campaign Name", "Campaign"])
        sku_col = find_col(df, ["Advertised SKU", "SKU"])
        asin_col = find_col(df, ["Advertised ASIN", "ASIN"])
        
        if not camp_col or not sku_col:
            return None, None, "Could not find 'Campaign Name' or 'Advertised SKU' columns."
            
        cols_to_keep = [camp_col, sku_col]
        if asin_col: cols_to_keep.append(asin_col)
            
        mapping_df = df[cols_to_keep].dropna(subset=[camp_col, sku_col]).drop_duplicates(subset=[camp_col])
        
        campaign_map = {}
        asin_map = {}
        
        for _, row in mapping_df.iterrows():
            camp_name = row[camp_col]
            sku = str(row[sku_col])
            
            entry = {"Advertised SKU": sku}
            if asin_col and pd.notna(row[asin_col]):
                entry["Advertised ASIN"] = row[asin_col]
            campaign_map[camp_name] = entry
            
            extracted_asin = extract_asin_from_text(camp_name)
            if extracted_asin:
                asin_map[extracted_asin] = sku
                
        return campaign_map, asin_map, None
    except Exception as e:
        return None, None, f"Error parsing ads report: {str(e)}"

@st.cache_data
def run_optimizer_logic(file_content, config):
    try:
        df = pd.read_excel(file_content)
    except Exception as e:
        return None, f"Error reading file: {e}", None, None

    df.columns = df.columns.map(lambda c: "" if pd.isna(c) else str(c).strip())
    col_map = {
        "Impressions": find_col(df, ["Impressions", "Impr"]), "Clicks": find_col(df, ["Clicks"]),
        "Spend": find_col(df, ["Spend", "Cost"]), "Sales": find_col(df, ["7 Day Total Sales", "Sales"]),
        "Orders": find_col(df, ["7 Day Total Orders", "Orders"]), "CPC": find_col(df, ["CPC", "Cost Per Click"]),
        "Campaign": find_col(df, ["Campaign Name", "Campaign"]), "Term": find_col(df, ["Customer Search Term", "Search Term"]),
        "Match": find_col(df, ["Match Type"]), "Targeting": find_col(df, ["Targeting", "Keyword"]),
        "Date": find_col(df, ["Date", "Day"]), "Start Date": find_col(df, ["Start Date"]), "End Date": find_col(df, ["End Date"])
    }
    if any(col_map[c] is None for c in ["Campaign", "Term", "Match"]):
        return None, f"Missing columns. Found: {col_map}", None, None
    
    df = df.rename(columns={v: k for k, v in col_map.items() if v is not None})
    for std_col in ["Impressions", "Clicks", "Spend", "Sales", "Orders", "CPC"]:
        if std_col not in df.columns: df[std_col] = 0
        df[std_col] = safe_numeric(df[std_col])

    # 1. Chart Data
    if "Start Date" in df.columns and "End Date" in df.columns: df_chart = expand_dates(df.copy())
    elif "Date" in df.columns:
        df_chart = df.copy()
        df_chart["Date"] = pd.to_datetime(df_chart["Date"], errors='coerce')
        df_chart = df_chart.dropna(subset=["Date"])
    else: df_chart = df.copy() 

    # 2. Optimizer Data
    df["Campaign Name"] = df["Campaign"]
    df["Customer Search Term"] = df["Term"]
    df["Match Type"] = df["Match"]
    df["Cost Per Click (CPC)"] = df["CPC"]
    df["7 Day Total Sales"] = df["Sales"]
    df["7 Day Total Orders"] = df["Orders"]
    
    df_opt = aggregate_for_optimization(df)
    df_opt["CTR"] = np.where(df_opt["Impressions"]>0, df_opt["Clicks"]/df_opt["Impressions"], 0.0)
    df_opt["ROAS"] = np.where(df_opt["Spend"]>0, df_opt["7 Day Total Sales"]/df_opt["Spend"], 0.0)
    df_opt["Campaign Median ROAS"] = df_opt.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)
    df_opt = flatten_dataframe_columns(df_opt)

    # Logic
    matcher = ExactMatcher(df_opt)
    outputs = {}
    
    # Survivors
    auto_pattern = r'close-match|loose-match|substitutes|complements|category=|asin|b0'
    discovery_mask = (~df_opt["Match Type"].astype(str).str.contains("exact", case=False)) | (df_opt["Targeting"].astype(str).str.contains(auto_pattern, case=False))
    discovery_df = df_opt[discovery_mask].copy()
    avg_cpc = discovery_df["Cost Per Click (CPC)"].replace(0, np.nan).mean() or 0.5
    
    readable_mask = (
        (discovery_df["Clicks"] >= config["CLICK_THRESHOLD"]) &
        (discovery_df["Spend"] >= config["SPEND_MULTIPLIER"] * avg_cpc) &
        (discovery_df["Impressions"] >= config["IMPRESSION_THRESHOLD"])
    )
    readable_df = discovery_df[readable_mask].copy()
    readable_df["Action"] = readable_df.apply(lambda r: "Promote" if (r["7 Day Total Sales"] > 0 and r["ROAS"] >= 1.2 * (r["Campaign Median ROAS"] or 2.5)) else "Stable", axis=1)
    
    promote_df = readable_df[readable_df["Action"]=="Promote"].copy()
    survivors = []
    dedupe_meta = []
    for _, row in promote_df.iterrows():
        matched, _ = matcher.find_match(row["Customer Search Term"], config["DEDUPE_SIMILARITY"])
        if matched: dedupe_meta.append({"Term": row["Customer Search Term"], "Matched": matched})
        else: survivors.append(row)
    outputs['Survivors'] = pd.DataFrame(survivors)
    outputs['Dedupe_Matches'] = pd.DataFrame(dedupe_meta)

    # Negatives
    negatives = []
    if not outputs['Survivors'].empty:
        for _, row in outputs['Survivors'].iterrows():
            negatives.append({"Type": "Isolation", "Campaign": row["Campaign Name"], "Term": row["Customer Search Term"], "Match Type": "Exact Negative"})
    
    bleeder_mask = (df_opt["7 Day Total Sales"] == 0) & (df_opt["Clicks"] >= config["NEGATIVE_CLICKS_THRESHOLD"]) & (df_opt["Spend"] >= config["NEGATIVE_SPEND_THRESHOLD"]) & (~df_opt["Match Type"].str.contains("exact", case=False))
    for _, row in df_opt[bleeder_mask].iterrows():
        negatives.append({"Type": "Performance", "Campaign": row["Campaign Name"], "Term": row["Customer Search Term"], "Match Type": "Exact Negative"})
    outputs['Negatives'] = pd.DataFrame(negatives)

    # Bids
    ex_mask = df_opt["Match Type"].str.contains("exact", case=False) | df_opt["Targeting"].str.contains(r"asin|b0", case=False)
    exact_df = df_opt[ex_mask].copy()
    if not exact_df.empty:
        res = exact_df.apply(lambda r: calculate_optimal_bid(r, config["ALPHA_EXACT_PT"], "Rule Based", config["MAX_BID_CHANGE"], 0), axis=1)
        exact_df["New Bid"] = res.apply(lambda x: x[0])
        exact_df["Reason"] = res.apply(lambda x: x[1])
        outputs['Exact_PT_Bids'] = exact_df[["Campaign Name", "Customer Search Term", "Impressions", "Clicks", "Spend", "ROAS", "Cost Per Click (CPC)", "New Bid", "Reason"]]

    # Broad Bids
    bp_mask = df_opt["Match Type"].str.contains("broad|phrase", case=False)
    pb = df_opt[bp_mask].copy()
    if not pb.empty:
        pb["Semantic_Group"] = pb["Customer Search Term"].apply(lambda x: tokens_sorted_string(x, 3))
        agg = pb.groupby(["Campaign Name", "Semantic_Group"], as_index=False).agg({"Impressions":"sum","Clicks":"sum","Spend":"sum","7 Day Total Sales":"sum","Cost Per Click (CPC)":"mean","Campaign Median ROAS":"mean"})
        agg["ROAS"] = np.where(agg["Spend"]>0, agg["7 Day Total Sales"]/agg["Spend"], 0.0)
        res = agg.apply(lambda r: calculate_optimal_bid(r, config["ALPHA_BROAD_PHRASE"], "Rule Based", config["MAX_BID_CHANGE"], 0), axis=1)
        agg["New Bid"] = res.apply(lambda x: x[0])
        agg["Reason"] = res.apply(lambda x: x[1])
        agg = agg.rename(columns={"Semantic_Group": "Customer Search Term"})
        outputs['Broad_Phrase_Bids'] = agg[["Campaign Name", "Customer Search Term", "Impressions", "Clicks", "Spend", "ROAS", "Cost Per Click (CPC)", "New Bid", "Reason"]]

    # Low Vol
    low_vol_mask = (df_opt["Clicks"] < 5) & (df_opt["Impressions"] > 100) & (df_opt["Match Type"].str.contains("exact", case=False))
    low_vol_df = df_opt[low_vol_mask].copy()
    if not low_vol_df.empty:
         res = low_vol_df.apply(lambda r: calculate_optimal_bid(r, 0, "Rule Based", 0.05, config["EXPLORE_BOOST"]), axis=1)
         low_vol_df["New Bid"] = res.apply(lambda x: x[0])
         low_vol_df["Reason"] = res.apply(lambda x: x[1]) 
         outputs['Low_Volume_Boosts'] = low_vol_df[["Campaign Name", "Customer Search Term", "Impressions", "Clicks", "Spend", "Cost Per Click (CPC)", "New Bid", "Reason"]]

    # Clusters
    auto_mask = df_opt["Targeting"].str.contains(r"close-match|loose-match|substitutes|complements|category=", case=False) | df_opt["Match Type"].str.contains("broad", case=False)
    auto_df = df_opt[auto_mask].copy()
    if not auto_df.empty:
        no_asin_mask = ~auto_df["Customer Search Term"].apply(is_asin)
        text_df = auto_df[no_asin_mask].copy()
        if not text_df.empty:
            text_df["Cluster_Key"] = text_df["Customer Search Term"].apply(lambda x: tokens_sorted_string(x))
            text_df = text_df[text_df["Cluster_Key"].str.len() > 0]
            
            term_stats = text_df.groupby(["Cluster_Key", "Customer Search Term"])["Clicks"].sum().reset_index()
            best_terms = term_stats.sort_values("Clicks", ascending=False).drop_duplicates("Cluster_Key")
            key_map = dict(zip(best_terms["Cluster_Key"], best_terms["Customer Search Term"]))
            text_df["Human_Cluster"] = text_df["Cluster_Key"].map(key_map)
            
            clusters = text_df.groupby("Human_Cluster").agg({"Customer Search Term": "count", "Clicks": "sum", "Spend": "sum", "7 Day Total Sales": "sum", "Campaign Name": lambda x: ", ".join(sorted(list(set(x))))[:100]}).rename(columns={"Customer Search Term": "Term Count", "Campaign Name": "Source Campaigns"}).reset_index()
            clusters["ROAS"] = np.where(clusters["Spend"]>0, clusters["7 Day Total Sales"]/clusters["Spend"], 0.0)
            outputs['Clusters'] = clusters[(clusters["Term Count"]>1) & (clusters["Spend"]>=config["CLUSTER_MIN_SPEND"])].sort_values("Spend", ascending=False)

    return outputs, None, df_opt, df_chart

# ==========================================
# 3. MODULE B: CAMPAIGN CREATOR (v11 LOGIC)
# ==========================================

AUTO_PT_MULTIPLIERS = {"close-match": 1.5, "loose-match": 1.2, "substitutes": 0.8, "complements": 1.0}
COLUMN_ORDER = ["Product","Entity","Operation","Campaign ID","Ad Group ID","Portfolio ID","Ad ID","Keyword ID","Product Targeting ID","Campaign Name","Ad Group Name","Start Date","End Date","Targeting Type","State","Daily Budget","SKU","Ad Group Default Bid","Bid","Keyword Text","Match Type","Bidding Strategy"]

def generate_campaigns(keyword_file, skus, portfolio_id, base_bid, total_daily_budget, selected_tactics, launch_date):
    try:
        df_kw = pd.read_excel(keyword_file)
        keywords = df_kw.iloc[:,0].dropna().astype(str).tolist()
    except:
        return pd.DataFrame(), "Error reading keyword file."

    if not skus or not keywords or not selected_tactics:
        return pd.DataFrame(), "Missing inputs."

    rows = []
    start_date_str = launch_date.strftime("%Y%m%d")
    tactic_weights = {"Auto": 20, "Exact": 40, "Broad": 20, "Phrase": 10, "PT-Defensive": 5, "PT-Offensive": 5}
    active_weights = {t: tactic_weights.get(t, 10) for t in selected_tactics}
    total_weight = sum(active_weights.values())
    
    for tactic in selected_tactics:
        campaign_budget = (active_weights[tactic] / total_weight) * total_daily_budget
        campaign_name = f"Launch_{tactic}_{start_date_str}"
        rows.append({"Entity": "Campaign", "Operation": "Create", "Campaign ID": campaign_name, "Campaign Name": campaign_name, "Start Date": start_date_str, "Targeting Type": "AUTO" if tactic == "Auto" else "MANUAL", "State": "Enabled", "Daily Budget": f"{campaign_budget:.2f}", "Bidding Strategy": "Dynamic bids - down only", "Portfolio ID": portfolio_id})
        ad_group_name = f"AG_{tactic}"
        rows.append({"Entity": "Ad Group", "Operation": "Create", "Campaign ID": campaign_name, "Ad Group ID": ad_group_name, "Ad Group Name": ad_group_name, "Ad Group Default Bid": f"{base_bid:.2f}", "State": "Enabled"})
        for sku in skus: rows.append({"Entity": "Product Ad", "Operation": "Create", "Campaign ID": campaign_name, "Ad Group ID": ad_group_name, "SKU": sku, "State": "Enabled"})
        if tactic == "Auto":
            for target, mult in AUTO_PT_MULTIPLIERS.items(): rows.append({"Entity": "Product Targeting", "Operation": "Create", "Campaign ID": campaign_name, "Ad Group ID": ad_group_name, "Targeting Type": "AUTO", "Product Targeting ID": f"auto-targeting-{target}", "Bid": f"{base_bid * mult:.2f}", "State": "Enabled"})
        elif tactic in ["Exact", "Broad", "Phrase"]:
            for kw in keywords: rows.append({"Entity": "Keyword", "Operation": "Create", "Campaign ID": campaign_name, "Ad Group ID": ad_group_name, "Keyword Text": kw, "Match Type": tactic.upper(), "Bid": f"{base_bid:.2f}", "State": "Enabled"})
        elif "PT" in tactic:
            for asin in keywords:
                if re.match(r'B0[A-Z0-9]{8}', asin): rows.append({"Entity": "Product Targeting", "Operation": "Create", "Campaign ID": campaign_name, "Ad Group ID": ad_group_name, "Product Targeting ID": f"asin=\"{asin}\"", "Bid": f"{base_bid:.2f}", "State": "Enabled"})

    df_out = pd.DataFrame(rows)
    for col in COLUMN_ORDER: 
        if col not in df_out.columns: df_out[col] = ""
    df_out['SKU'] = df_out.apply(lambda r: r['SKU'] if str(r['Entity']).strip().lower() == 'product ad' else "", axis=1)
    return df_out[COLUMN_ORDER], None

def generate_bulk_from_harvest(df_harvest, portfolio_id, total_daily_budget, launch_date):
    """
    Generates a bulk file specifically for Harvested keywords.
    """
    COLUMN_ORDER = [
        "Product", "Entity", "Operation", "Campaign ID", "Ad Group ID", "Portfolio ID", 
        "Ad ID", "Keyword ID", "Product Targeting ID", "Campaign Name", "Ad Group Name", 
        "Start Date", "End Date", "Targeting Type", "State", "Daily Budget", "SKU", 
        "Ad Group Default Bid", "Bid", "Keyword Text", "Match Type", "Bidding Strategy",
        "Placement", "Percentage", "Product Targeting Expression", "Audience ID", 
        "Shopper Cohort Percentage", "Shopper Cohort Type", "Creative ID", "Tactic"
    ]
    
    rows = []
    missing_sku_campaigns = set()
    start_date_str = launch_date.strftime("%Y%m%d")
    
    df_work = df_harvest.copy()
    if "Advertised SKU" not in df_work.columns:
        df_work["Advertised SKU"] = ""
    
    missing_mask = (df_work["Advertised SKU"].isna()) | (df_work["Advertised SKU"] == "")
    if missing_mask.any():
        missing_campaigns = df_work.loc[missing_mask, "Campaign Name"].unique()
        missing_sku_campaigns.update(missing_campaigns)
        df_work.loc[missing_mask, "Advertised SKU"] = "SKU_MISSING"

    grouped = df_work.groupby("Advertised SKU")
    
    for sku_key, group in grouped:
        if not sku_key or sku_key.lower() == 'nan': 
            continue
            
        is_missing_sku = (sku_key == "SKU_MISSING")
        sku_val = "" if is_missing_sku else sku_key
        
        campaign_name = f"Harvest_{sku_key}_{start_date_str}"
        
        rows.append({
            "Product": "Sponsored Products",
            "Entity": "Campaign",
            "Operation": "Create",
            "Campaign ID": campaign_name,
            "Campaign Name": campaign_name,
            "Start Date": start_date_str,
            "Targeting Type": "MANUAL",
            "State": "Enabled",
            "Daily Budget": f"{total_daily_budget:.2f}",
            "Bidding Strategy": "Dynamic bids - down only",
            "Portfolio ID": portfolio_id if portfolio_id else ""
        })
        
        ad_group_name = f"AG_Exact_{sku_key}"
        avg_bid = pd.to_numeric(group["New Bid"], errors='coerce').mean()
        if pd.isna(avg_bid): avg_bid = 1.0

        rows.append({
            "Product": "Sponsored Products",
            "Entity": "Ad Group",
            "Operation": "Create",
            "Campaign ID": campaign_name,
            "Campaign Name": campaign_name,
            "Ad Group ID": ad_group_name,
            "Ad Group Name": ad_group_name,
            "Start Date": start_date_str,
            "Targeting Type": "MANUAL",
            "State": "Enabled",
            "Ad Group Default Bid": f"{avg_bid:.2f}"
        })
        
        rows.append({
            "Product": "Sponsored Products",
            "Entity": "Product Ad",
            "Operation": "Create",
            "Campaign ID": campaign_name,
            "Campaign Name": campaign_name,
            "Ad Group ID": ad_group_name,
            "Ad Group Name": ad_group_name,
            "Start Date": start_date_str,
            "Targeting Type": "MANUAL",
            "State": "Enabled",
            "SKU": sku_val, 
            "Ad Group Default Bid": f"{avg_bid:.2f}" 
        })
        
        for _, row in group.iterrows():
            term = str(row["Term"])
            kw_bid = row.get("New Bid", avg_bid)
            if pd.isna(kw_bid): kw_bid = avg_bid
            
            if is_asin(term):
                rows.append({
                    "Product": "Sponsored Products",
                    "Entity": "Product Targeting",
                    "Operation": "Create",
                    "Campaign ID": campaign_name,
                    "Campaign Name": campaign_name,
                    "Ad Group ID": ad_group_name,
                    "Ad Group Name": ad_group_name,
                    "Start Date": start_date_str,
                    "Targeting Type": "MANUAL",
                    "State": "Enabled",
                    "Bid": f"{kw_bid:.2f}",
                    "Ad Group Default Bid": f"{avg_bid:.2f}",
                    "Product Targeting Expression": f'asin="{term.upper()}"' 
                })
            else:
                rows.append({
                    "Product": "Sponsored Products",
                    "Entity": "Keyword",
                    "Operation": "Create",
                    "Campaign ID": campaign_name,
                    "Campaign Name": campaign_name,
                    "Ad Group ID": ad_group_name,
                    "Ad Group Name": ad_group_name,
                    "Start Date": start_date_str,
                    "Targeting Type": "MANUAL",
                    "State": "Enabled",
                    "Bid": f"{kw_bid:.2f}",
                    "Ad Group Default Bid": f"{avg_bid:.2f}",
                    "Keyword Text": term,
                    "Match Type": "EXACT"
                })

    df_out = pd.DataFrame(rows)
    for col in COLUMN_ORDER: 
        if col not in df_out.columns: df_out[col] = ""
    
    return df_out[COLUMN_ORDER].fillna(""), list(missing_sku_campaigns)

# ==========================================
# 4. MAIN UI
# ==========================================

# --- SIDEBAR NAV ---
st.sidebar.image("s2c_logo.png", width=180)
st.sidebar.markdown("## **Navigation**")
if st.sidebar.button("🏠 Home", use_container_width=True): navigate_to('home')
st.sidebar.markdown("---")
st.sidebar.caption("Modules")
if st.sidebar.button("🚀 Campaign Creator", use_container_width=True): navigate_to('creator')
if st.sidebar.button("📊 PPC Optimizer", use_container_width=True): navigate_to('optimizer')

# --- ROUTING ---

if st.session_state['current_module'] == 'home':
    st.markdown("<div class='main-header'><h1>S2C LaunchPad Suite 🚀</h1><p style='font-size:18px; color:#666;'>The ultimate toolkit for Amazon PPC Management</p></div>", unsafe_allow_html=True)
    
    c1, c2 = st.columns(2)
    with c1:
        if st.button("🚀 Campaign Creator\n\nLaunch new products rapidly.", key="btn_c1", help="Click to open Creator"): navigate_to('creator')
    with c2:
        if st.button("📊 PPC Optimizer\n\nAnalyze & Optimize Campaigns.", key="btn_c2", help="Click to open Optimizer"): navigate_to('optimizer')

elif st.session_state['current_module'] == 'creator':
    st.title("🆕 Campaign Creator")

    # --- CHECK FOR INCOMING HARVEST DATA ---
    if 'harvest_payload' in st.session_state:
        st.info("🚀 **Harvest Mode Active:** Data received from PPC Optimizer.")
        
        harvest_df = st.session_state['harvest_payload']
        st.dataframe(harvest_df.head())
        
        if 'harvest_bulk_file' not in st.session_state:
            st.session_state['harvest_bulk_file'] = None
        if 'harvest_missing_skus' not in st.session_state:
            st.session_state['harvest_missing_skus'] = []

        with st.form("harvest_creator_form"):
            st.markdown("### Campaign Settings")
            c1, c2 = st.columns(2)
            with c1:
                budget = st.number_input("Daily Budget per New Campaign ($)", 10.0, 1000.0, 20.0)
                launch_date = st.date_input("Launch Date", datetime.now())
            with c2:
                portfolio = st.text_input("Portfolio ID (Optional)")
                st.caption(f"Will generate campaigns grouped by SKU for {len(harvest_df)} keywords.")

            submitted = st.form_submit_button("🚀 Generate Harvest Campaigns")
            
            if submitted:
                df_bulk, missing_skus = generate_bulk_from_harvest(harvest_df, portfolio, budget, launch_date)
                st.session_state['harvest_bulk_file'] = df_bulk
                st.session_state['harvest_missing_skus'] = missing_skus
                
                st.success(f"Generated {len(df_bulk)} rows!")

        if st.session_state['harvest_bulk_file'] is not None:
            df_bulk = st.session_state['harvest_bulk_file']
            missing_skus = st.session_state['harvest_missing_skus']
            
            if missing_skus:
                st.warning(f"⚠️ **Missing SKUs Detected:** The following source campaigns could not be mapped to a SKU. Their rows have been generated in 'Harvest_SKU_MISSING_...' campaigns with empty SKU fields for manual entry:\n\n" + ", ".join(sorted(missing_skus)))
            
            st.dataframe(df_bulk.head(50))
            st.download_button(
                "📥 Download Bulk File", 
                to_excel_download(df_bulk, "bulk_harvest.xlsx"), 
                f"harvest_campaigns_{datetime.now().date()}.xlsx",
                use_container_width=True
            )
                
        if st.button("❌ Clear & Return to Standard Creator"):
            del st.session_state['harvest_payload']
            if 'harvest_bulk_file' in st.session_state:
                del st.session_state['harvest_bulk_file']
            if 'harvest_missing_skus' in st.session_state:
                del st.session_state['harvest_missing_skus']
            st.rerun()

    else:
        if 'std_bulk_file' not in st.session_state:
            st.session_state['std_bulk_file'] = None

        with st.form("creator_form"):
            c1, c2 = st.columns(2)
            with c1:
                skus = st.text_input("SKUs (comma separated)", help="e.g., SKU-A, SKU-B").split(',')
                base_bid = st.number_input("Base Bid ($)", 0.5, 10.0, 1.0)
                launch_date = st.date_input("Launch Date", datetime.now())
            with c2:
                budget = st.number_input("Total Daily Budget ($)", 10.0, 1000.0, 50.0)
                portfolio = st.text_input("Portfolio ID (Optional)")
            tactics = st.multiselect("Select Campaign Types", ["Auto", "Exact", "Broad", "Phrase", "PT-Defensive", "PT-Offensive"], default=["Auto", "Exact", "Broad"])
            kw_file = st.file_uploader("Upload Keyword/ASIN List (.xlsx)", type="xlsx")
            
            submitted = st.form_submit_button("🚀 Generate Campaigns")
            
            if submitted:
                if kw_file:
                    df_bulk, err = generate_campaigns(kw_file, skus, portfolio, base_bid, budget, tactics, launch_date)
                    if err: 
                        st.error(err)
                    else:
                        st.session_state['std_bulk_file'] = df_bulk
                        st.success(f"Generated {len(df_bulk)} rows!")
                else: 
                    st.warning("Please upload a keyword file.")

        if st.session_state['std_bulk_file'] is not None:
            df_bulk = st.session_state['std_bulk_file']
            st.dataframe(df_bulk.head(50))
            st.download_button("📥 Download Bulk File", to_excel_download(df_bulk, "bulk.xlsx"), f"launch_campaigns_{datetime.now().date()}.xlsx")


elif st.session_state['current_module'] == 'optimizer':
    st.title("📊 PPC Optimizer")
    st.markdown("Upload your Sponsored Products Search Term Report to identify winners, losers, and bid opportunities.")
    
    with st.sidebar.expander("⚙️ Optimization Rules", expanded=True):
        st.subheader("Harvesting")
        click_thresh = st.slider("Min Clicks", 5, 50, 10)
        dedupe_sim = st.slider("Dedupe Similarity", 0.5, 1.0, 0.9)
        st.divider()
        st.subheader("Negatives")
        neg_click = st.number_input("Bleeder Clicks", value=15)
        neg_spend = st.number_input("Bleeder Spend ($)", value=5.0)
        st.divider()
        st.subheader("Bidding")
        alpha_exact = st.slider("Alpha (Exact)", 0.05, 0.5, 0.2)
        alpha_broad = st.slider("Alpha (Broad)", 0.05, 0.5, 0.15)
        max_bid_change = st.slider("Max Change %", 0.05, 0.5, 0.15)
        explore_boost = st.number_input("Explore Boost %", value=0.05)
        st.divider()
        st.subheader("Clusters")
        clust_min_spend = st.number_input("Min Spend ($)", 20.0)
        clust_min_clicks = st.number_input("Min Clicks", 5)

    # --- SHARED ID LOOKUP UPLOAD (SIDEBAR) ---
    st.sidebar.divider()
    st.sidebar.markdown("### 📥 Bulk File Setup")
    st.sidebar.info("Upload **Campaign ID Lookup** here to enable Bulk File creation for Negatives and Bids.")
    id_lookup_file = st.sidebar.file_uploader("Upload ID Lookup (CSV/XLSX)", type=["csv", "xlsx"], key="sidebar_id_uploader")

    # Initialize or update ID maps in session state
    if 'id_maps' not in st.session_state:
        st.session_state['id_maps'] = None

    if id_lookup_file:
        if st.session_state['id_maps'] is None:  # Only parse if not already done or file changed
            with st.spinner("Mapping Bulk IDs..."):
                c_map, k_map, id_err = parse_id_lookup(id_lookup_file)
                if id_err:
                    st.sidebar.error(id_err)
                    st.session_state['id_maps'] = None
                else:
                    st.session_state['id_maps'] = {"campaigns": c_map, "keywords": k_map}
                    st.sidebar.success(f"Mapped {len(c_map)} Campaigns & {len(k_map)} Keywords")
    
    config = {
        "CLICK_THRESHOLD": click_thresh, "SPEND_MULTIPLIER": 3, "IMPRESSION_THRESHOLD": 250,
        "DEDUPE_SIMILARITY": dedupe_sim, "NEGATIVE_CLICKS_THRESHOLD": neg_click,
        "NEGATIVE_SPEND_THRESHOLD": neg_spend, "ALPHA_EXACT_PT": alpha_exact,
        "ALPHA_BROAD_PHRASE": alpha_broad, "MAX_BID_CHANGE": max_bid_change,
        "BIDDING_POLICY": "Rule Based", "NEGATE_DUPLICATES": True,
        "EXPLORE_BOOST": explore_boost, "CLUSTER_MIN_SPEND": clust_min_spend, "CLUSTER_MIN_CLICKS": clust_min_clicks
    }

    upl = st.file_uploader("Upload Search Term Report (.xlsx)", type="xlsx")
    
    if upl:
        with st.spinner("Analyzing... (Aggregating Data + Prorating Charts)"):
            outputs, err, df_opt, df_chart = run_optimizer_logic(upl, config)
            
        if err:
            st.error(err)
        else:
            t1, t2, t3, t4, t5, t6, t7, t8 = st.tabs([
                "📊 Dashboard", "🤖 AI Report", "💎 New Keywords", "🛑 Negatives", 
                "💰 Bids", "🧪 Exploration", "🧠 Auto Clusters", "📥 Export"
            ])
            
            # 1. DASHBOARD
            with t1:
                st.markdown('<div class="tab-blurb"><b>Intent:</b> Visualize account performance trends using accurate daily data.</div>', unsafe_allow_html=True)
                
                tot_spend = df_chart["Spend"].sum()
                tot_sales = df_chart["Sales"].sum()
                tot_clicks = df_chart["Clicks"].sum()
                tot_impr = df_chart["Impressions"].sum()
                tot_orders = df_chart["Orders"].sum()
                
                roas = tot_sales / tot_spend if tot_spend > 0 else 0
                acos = (tot_spend / tot_sales * 100) if tot_sales > 0 else 0
                cpc = tot_spend / tot_clicks if tot_clicks > 0 else 0
                ctr = (tot_clicks / tot_impr * 100) if tot_impr > 0 else 0
                cvr = (tot_orders / tot_clicks * 100) if tot_clicks > 0 else 0
                
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total Spend", f"${tot_spend:,.2f}")
                c2.metric("Total Sales", f"${tot_sales:,.2f}")
                c3.metric("ROAS", f"{roas:.2f}x")
                c4.metric("ACoS", f"{acos:.1f}%")
                
                c5, c6, c7, c8, c9 = st.columns(5)
                c5.metric("Impressions", f"{tot_impr:,.0f}")
                c6.metric("Clicks", f"{tot_clicks:,.0f}")
                c7.metric("CPC", f"${cpc:.2f}")
                c8.metric("CTR", f"{ctr:.2f}%")
                c9.metric("CVR", f"{cvr:.2f}%")
                
                st.divider()
                col_ctrl1, col_ctrl2, col_ctrl3 = st.columns(3)
                with col_ctrl1: time_agg = st.selectbox("Timeframe", ["Daily", "Weekly", "Monthly", "Quarterly"])
                with col_ctrl2: y1_metric = st.selectbox("Bar Metric", ["Spend", "Sales", "Clicks", "Impressions"])
                with col_ctrl3: y2_metric = st.selectbox("Line Metric", ["ACoS", "ROAS", "CPC", "CTR", "CVR"])
                
                if not df_chart.empty and "Date" in df_chart.columns:
                    agg_map = {"Daily": "D", "Weekly": "W-MON", "Monthly": "MS", "Quarterly": "QS"}
                    time_df = df_chart.sort_values("Date").set_index("Date")
                    numeric_cols = ["Spend", "Sales", "Clicks", "Impressions", "Orders"]
                    grp = time_df[numeric_cols].resample(agg_map[time_agg], label='left', closed='left').sum().reset_index()
                    
                    grp["ACoS"] = np.where(grp["Sales"]>0, grp["Spend"]/grp["Sales"]*100, 0)
                    grp["ROAS"] = np.where(grp["Spend"]>0, grp["Sales"]/grp["Spend"], 0)
                    grp["CPC"] = np.where(grp["Clicks"]>0, grp["Spend"]/grp["Clicks"], 0)
                    grp["CTR"] = np.where(grp["Impressions"]>0, grp["Clicks"]/grp["Impressions"]*100, 0)
                    grp["CVR"] = np.where(grp["Clicks"]>0, grp["Orders"]/grp["Clicks"]*100, 0)
                    
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    fig.add_trace(go.Bar(x=grp["Date"], y=grp[y1_metric], name=y1_metric, marker=dict(color='#6366f1', line=dict(width=0)), opacity=0.8), secondary_y=False)
                    fig.add_trace(go.Scatter(x=grp["Date"], y=grp[y2_metric], name=y2_metric, mode='lines+markers', marker=dict(size=8, color='#ec4899'), line=dict(width=3)), secondary_y=True)
                    fig.update_layout(height=400, plot_bgcolor='white', paper_bgcolor='white', margin=dict(l=20, r=20, t=40, b=20), hovermode="x unified")
                    st.plotly_chart(fig, use_container_width=True)
            
            with t2:
                st.markdown('<div class="tab-blurb"><b>Intent:</b> Audit account health and quantify impact using AI.</div>', unsafe_allow_html=True)
                if st.button("Generate AI Analysis"):
                    with st.spinner("Consulting OpenAI..."):
                        st.markdown(get_llm_analysis(df_opt, outputs))
            
            with t3:
                st.markdown(f'<div class="tab-blurb"><b>Intent:</b> Identify {len(outputs.get("Survivors", []))} high-performing terms for Exact Match harvesting.</div>', unsafe_allow_html=True)
                st.dataframe(outputs.get('Survivors', pd.DataFrame()), use_container_width=True)
            
            # --- TAB 4: NEGATIVES (UPDATED) ---
            with t4:
                st.markdown(f'<div class="tab-blurb"><b>Intent:</b> Block {len(outputs.get("Negatives", []))} budget-draining search terms.</div>', unsafe_allow_html=True)
                
                st.dataframe(outputs.get('Negatives', pd.DataFrame()), use_container_width=True)
                
                st.divider()
                st.subheader("📥 Create Negatives Bulk File")
                
                if st.session_state['id_maps']:
                    if st.button("Generate Negatives Bulk File"):
                        df_neg_bulk, unmapped = generate_negatives_bulk(outputs.get('Negatives', pd.DataFrame()), st.session_state['id_maps']['campaigns'])
                        
                        if unmapped > 0:
                            st.warning(f"⚠️ {unmapped} terms could not be mapped to a Campaign ID. Check lookup file coverage.")
                        
                        st.success(f"Generated {len(df_neg_bulk)} negative keyword rows!")
                        st.dataframe(df_neg_bulk.head())
                        st.download_button(
                            "📥 Download Negatives Bulk File", 
                            to_excel_download(df_neg_bulk, "bulk_negatives.xlsx"), 
                            f"bulk_negatives_{datetime.now().date()}.xlsx",
                            use_container_width=True
                        )
                else:
                    st.warning("⚠️ Please upload the **Campaign ID Lookup** file in the Sidebar to generate bulk files.")
            
            # --- TAB 5: BIDS (UPDATED) ---
            with t5:
                st.markdown('<div class="tab-blurb"><b>Intent:</b> Align bids with your Target ROAS based on performance.</div>', unsafe_allow_html=True)
                
                bid_df = pd.concat([outputs.get('Exact_PT_Bids', pd.DataFrame()), outputs.get('Broad_Phrase_Bids', pd.DataFrame())], ignore_index=True)
                
                c1, c2 = st.columns(2)
                with c1: 
                    st.subheader("Exact/PT")
                    st.dataframe(outputs.get('Exact_PT_Bids', pd.DataFrame()), use_container_width=True)
                with c2: 
                    st.subheader("Broad/Phrase")
                    st.dataframe(outputs.get('Broad_Phrase_Bids', pd.DataFrame()), use_container_width=True)
                
                st.divider()
                st.subheader("📥 Create Bid Update Bulk File")
                
                if st.session_state['id_maps']:
                    if st.button("Generate Bids Bulk File"):
                        df_bid_bulk, unmapped, skipped = generate_bids_bulk(bid_df, st.session_state['id_maps']['keywords'])
                        
                        if skipped > 0:
                            st.info(f"ℹ️ Skipped {skipped} 'Hold' bids as requested.")
                        if unmapped > 0:
                            st.warning(f"⚠️ {unmapped} bids could not be mapped. Ensure the lookup file contains exact Keyword Text matches.")
                        
                        st.success(f"Generated {len(df_bid_bulk)} bid update rows!")
                        st.dataframe(df_bid_bulk.head())
                        st.download_button(
                            "📥 Download Bids Bulk File", 
                            to_excel_download(df_bid_bulk, "bulk_bids.xlsx"), 
                            f"bulk_bids_{datetime.now().date()}.xlsx",
                            use_container_width=True
                        )
                else:
                    st.warning("⚠️ Please upload the **Campaign ID Lookup** file in the Sidebar to generate bulk files.")
            
            with t6:
                st.markdown('<div class="tab-blurb"><b>Intent:</b> Boost low-volume terms to test their potential.</div>', unsafe_allow_html=True)
                st.dataframe(outputs.get('Low_Volume_Boosts', pd.DataFrame()), use_container_width=True)
            
            with t7:
                st.markdown('<div class="tab-blurb"><b>Intent:</b> Identify semantic themes for new product niches.</div>', unsafe_allow_html=True)
                st.dataframe(outputs.get('Clusters', pd.DataFrame()), use_container_width=True)
            
            with t8:
                st.markdown('<div class="tab-blurb"><b>Intent:</b> Prepare data for execution. Enrich with SKU data for instant campaign creation.</div>', unsafe_allow_html=True)
                
                # ... [Existing Export Tab Logic Preserved] ...
                if 'opt_bulk_file' not in st.session_state:
                    st.session_state['opt_bulk_file'] = None
                
                survivor_df = outputs.get('Survivors', pd.DataFrame()).copy()
                cluster_df = outputs.get('Clusters', pd.DataFrame()).copy()

                if not cluster_df.empty:
                    cluster_df = cluster_df.rename(columns={
                        "Human_Cluster": "Customer Search Term", 
                        "Source Campaigns": "Campaign Name"
                    })
                    if "Cost Per Click (CPC)" not in cluster_df.columns:
                        cluster_df["Cost Per Click (CPC)"] = np.where(cluster_df["Clicks"] > 0, cluster_df["Spend"] / cluster_df["Clicks"], 0.0)
                    cluster_df["Match Type"] = "Auto Cluster" 

                combined_df = pd.concat([survivor_df, cluster_df], ignore_index=True)
                
                if not combined_df.empty and "Customer Search Term" in combined_df.columns:
                     combined_df = combined_df.drop_duplicates(subset=["Customer Search Term"])
                
                st.subheader("A. Harvest New Campaigns")
                
                if not combined_df.empty:
                    st.markdown("Upload **Sponsored Products Advertised Product Report** (or Purchased Product Report) to map SKUs.")
                    ads_file = st.file_uploader("Upload Ads Report (.xlsx or .csv)", type=["xlsx", "csv"], key="ads_upload")
                    
                    export_df = combined_df.copy()
                    export_df["Current CPC"] = export_df["Cost Per Click (CPC)"]
                    export_df["New Bid"] = export_df["Current CPC"] * 1.05 
                    
                    if "Customer Search Term" in export_df.columns:
                        export_df["Term"] = export_df["Customer Search Term"]
                    else:
                        export_df["Term"] = ""

                    if ads_file:
                        mapping, asin_map, map_err = map_ads_report(ads_file)
                        if map_err:
                            st.error(map_err)
                        else:
                            st.success("✅ Mapping Successful! SKUs and ASINs added.")
                            export_df["Advertised SKU"] = export_df["Campaign Name"].map(lambda x: mapping.get(x, {}).get("Advertised SKU", ""))
                            export_df["Advertised ASIN"] = export_df["Campaign Name"].map(lambda x: mapping.get(x, {}).get("Advertised ASIN", ""))
                            
                            def fallback_sku(row):
                                current_sku = row.get("Advertised SKU", "")
                                if current_sku and str(current_sku).strip(): return current_sku
                                extracted_asin = extract_asin_from_text(row["Campaign Name"])
                                if extracted_asin and extracted_asin in asin_map: return asin_map[extracted_asin]
                                return ""

                            export_df["Advertised SKU"] = export_df.apply(fallback_sku, axis=1)
                    else:
                        export_df["Advertised SKU"] = ""
                        export_df["Advertised ASIN"] = ""

                    for c in ["Advertised SKU", "Advertised ASIN"]:
                        if c not in export_df.columns: export_df[c] = ""
                        
                    final_view = export_df[["Campaign Name", "Term", "Advertised SKU", "Advertised ASIN", "Current CPC", "New Bid", "7 Day Total Sales", "ROAS"]]
                    st.dataframe(final_view, use_container_width=True)

                    c1, c2 = st.columns(2)
                    with c1:
                        st.download_button(
                            "📥 Download Enriched Harvest List", 
                            to_excel_download(final_view, "harvest_enriched.xlsx"), 
                            f"harvest_enriched_{datetime.now().date()}.xlsx",
                            use_container_width=True
                        )
                    with c2:
                        if st.button("🚀 Send to Campaign Creator", use_container_width=True):
                            st.session_state['harvest_payload'] = final_view
                            navigate_to('creator')
                            st.rerun()
                else:
                    st.info("No harvest candidates found.")