import streamlit as st
import pandas as pd
import numpy as np
import re
import difflib
from io import BytesIO
from collections import defaultdict
from datetime import datetime, timedelta
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
    .metric-card {
        background-color: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        padding: 15px;
        text-align: center;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }
    .metric-value { font-size: 24px; font-weight: bold; color: #1f2937; }
    .metric-label { font-size: 14px; color: #6b7280; }
    .tab-description {
        background-color: #f8fafc;
        border-left: 4px solid #3b82f6;
        padding: 15px;
        margin-bottom: 20px;
        border-radius: 4px;
        color: #334155;
    }
    .tab-description ul {
        margin-bottom: 0;
        padding-left: 20px;
    }
</style>
""", unsafe_allow_html=True)

if 'current_module' not in st.session_state:
    st.session_state['current_module'] = 'home'

def navigate_to(module):
    st.session_state['current_module'] = module

# ==========================================
# 1. ROBUST MAPPING UTILITIES
# ==========================================

class SmartMapper:
    ALIAS_MAP = {
        "Impressions": ["impressions", "impr"],
        "Clicks": ["clicks"],
        "Spend": ["spend", "cost", "total spend"],
        "Sales": ["7 day total sales", "14 day total sales", "sales", "total sales"],
        "Orders": ["7 day total orders", "14 day total orders", "orders", "total orders"],
        "CPC": ["cpc", "cost per click"],
        "Campaign": ["campaign name", "campaign"],
        "AdGroup": ["ad group name", "ad group"],
        "Term": ["customer search term", "search term", "query"],
        "Keyword": ["keyword text", "targeting", "keyword"],
        "TargetingExpression": ["product targeting expression", "targeting expression"], 
        "Match": ["match type"],
        "Date": ["date", "day", "start date"],
        "CampaignId": ["campaign id"],
        "AdGroupId": ["ad group id"],
        "KeywordId": ["keyword id", "target id"],
        "TargetingId": ["product targeting id", "targeting id"],
        # For Enrichment mapping
        "SKU": ["sku", "advertised sku"],
        "ASIN": ["asin", "advertised asin"], 
        "Entity": ["entity"], 
        "AdGroupDefaultBid": ["ad group default bid"]
    }

    @staticmethod
    def normalize(text):
        if not isinstance(text, str): return ""
        return re.sub(r"[^a-z0-9]", "", text.lower())

    @classmethod
    def map_columns(cls, df):
        mapping = {}
        df_cols = list(df.columns)
        normalized_cols = {cls.normalize(c): c for c in df_cols}

        for standard, aliases in cls.ALIAS_MAP.items():
            found = None
            for alias in aliases:
                norm_alias = cls.normalize(alias)
                if norm_alias in normalized_cols:
                    found = normalized_cols[norm_alias]
                    break
            if not found:
                for alias in aliases:
                    norm_alias = cls.normalize(alias)
                    for col_norm, col_orig in normalized_cols.items():
                        if norm_alias in col_norm:
                            found = col_orig
                            break
                    if found: break
            mapping[standard] = found
        return mapping

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

def is_asin(s: str) -> bool:
    return bool(re.search(r'\bb0[a-z0-9]{8}\b', str(s).lower()))

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
# 2. BULK FILE GENERATORS (STRICT SCHEMA)
# ==========================================

# Standard columns for Bulk Upload (Update & Creation) - STRICTLY ALIGNED
BULK_COLUMNS_UNIVERSAL = [
    "Product", "Entity", "Operation", "Campaign ID", "Ad Group ID", "Portfolio ID",
    "Ad ID", "Keyword ID", "Product Targeting ID", "Campaign Name", "Ad Group Name",
    "Start Date", "End Date", "Targeting Type", "State", "Daily Budget", "SKU",
    "Ad Group Default Bid", "Bid", "Keyword Text", "Match Type", "Bidding Strategy",
    "Placement", "Percentage", "Product Targeting Expression"
]

def generate_negatives_direct(negatives_df):
    rows = []
    for _, row in negatives_df.iterrows():
        camp_id = row.get("CampaignId")
        ag_id = row.get("AdGroupId")
        term = str(row.get("Term", "")).strip()

        if not camp_id: continue 

        common = {
            "Product": "Sponsored Products",
            "Operation": "create",
            "Campaign ID": camp_id,
            "Campaign Name": row.get("Campaign Name", ""),
            "Keyword Text": term,
            "Match Type": "negativeExact",
            "State": "enabled"
        }

        if ag_id:
            rows.append({**common, "Entity": "Negative Keyword", "Ad Group ID": ag_id})
        else:
            rows.append({**common, "Entity": "Campaign Negative Keyword"})
            
    df_out = pd.DataFrame(rows)
    
    if df_out.empty:
        df_out = pd.DataFrame(columns=BULK_COLUMNS_UNIVERSAL)
    else:
        for col in BULK_COLUMNS_UNIVERSAL:
            if col not in df_out.columns: df_out[col] = ""
        df_out = df_out[BULK_COLUMNS_UNIVERSAL]
        
    return df_out

def generate_bids_direct(bids_df, bid_type="Keyword"):
    rows = []
    skipped_hold_count = 0
    
    for _, row in bids_df.iterrows():
        if "hold" in str(row.get("Reason", "")).lower():
            skipped_hold_count += 1
            continue
            
        new_bid = row.get("New Bid", 0.0)
        # Handle Aggregated IDs vs Direct IDs
        kw_id = row.get("KeywordId") or row.get("TargetingId")
        camp_id = row.get("CampaignId")
        ag_id = row.get("AdGroupId")
        
        if not kw_id or not camp_id: continue

        row_data = {
            "Product": "Sponsored Products",
            "Operation": "update",
            "Campaign ID": camp_id,
            "Campaign Name": row.get("Campaign Name", ""),
            "Ad Group ID": ag_id,
            "Ad Group Name": row.get("Ad Group Name", ""),
            "State": "enabled",
            "Bid": f"{new_bid:.2f}"
        }
        
        # Determine Entity Type based on input or targeting
        targeting_text = str(row.get("Targeting", "")).lower()
        
        # Check if it's a Keyword update or PT update
        if "asin=" in targeting_text or "category=" in targeting_text or "close-match" in targeting_text or "loose-match" in targeting_text or "substitutes" in targeting_text or "complements" in targeting_text:
            row_data["Entity"] = "Product Targeting"
            row_data["Product Targeting ID"] = kw_id 
            # Updated to explicitly include expression as requested
            row_data["Product Targeting Expression"] = row.get("Targeting", "") 
        else:
            row_data["Entity"] = "Keyword"
            row_data["Keyword ID"] = kw_id
            row_data["Keyword Text"] = row.get("Targeting", "")
            row_data["Match Type"] = row.get("Match Type", "exact")
            
        rows.append(row_data)

    df_out = pd.DataFrame(rows)
    
    if df_out.empty: 
        df_out = pd.DataFrame(columns=BULK_COLUMNS_UNIVERSAL)
    else:
        for col in BULK_COLUMNS_UNIVERSAL:
            if col not in df_out.columns: df_out[col] = ""
        df_out = df_out[BULK_COLUMNS_UNIVERSAL]
        
    return df_out, skipped_hold_count

# ==========================================
# 3. OPTIMIZER LOGIC
# ==========================================

class ExactMatcher:
    def __init__(self, df: pd.DataFrame):
        match_col = "Match Type" if "Match Type" in df.columns else "Match"
        if match_col not in df.columns:
            self.exact_keywords = set()
            return
        match_types = df[match_col].astype(str).fillna("")
        exact_rows = df[match_types.str.contains("exact", case=False, na=False)]
        term_col = "Customer Search Term" if "Customer Search Term" in df.columns else "Term"
        self.exact_keywords = set(exact_rows[term_col].astype(str).apply(normalize_text).unique())
        self.token_index = defaultdict(set)
        for kw in self.exact_keywords:
            tokens = get_tokens(kw)
            for t in tokens: self.token_index[t].add(kw)

    def find_match(self, term: str, threshold: float = 0.90):
        norm_term = normalize_text(str(term))
        if not norm_term: return None, 0.0
        if norm_term in self.exact_keywords: return norm_term, 1.0
        term_tokens = get_tokens(norm_term)
        candidates = set()
        for t in term_tokens:
            if t in self.token_index: candidates.update(self.token_index[t])
        if not candidates: return None, 0.0
        best_match = None
        best_score = 0.0
        for cand in candidates:
            score = difflib.SequenceMatcher(None, norm_term, cand).ratio()
            if score > best_score:
                best_score = score
                best_match = cand
        if best_score >= threshold: return best_match, best_score
        return None, 0.0

def calculate_optimal_bid(row, alpha, policy, max_change, low_vol_boost=0.0):
    current_cpc = float(row.get("Cost Per Click (CPC)", 0) or 0)
    target_roas = float(row.get("Campaign Median ROAS", 2.5))
    actual_roas = float(row.get("ROAS", 0))
    clicks = float(row.get("Clicks", 0))
    
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

def get_llm_analysis(stats):
    try:
        api_key = st.secrets["OPENAI_API_KEY"]
        client = OpenAI(api_key=api_key)
    except Exception:
        return "⚠️ **OpenAI API Key Missing**. Add to secrets.toml."

    try:
        prompt = f"""
        You are an Amazon PPC Audit Tool.
        INPUT DATA:
        - Total Search Terms Processed: {stats['total_rows']:,}
        - Total Spend Analyzed: ${stats['total_spend']:,.2f}
        
        OPTIMIZATION ACTIONS TAKEN:
        1. Harvests (New Winners): {stats['harvest_count']} terms (Moving to Exact Match).
        2. Negatives (Bleeders): {stats['negative_count']} terms (Adding Negative Exact).
        3. Bid Updates: {stats['bid_update_count']} existing targets optimized.
        4. Clusters Found: {stats['cluster_count']} thematic clusters identified.
        
        DATA SEGMENTATION:
        - Keyword Bids (Direct): Optimized based on individual performance.
        - PT Bids (ASINs): Optimized based on individual performance.
        - Auto/Category Bids (Aggregated): Aggregated search terms back to Target Level before optimizing.
        
        TASK:
        Write a transparent, action-oriented summary.
        - Section 1: "Input Profile" (What kind of data did we see?)
        - Section 2: "Optimization Actions" (Explain *why* we harvested/negated these amounts).
        - Section 3: "Untouched Data" (Explain that low data terms are left for exploration).
        Keep it professional and analytical.
        """
        response = client.chat.completions.create(
            model="gpt-4o", messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"❌ Error calling OpenAI: {str(e)}"

@st.cache_data
def run_optimizer_logic(file_content, config):
    try:
        df = pd.read_excel(file_content) if file_content.name.endswith('.xlsx') else pd.read_csv(file_content)
    except Exception as e:
        return None, f"Error reading file: {e}", None, None

    col_map = SmartMapper.map_columns(df)
    required = ["Impressions", "Clicks", "Spend", "Sales", "Term", "Campaign"]
    missing = [req for req in required if col_map[req] is None]
    if missing:
        return None, f"❌ Missing columns: {', '.join(missing)}", None, None

    df = df.rename(columns={v: k for k, v in col_map.items() if v is not None})
    
    for col in ["Impressions", "Clicks", "Spend", "Sales", "Orders", "CPC"]:
        if col not in df.columns: df[col] = 0
        df[col] = safe_numeric(df[col])

    for id_col in ["CampaignId", "AdGroupId", "KeywordId", "TargetingId"]:
        if id_col not in df.columns: df[id_col] = ""

    df["Campaign Name"] = df["Campaign"]
    df["Ad Group Name"] = df.get("AdGroup", "")
    df["Customer Search Term"] = df["Term"]
    df["Match Type"] = df.get("Match", "broad").fillna("broad").astype(str)
    df["Cost Per Click (CPC)"] = df["CPC"]
    df["7 Day Total Sales"] = df["Sales"]
    df["7 Day Total Orders"] = df["Orders"]
    
    if "Keyword" in df.columns: 
        df["Targeting"] = df["Keyword"].replace("", np.nan)
    else:
        df["Targeting"] = pd.Series([np.nan]*len(df))
        
    if "TargetingExpression" in df.columns:
        df["Targeting"] = df["Targeting"].fillna(df["TargetingExpression"])
    
    df["Targeting"] = df["Targeting"].fillna(df["Customer Search Term"]).fillna("").astype(str)

    df_opt = df.copy()
    df_opt["CTR"] = np.where(df_opt["Impressions"]>0, df_opt["Clicks"]/df_opt["Impressions"], 0.0)
    df_opt["ROAS"] = np.where(df_opt["Spend"]>0, df_opt["7 Day Total Sales"]/df_opt["Spend"], 0.0)
    df_opt["Campaign Median ROAS"] = df_opt.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)

    # 1. IDENTIFY HARVEST CANDIDATES (SURVIVORS)
    matcher = ExactMatcher(df_opt)
    outputs = {}
    
    # Regex patterns for Segmentation
    auto_cat_pattern = r'close-match|loose-match|substitutes|complements|category='
    asin_pattern = r'asin='
    
    # Discovery df: Not Exact Match OR is Auto/Category/ASIN Target
    discovery_mask = (~df_opt["Match Type"].str.contains("exact", case=False)) | (df_opt["Targeting"].str.contains(auto_cat_pattern + "|" + asin_pattern, case=False, regex=True))
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
    for _, row in promote_df.iterrows():
        matched, _ = matcher.find_match(row["Customer Search Term"], config["DEDUPE_SIMILARITY"])
        if not matched: survivors.append(row)
        
    outputs['Survivors'] = pd.DataFrame(survivors)
    if not outputs['Survivors'].empty:
        outputs['Survivors']["New Bid"] = outputs['Survivors']["Cost Per Click (CPC)"] * 1.1

    # 2. IDENTIFY NEGATIVES
    negatives = []
    if not outputs['Survivors'].empty:
        for _, row in outputs['Survivors'].iterrows():
            negatives.append({
                "Type": "Isolation", "Campaign Name": row["Campaign Name"], "CampaignId": row.get("CampaignId"),
                "AdGroupId": row.get("AdGroupId"), "Term": row["Customer Search Term"], "Match Type": "Exact Negative",
                "Impressions": row.get("Impressions"), "Clicks": row.get("Clicks"), "Spend": row.get("Spend"), 
                "7 Day Total Sales": row.get("7 Day Total Sales"), "7 Day Total Orders": row.get("7 Day Total Orders"),
                "CTR": row.get("CTR"), "Cost Per Click (CPC)": row.get("Cost Per Click (CPC)"), "ROAS": row.get("ROAS")
            })
            
    # Bleeder Logic
    bleeder_mask = (df_opt["7 Day Total Sales"] == 0) & (df_opt["Clicks"] >= config["NEGATIVE_CLICKS_THRESHOLD"]) & (df_opt["Spend"] >= config["NEGATIVE_SPEND_THRESHOLD"]) & (~df_opt["Match Type"].str.contains("exact", case=False))
    for _, row in df_opt[bleeder_mask].iterrows():
        negatives.append({
            "Type": "Performance", "Campaign Name": row["Campaign Name"], "CampaignId": row.get("CampaignId"),
            "AdGroupId": row.get("AdGroupId"), "Term": row["Customer Search Term"], "Match Type": "Exact Negative",
            "Impressions": row.get("Impressions"), "Clicks": row.get("Clicks"), "Spend": row.get("Spend"), 
            "7 Day Total Sales": row.get("7 Day Total Sales"), "7 Day Total Orders": row.get("7 Day Total Orders"),
            "CTR": row.get("CTR"), "Cost Per Click (CPC)": row.get("Cost Per Click (CPC)"), "ROAS": row.get("ROAS")
        })
    outputs['Negatives'] = pd.DataFrame(negatives)

    # ==========================================
    # 3. BID OPTIMIZATION (SEGMENTED)
    # ==========================================
    
    # A. EXCLUSION: Exclude Harvested Terms from ALL Bid Updates
    harvested_terms = set(outputs['Survivors']['Customer Search Term'].str.lower().tolist()) if not outputs['Survivors'].empty else set()
    
    # Filter df_opt to REMOVE harvested terms before any bid calc
    # We do NOT remove bleeders here because we might still want to drop the bid on them if not negative yet
    df_clean_bids = df_opt[~df_opt["Customer Search Term"].str.lower().isin(harvested_terms)].copy()

    # B. GROUP 1: KEYWORDS (Exact, Phrase, Broad - Not Auto/Cat/ASIN)
    # Logic: Match Type is standard AND Targeting does NOT contain auto/cat/asin patterns
    kw_mask = (
        df_clean_bids["Match Type"].str.contains("exact|phrase|broad", case=False) & 
        ~df_clean_bids["Targeting"].str.contains(auto_cat_pattern + "|" + asin_pattern, case=False, regex=True)
    )
    df_kw = df_clean_bids[kw_mask].copy()
    
    if not df_kw.empty:
        # Apply rule per row
        res = df_kw.apply(lambda r: calculate_optimal_bid(r, config["ALPHA_BROAD_PHRASE"] if "exact" not in r["Match Type"].lower() else config["ALPHA_EXACT_PT"], "Rule Based", config["MAX_BID_CHANGE"], 0), axis=1)
        df_kw["New Bid"] = res.apply(lambda x: x[0])
        df_kw["Reason"] = res.apply(lambda x: x[1])
        outputs['Keyword_Bids'] = df_kw
    else:
        outputs['Keyword_Bids'] = pd.DataFrame()

    # C. GROUP 2: PRODUCT TARGETING (ASINs - Not Category)
    # Logic: Targeting contains 'asin=' but NOT 'category='
    pt_mask = (
        df_clean_bids["Targeting"].str.contains("asin=", case=False) & 
        ~df_clean_bids["Targeting"].str.contains("category=", case=False)
    )
    df_pt = df_clean_bids[pt_mask].copy()
    
    if not df_pt.empty:
        res = df_pt.apply(lambda r: calculate_optimal_bid(r, config["ALPHA_EXACT_PT"], "Rule Based", config["MAX_BID_CHANGE"], 0), axis=1)
        df_pt["New Bid"] = res.apply(lambda x: x[0])
        df_pt["Reason"] = res.apply(lambda x: x[1])
        outputs['PT_Bids'] = df_pt
    else:
        outputs['PT_Bids'] = pd.DataFrame()

    # D. GROUP 3: AUTO & CATEGORY (AGGREGATED)
    # Logic: Targeting contains auto patterns OR 'category='
    ac_mask = df_clean_bids["Targeting"].str.contains(auto_cat_pattern, case=False, regex=True)
    df_ac = df_clean_bids[ac_mask].copy()
    
    if not df_ac.empty:
        # AGGREGATION STEP: Roll back to AdGroup + TargetingId
        # We need TargetingId to bid. If missing, we might use AdGroupId but Bulk Ops prefers TargetId for "Product Targeting" entity updates.
        
        # Ensure ID columns are present
        if "TargetingId" not in df_ac.columns: df_ac["TargetingId"] = ""
        
        # Define Grouping Keys
        # Use TargetingId if available. If not, use AdGroupId + Targeting Expression (less reliable for bulk upload but good for analysis)
        # We filter out rows with no TargetingId for valid bulk updates
        valid_ac = df_ac[df_ac["TargetingId"].astype(str).str.len() > 1].copy()
        
        if not valid_ac.empty:
            agg_cols = {
                "Impressions": "sum", "Clicks": "sum", "Spend": "sum", "7 Day Total Sales": "sum", 
                "7 Day Total Orders": "sum", "Cost Per Click (CPC)": "mean", "Campaign Median ROAS": "mean"
            }
            # Keep Metadata (First occurrence)
            meta_cols = {c: 'first' for c in ["Campaign Name", "Ad Group Name", "Targeting", "CampaignId", "AdGroupId", "Match Type", "TargetingId"]}
            
            # Group By Target ID (and Campaign/AdGroup for uniqueness)
            grouped_ac = valid_ac.groupby(["CampaignId", "AdGroupId", "TargetingId"], as_index=False).agg({**agg_cols, **meta_cols})
            
            # Recalculate Aggregated Metrics
            grouped_ac["ROAS"] = np.where(grouped_ac["Spend"]>0, grouped_ac["7 Day Total Sales"]/grouped_ac["Spend"], 0.0)
            
            # Apply Bid Rule to AGGREGATED Data
            res = grouped_ac.apply(lambda r: calculate_optimal_bid(r, config["ALPHA_BROAD_PHRASE"], "Rule Based", config["MAX_BID_CHANGE"], 0), axis=1)
            grouped_ac["New Bid"] = res.apply(lambda x: x[0])
            grouped_ac["Reason"] = res.apply(lambda x: x[1])
            
            outputs['Auto_Category_Bids'] = grouped_ac
        else:
            outputs['Auto_Category_Bids'] = pd.DataFrame()
    else:
        outputs['Auto_Category_Bids'] = pd.DataFrame()

    # 4. CLUSTERS (Same logic as before)
    auto_mask_clust = df_opt["Targeting"].str.contains(r"close-match|loose-match|substitutes|complements|category=", case=False) | df_opt["Match Type"].str.contains("broad", case=False)
    auto_df = df_opt[auto_mask_clust].copy()
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
            
            clusters = text_df.groupby("Human_Cluster").agg({
                "Customer Search Term": "count", "Clicks": "sum", "Spend": "sum", "7 Day Total Sales": "sum", 
                "Cost Per Click (CPC)": "mean",
                "Campaign Name": lambda x: ", ".join(sorted(list(set(x))))[:100]
            }).rename(columns={"Customer Search Term": "Term Count", "Campaign Name": "Source Campaigns"}).reset_index()
            clusters["ROAS"] = np.where(clusters["Spend"]>0, clusters["7 Day Total Sales"]/clusters["Spend"], 0.0)
            
            outputs['Clusters'] = clusters[(clusters["Term Count"]>1) & (clusters["Spend"]>=10)].sort_values("Spend", ascending=False)
            if not outputs['Clusters'].empty:
                outputs['Clusters']["New Bid"] = outputs['Clusters']["Cost Per Click (CPC)"] * 1.15
                outputs['Clusters'] = outputs['Clusters'].rename(columns={"Human_Cluster": "Customer Search Term"})
    
    if 'Clusters' not in outputs:
        outputs['Clusters'] = pd.DataFrame()

    total = len(df)
    h_count = len(outputs['Survivors'])
    n_count = len(outputs['Negatives'])
    
    # Calculate stats based on 3 groups
    bids_updated_count = 0
    bids_hold_count = 0
    for key in ['Keyword_Bids', 'PT_Bids', 'Auto_Category_Bids']:
        if key in outputs and not outputs[key].empty:
            bids_hold_count += len(outputs[key][outputs[key]["Reason"].astype(str).str.contains("Hold", case=False)])
            bids_updated_count += len(outputs[key][~outputs[key]["Reason"].astype(str).str.contains("Hold", case=False)])
        
    c_count = len(outputs['Clusters'])
    
    optimized_rows = h_count + n_count + bids_updated_count
    untouched = max(0, total - optimized_rows)
    
    stats = {
        "total_rows": total,
        "total_spend": df["Spend"].sum(),
        "harvest_count": h_count,
        "negative_count": n_count,
        "bid_update_count": bids_updated_count,
        "bid_hold_count": bids_hold_count,
        "cluster_count": c_count,
        "untouched_rows": untouched,
        "untouched_pct": (untouched/total)*100 if total > 0 else 0
    }

    return outputs, None, df_opt, stats

# ==========================================
# 4. CAMPAIGN CREATOR (HARVEST)
# ==========================================

def map_skus_from_file(harvest_df, campaigns_file):
    try:
        camp_df = pd.read_csv(campaigns_file) if campaigns_file.name.endswith('.csv') else pd.read_excel(campaigns_file)
        sku_map = {} 
        asin_sku_map = {} 
        
        col_map = SmartMapper.map_columns(camp_df)
        c_col = col_map.get("Campaign")
        s_col = col_map.get("SKU")
        a_col = col_map.get("ASIN")
        e_col = col_map.get("Entity")
        
        if c_col and s_col:
            if e_col and "Product Ad" in camp_df[e_col].unique():
                 ads = camp_df[camp_df[e_col] == "Product Ad"]
            else:
                 ads = camp_df.dropna(subset=[s_col])
            
            sku_map = pd.Series(ads[s_col].values, index=ads[c_col]).to_dict()
            if a_col:
                asin_sku_map = pd.Series(ads[s_col].values, index=ads[a_col]).to_dict()

            def resolve_sku(row):
                c_name = row.get("Campaign Name")
                if c_name in sku_map: return sku_map[c_name]
                
                match = re.search(r'_(B0[A-Z0-9]{8})', str(c_name))
                if match:
                    extracted_asin = match.group(1)
                    if extracted_asin in asin_sku_map:
                        return asin_sku_map[extracted_asin]
                        
                return "SKU_NEEDED"

            harvest_df["Advertised SKU"] = harvest_df.apply(resolve_sku, axis=1)
            found = harvest_df[harvest_df["Advertised SKU"] != "SKU_NEEDED"].shape[0]
            return harvest_df, f"✅ Mapped SKUs for {found} terms (using direct & fallback methods)."
        else:
            return harvest_df, f"⚠️ Could not find mapped columns. Found: {col_map}"
    except Exception as e:
        return harvest_df, f"❌ Error mapping SKUs: {str(e)}"

def generate_bulk_from_harvest(df_harvest, portfolio_id, total_daily_budget, launch_date):
    rows = []
    start_date_str = launch_date.strftime("%Y%m%d")
    
    if "Advertised SKU" not in df_harvest.columns:
        df_harvest["Advertised SKU"] = "SKU_NEEDED"

    if "Match Type" not in df_harvest.columns:
        df_harvest["Match Type"] = "EXACT"

    grouped = df_harvest.groupby("Advertised SKU")
    
    for sku_key, group in grouped:
        for match_type, sub_group in group.groupby("Match Type"):
            match_suffix = "Cluster" if match_type == "PHRASE" else "Exact"
            campaign_name = f"Harvest_{match_suffix}_{sku_key}_{start_date_str}"
            
            # Campaign Row
            rows.append({
                "Product": "Sponsored Products", "Entity": "Campaign", "Operation": "Create",
                "Campaign ID": campaign_name, "Campaign Name": campaign_name,
                "Start Date": start_date_str, "Targeting Type": "MANUAL", "State": "Enabled",
                "Daily Budget": f"{total_daily_budget:.2f}", "Bidding Strategy": "Dynamic bids - down only",
                "Portfolio ID": portfolio_id or ""
            })
            
            ag_name = f"AG_{match_suffix}_{sku_key}"
            avg_bid = pd.to_numeric(sub_group["New Bid"], errors='coerce').fillna(1.0).mean()
            
            # Ad Group Row
            rows.append({
                "Product": "Sponsored Products", "Entity": "Ad Group", "Operation": "Create",
                "Campaign ID": campaign_name, "Campaign Name": campaign_name, # Added to ensure link
                "Ad Group ID": ag_name, "Ad Group Name": ag_name,
                "Start Date": start_date_str, "State": "Enabled", "Ad Group Default Bid": f"{avg_bid:.2f}"
            })
            
            # Product Ad Row
            rows.append({
                "Product": "Sponsored Products", "Entity": "Product Ad", "Operation": "Create",
                "Campaign ID": campaign_name, "Campaign Name": campaign_name, # Added to ensure link
                "Ad Group ID": ag_name, "Ad Group Name": ag_name, # Added to ensure link
                "SKU": sku_key, "State": "Enabled"
            })
            
            # Keyword / Target Rows
            for _, row in sub_group.iterrows():
                term = str(row["Customer Search Term"])
                kw_bid = row.get("New Bid", avg_bid)
                
                common_kw = {
                    "Product": "Sponsored Products", "Operation": "Create",
                    "Campaign ID": campaign_name, "Campaign Name": campaign_name, # Added to ensure link
                    "Ad Group ID": ag_name, "Ad Group Name": ag_name, # Added to ensure link
                    "Bid": f"{kw_bid:.2f}", "State": "Enabled"
                }
                
                if is_asin(term):
                    rows.append({**common_kw, "Entity": "Product Targeting", "Product Targeting Expression": f'asin="{term.upper()}"'})
                else:
                    m_type = "PHRASE" if match_suffix == "Cluster" else "EXACT"
                    rows.append({**common_kw, "Entity": "Keyword", "Keyword Text": term, "Match Type": m_type})
                
    df_out = pd.DataFrame(rows)
    
    if df_out.empty:
        df_out = pd.DataFrame(columns=BULK_COLUMNS_UNIVERSAL)
    else:
        for col in BULK_COLUMNS_UNIVERSAL:
            if col not in df_out.columns: df_out[col] = ""
        df_out = df_out[BULK_COLUMNS_UNIVERSAL]
        
    return df_out

# ==========================================
# 5. UI ROUTING
# ==========================================

# --- SIDEBAR ---
st.sidebar.markdown("## **S2C LaunchPad**")
if st.sidebar.button("🏠 Home", use_container_width=True): navigate_to('home')
st.sidebar.markdown("---")
if st.sidebar.button("📊 Optimizer (Single File)", use_container_width=True): navigate_to('optimizer')
if st.sidebar.button("🚀 Creator (Harvest)", use_container_width=True): navigate_to('creator')

if st.session_state['current_module'] == 'home':
    st.markdown("<div class='main-header'><h1>S2C LaunchPad 🚀</h1><p>Single-File Amazon PPC Optimization</p></div>", unsafe_allow_html=True)
    st.info("ℹ️ **Update:** Now supports **3-Way Bid Segmentation** (Keyword, PT, Auto/Cat Aggregated) and **Harvest Exclusion**.")

elif st.session_state['current_module'] == 'optimizer':
    st.title("📊 PPC Optimizer")
    
    with st.sidebar.expander("⚙️ Rules", expanded=False):
        click_thresh = st.slider("Min Clicks", 5, 50, 10)
        neg_click = st.number_input("Bleeder Clicks", value=15)
        neg_spend = st.number_input("Bleeder Spend ($)", value=5.0)
        st.divider()
        alpha_exact = st.slider("Alpha (Exact)", 0.05, 0.5, 0.2)
        max_bid_change = st.slider("Max Change %", 0.05, 0.5, 0.15)
        explore_boost = st.number_input("Explore Boost %", value=0.05)

    config = {
        "CLICK_THRESHOLD": click_thresh, "SPEND_MULTIPLIER": 3, "IMPRESSION_THRESHOLD": 250,
        "DEDUPE_SIMILARITY": 0.9, "NEGATIVE_CLICKS_THRESHOLD": neg_click,
        "NEGATIVE_SPEND_THRESHOLD": neg_spend, "ALPHA_EXACT_PT": alpha_exact,
        "ALPHA_BROAD_PHRASE": 0.15, "MAX_BID_CHANGE": max_bid_change, "EXPLORE_BOOST": explore_boost
    }

    upl = st.file_uploader("Upload 'SP Search Term Report' (from Bulk Download)", type=["csv", "xlsx"])
    
    if upl:
        with st.spinner("Processing Segmentation & Aggregation..."):
            outputs, err, df_opt, stats = run_optimizer_logic(upl, config)
        
        if err:
            st.error(err)
        else:
            t1, t2, t3, t4, t5, t6 = st.tabs(["📊 Dashboard", "💎 Harvest", "🛑 Negatives", "💰 Bids", "🧠 Clusters", "🚀 Actions & Export"])
            
            with t1:
                st.markdown("### 🔍 Account Overview")
                st.markdown("""
                <div class="tab-description">
                <b>Purpose:</b> High-level health check of your account performance.<br>
                <ul>
                    <li>Analyzes total spend vs. sales to compute global ROAS and ACOS.</li>
                    <li>Quantifies the volume of data processed vs. long-tail data left untouched.</li>
                </ul>
                </div>
                """, unsafe_allow_html=True)
                
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Spend", f"${stats['total_spend']:,.0f}")
                m2.metric("Sales", f"${df_opt['7 Day Total Sales'].sum():,.0f}")
                m3.metric("ROAS", f"{df_opt['7 Day Total Sales'].sum()/stats['total_spend']:.2f}x" if stats['total_spend'] > 0 else "0.00x")
                m4.metric("ACOS", f"{(stats['total_spend']/df_opt['7 Day Total Sales'].sum())*100:.1f}%" if df_opt['7 Day Total Sales'].sum() > 0 else "0.0%")
                
                m5, m6, m7, m8 = st.columns(4)
                m5.metric("Impressions", f"{df_opt['Impressions'].sum():,.0f}")
                m6.metric("Clicks", f"{df_opt['Clicks'].sum():,.0f}")
                m7.metric("CTR", f"{(df_opt['Clicks'].sum()/df_opt['Impressions'].sum())*100:.2f}%" if df_opt['Impressions'].sum() > 0 else "0.00%")
                m8.metric("CPC", f"${df_opt['Clicks'].sum() and stats['total_spend']/df_opt['Clicks'].sum():.2f}")

                st.divider()
                if st.button("Generate AI Explanation"):
                    with st.spinner("Analyzing logic..."):
                        st.markdown(get_llm_analysis(stats))

            with t2:
                st.subheader("💎 Harvest Candidates")
                st.markdown(f"""
                <div class="tab-description">
                <b>Purpose:</b> Identify high-performing search terms from Auto/Broad/Phrase campaigns to launch as Exact Match.<br>
                <ul>
                    <li>Filters for terms with > {click_thresh} clicks and profitable ROAS.</li>
                    <li><b>NOTE:</b> These terms are automatically excluded from the 'Bids' tab calculations to prevent conflict.</li>
                </ul>
                </div>
                """, unsafe_allow_html=True)
                
                c1, c2 = st.columns(2)
                c1.metric("Harvest Opportunities", f"{stats['harvest_count']}")
                c2.metric("Potential New Revenue", f"${outputs.get('Survivors', pd.DataFrame())['7 Day Total Sales'].sum():,.2f}")
                
                st.dataframe(outputs.get('Survivors', pd.DataFrame()))
                if st.button("Prepare for Campaign Creator", key="btn_harvest_prep"):
                    survivors = outputs.get('Survivors', pd.DataFrame()).copy()
                    survivors["Match Type"] = "EXACT"
                    
                    if 'harvest_payload' in st.session_state:
                        st.session_state['harvest_payload'] = pd.concat([st.session_state['harvest_payload'], survivors]).drop_duplicates(subset=["Customer Search Term"])
                    else:
                        st.session_state['harvest_payload'] = survivors
                        
                    st.toast("Harvest data sent to Actions tab!", icon="✅")

            with t3:
                st.subheader("🛑 Negative Candidates")
                st.markdown(f"""
                <div class="tab-description">
                <b>Purpose:</b> Reduce wasted ad spend by negating non-performing terms.<br>
                <ul>
                    <li><b>Bleeders:</b> Terms with > {neg_click} clicks and 0 sales.</li>
                    <li><b>Isolation:</b> Negates harvested terms from their source campaigns.</li>
                </ul>
                </div>
                """, unsafe_allow_html=True)
                
                neg_df = outputs.get('Negatives', pd.DataFrame())
                
                c1, c2 = st.columns(2)
                c1.metric("Negative Keywords Found", f"{stats['negative_count']}")
                c2.metric("Wasted Spend to Cut", f"${neg_df['Spend'].sum():,.2f}" if not neg_df.empty else "$0.00")
                
                st.dataframe(neg_df)
                if not neg_df.empty:
                    if "CampaignId" in neg_df.columns:
                        neg_bulk = generate_negatives_direct(neg_df)
                        st.download_button("📥 Download Negatives Bulk File", to_excel_download(neg_bulk, "negatives.xlsx"), "negatives.xlsx")
                    else:
                        st.warning("⚠️ Missing 'CampaignId'. Cannot create bulk file.")

            with t4:
                st.subheader("💰 Bid Adjustments (Segmented)")
                st.markdown("""
                <div class="tab-description">
                <b>Purpose:</b> Optimize bids with segment-specific logic.<br>
                <b>Groups:</b>
                <ul>
                    <li><b>1. Keywords:</b> Direct bid updates for Exact/Phrase/Broad match types.</li>
                    <li><b>2. PT (ASINs):</b> Direct bid updates for Product Targeting.</li>
                    <li><b>3. Auto/Category:</b> Aggregated performance by Target ID, then optimized.</li>
                </ul>
                <i>Note: Harvested terms are excluded from this analysis.</i>
                </div>
                """, unsafe_allow_html=True)
                
                b1, b2, b3 = st.tabs(["Keywords", "Product Targeting", "Auto & Category (Aggregated)"])
                
                kw_df = outputs.get('Keyword_Bids', pd.DataFrame())
                pt_df = outputs.get('PT_Bids', pd.DataFrame())
                ac_df = outputs.get('Auto_Category_Bids', pd.DataFrame())

                with b1:
                    st.caption(f"Optimizing {len(kw_df)} keywords based on individual term performance.")
                    st.dataframe(kw_df)
                with b2:
                    st.caption(f"Optimizing {len(pt_df)} ASIN targets based on individual performance.")
                    st.dataframe(pt_df)
                with b3:
                    st.caption(f"Optimizing {len(ac_df)} Auto/Category targets. Data was aggregated from search terms up to the Target level.")
                    st.dataframe(ac_df)

                # Generation Logic
                st.divider()
                st.subheader("📥 Download Bid Updates")
                
                # We can combine KW and PT for one file, or keep separate. 
                # AC Bids usually go into the PT file (Entity=Product Targeting)
                
                final_kw_rows, skip_kw = generate_bids_direct(kw_df)
                final_pt_rows_pt, skip_pt = generate_bids_direct(pt_df)
                final_ac_rows, skip_ac = generate_bids_direct(ac_df)
                
                # Merge PT and AC for the Targeting File
                final_pt_combined = pd.concat([final_pt_rows_pt, final_ac_rows])

                c1, c2 = st.columns(2)
                c1.download_button("📥 Download Keyword Bids", to_excel_download(final_kw_rows, "kw_bids.xlsx"), "kw_bids.xlsx")
                c2.download_button("📥 Download Targeting Bids (ASIN/Auto/Cat)", to_excel_download(final_pt_combined, "pt_bids.xlsx"), "pt_bids.xlsx")
                
                total_skipped = skip_kw + skip_pt + skip_ac
                if total_skipped > 0:
                    st.info(f"ℹ️ Note: {total_skipped} targets marked as 'Hold' were excluded from download files.")

            with t5:
                st.subheader("🧠 Auto Clusters")
                st.markdown("""
                <div class="tab-description">
                <b>Purpose:</b> Discover new semantic themes (product niches) from search term patterns.<br>
                </div>
                """, unsafe_allow_html=True)
                
                clust_df = outputs.get('Clusters', pd.DataFrame())
                
                c1, c2 = st.columns(2)
                c1.metric("Clusters Identified", f"{stats['cluster_count']}")
                c2.metric("Total Cluster Spend", f"${clust_df['Spend'].sum():,.2f}" if not clust_df.empty else "$0.00")
                
                st.dataframe(clust_df)
                
                if not clust_df.empty:
                    if st.button("Prepare for Campaign Creator (Phrase Match)", key="btn_cluster_prep"):
                        clust_prep = clust_df.copy()
                        clust_prep["Match Type"] = "PHRASE"
                        
                        if 'harvest_payload' in st.session_state:
                            st.session_state['harvest_payload'] = pd.concat([st.session_state['harvest_payload'], clust_prep]).drop_duplicates(subset=["Customer Search Term"])
                        else:
                            st.session_state['harvest_payload'] = clust_prep
                            
                        st.toast("Clusters sent to Actions tab as Phrase Match!", icon="🧠")

            with t6:
                st.subheader("🚀 Harvest Enrichment & Export")
                
                if 'harvest_payload' in st.session_state:
                    h_df = st.session_state['harvest_payload']
                    
                    if "Match Type" in h_df.columns:
                        st.write("Harvest Composition:")
                        st.write(h_df["Match Type"].value_counts())
                    
                    st.markdown("#### 1. Map SKUs (Optional)")
                    camp_file = st.file_uploader("Upload Campaigns File", type=["csv", "xlsx"], key="enrich_upload")
                    
                    if camp_file:
                        h_df, msg = map_skus_from_file(h_df, camp_file)
                        st.session_state['harvest_payload'] = h_df
                        st.success(msg)
                    
                    if "New Bid" not in h_df.columns and "Cost Per Click (CPC)" in h_df.columns:
                        h_df["New Bid"] = h_df["Cost Per Click (CPC)"] * 1.1

                    if "Advertised SKU" not in h_df.columns:
                        h_df["Advertised SKU"] = "SKU_NEEDED"
                    
                    st.dataframe(h_df[["Campaign Name", "Customer Search Term", "Advertised SKU", "New Bid", "Match Type" if "Match Type" in h_df.columns else "New Bid"]])
                    
                    st.markdown("#### 2. Generate Creation File")
                    budget = st.number_input("Daily Budget ($)", 10.0, value=20.0)
                    pid = st.text_input("Portfolio ID")
                    
                    if st.button("Generate Harvest Bulk File"):
                        res = generate_bulk_from_harvest(h_df, pid, budget, datetime.now())
                        st.download_button("📥 Download Harvest Bulk File", to_excel_download(res, "harvest_creation.xlsx"), "harvest_creation.xlsx")
                        
                        with st.expander("👁️ Preview Harvest Bulk File"):
                            st.dataframe(res.head(20))
                else:
                    st.warning("No harvest data pending. Go to '💎 Harvest' or '🧠 Clusters' tab and click 'Prepare'.")

elif st.session_state['current_module'] == 'creator':
    st.title("🚀 Campaign Creator")
    st.info("Use the 'Actions & Export' tab in the Optimizer to generate files.")
    if st.button("Go to Optimizer"): navigate_to('optimizer')