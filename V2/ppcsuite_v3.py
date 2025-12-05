import streamlit as st
import pandas as pd
import numpy as np
import re
import xlsxwriter
import difflib
from io import BytesIO
from collections import defaultdict
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from openai import OpenAI

# ==========================================
# CREATOR MODULE CONSTANTS
# ==========================================
AUTO_PT_MULTIPLIERS = {
    "close-match": 1.5,
    "loose-match": 1.2,
    "substitutes": 0.8,
    "complements": 1.0
}
DEFAULT_EXACT_TOP = 5
DEFAULT_PHRASE_NEXT = 7

COLUMN_ORDER_CREATOR = [
    "Product","Entity","Operation","Campaign ID","Ad Group ID","Portfolio ID","Ad ID","Keyword ID",
    "Product Targeting ID","Campaign Name","Ad Group Name","Start Date","End Date","Targeting Type",
    "State","Daily Budget","SKU","Ad Group Default Bid","Bid","Keyword Text",
    "Native Language Keyword","Native Language Locale","Match Type","Bidding Strategy","Placement","Percentage",
    "Product Targeting Expression","Audience ID","Shopper Cohort Percentage","Shopper Cohort Type",
    "Creative ID","Tactic"
]

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

def navigate_to(module: str):
    """Navigate between modules in the app."""
    st.session_state['current_module'] = module

# ==========================================
# 1. ROBUST MAPPING UTILITIES
# ==========================================

class SmartMapper:
    """Smart column mapper with alias support for Amazon PPC reports."""
    
    ALIAS_MAP = {
        "Impressions": ["impressions", "impr"],
        "Clicks": ["clicks"],
        "Spend": ["spend", "cost", "total spend"],
        "Sales": ["7 day total sales", "14 day total sales", "sales", "total sales"],
        "Orders": ["7 day total orders", "14 day total orders", "orders", "total orders"],
        "Sales14": ["14 day total sales"],
        "Orders14": ["14 day total orders"],
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
        "SKU": ["sku", "advertised sku"],
        "ASIN": ["asin", "advertised asin"], 
        "Entity": ["entity"], 
        "AdGroupDefaultBid": ["ad group default bid"]
    }

    @staticmethod
    def normalize(text: str) -> str:
        """Normalize text for fuzzy matching."""
        if not isinstance(text, str):
            return ""
        return re.sub(r"[^a-z0-9]", "", text.lower())

    @classmethod
    def map_columns(cls, df: pd.DataFrame) -> dict:
        """Map DataFrame columns to standard names using aliases."""
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
                    if found:
                        break
            mapping[standard] = found
        return mapping

def safe_numeric(series: pd.Series) -> pd.Series:
    """Convert series to numeric, handling currency symbols and errors."""
    clean = series.astype(str).str.replace(r"[^0-9\.\-]", "", regex=True)
    clean = clean.replace("", "0")
    return pd.to_numeric(clean, errors="coerce").fillna(0)

def normalize_text(s: str) -> str:
    """Normalize text for string matching."""
    if not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def get_tokens(s: str) -> set:
    """Extract meaningful tokens from text."""
    tokens = {t for t in normalize_text(str(s)).split() if len(t) > 2}
    return {t for t in tokens if not t.startswith('b0')}

def tokens_sorted_string(s: str, n_tokens: int = None) -> str:
    """Create sorted token string for clustering."""
    tokens = sorted(list(get_tokens(s)))
    if n_tokens:
        tokens = tokens[:n_tokens]
    return " ".join(tokens)

def is_asin(s: str) -> bool:
    """Check if string contains an ASIN."""
    return bool(re.search(r'\bb0[a-z0-9]{8}\b', str(s).lower()))

def to_excel_download(df_or_dict, filename_prefix: str = "data") -> bytes:
    """Convert DataFrame or dict of DataFrames to Excel bytes."""
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
# CREATOR MODULE HELPERS
# ==========================================

def parse_skus(manual_text):
    if not manual_text: return []
    return [s.strip() for s in manual_text.split(",") if s.strip()]

def parse_keywords_creator(uploaded):
    if uploaded is None: return []
    try:
        if uploaded.name.endswith('.csv'): df = pd.read_csv(uploaded).fillna("")
        else: df = pd.read_excel(uploaded).fillna("")
        if df.empty: return []
        col0 = df.columns[0]
        return [str(x).strip() for x in df[col0].astype(str).tolist() if str(x).strip()]
    except: return []

def parse_asins(text):
    if not text: return []
    return [s.strip() for s in text.split(",") if s.strip()]

def calc_base_bid(price_val, acos_pct, cvr_pct):
    base = price_val * (cvr_pct/100.0) * (acos_pct/100.0)
    return round(max(0.5, base), 2)

def allocate_budget_by_priority(total_budget, tactics):
    n = len(tactics)
    raw = [n - idx for idx in range(n)]
    s = sum(raw)
    if s == 0: return {t: round(total_budget / n, 2) for t in tactics}
    return {tactics[i]: round(total_budget * (raw[i] / s), 2) for i in range(n)}

def append_row_dict(entities, row_dict):
    row = [row_dict.get(col, "") for col in COLUMN_ORDER_CREATOR]
    if len(row) < len(COLUMN_ORDER_CREATOR): row += [""] * (len(COLUMN_ORDER_CREATOR) - len(row))
    entities.append(row[:len(COLUMN_ORDER_CREATOR)])

def to_excel_with_metadata(df_main, metadata):
    output = BytesIO()
    # Using openpyxl to match your main file dependencies
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_main.to_excel(writer, index=False, sheet_name="bulk_rows")
        md = pd.DataFrame(list(metadata.items()), columns=["Key","Value"])
        md.to_excel(writer, index=False, sheet_name="metadata")
    return output.getvalue()

# ==========================================
# 2. BULK FILE GENERATORS (STRICT SCHEMA)
# ==========================================

BULK_COLUMNS_UNIVERSAL = [
    "Product", "Entity", "Operation", "Campaign ID", "Ad Group ID", "Portfolio ID",
    "Ad ID", "Keyword ID", "Product Targeting ID", "Campaign Name", "Ad Group Name",
    "Start Date", "End Date", "Targeting Type", "State", "Daily Budget", "SKU",
    "Ad Group Default Bid", "Bid", "Keyword Text", "Match Type", "Bidding Strategy",
    "Placement", "Percentage", "Product Targeting Expression"
]

def generate_negatives_direct(negatives_df: pd.DataFrame) -> pd.DataFrame:
    """Generate bulk upload file for negative keywords."""
    rows = []
    for _, row in negatives_df.iterrows():
        camp_id = row.get("CampaignId")
        ag_id = row.get("AdGroupId")
        term = str(row.get("Term", "")).strip()

        if not camp_id:
            continue 

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
            if col not in df_out.columns:
                df_out[col] = ""
        df_out = df_out[BULK_COLUMNS_UNIVERSAL]
        
    return df_out

def generate_bids_direct(bids_df: pd.DataFrame, bid_type="Keyword") -> tuple[pd.DataFrame, int]:
    """
    Generate bulk upload files for bid updates.
    Adapted from V2 logic to handle both Keywords and Product Targeting in a generic way.
    """
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
            # Explicitly include expression as requested
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
# 3. EXACT MATCHER FOR DEDUPLICATION
# ==========================================

class ExactMatcher:
    """Fuzzy matcher for detecting existing exact match keywords."""
    
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
            for t in tokens:
                self.token_index[t].add(kw)

    def find_match(self, term: str, threshold: float = 0.90) -> tuple[str | None, float]:
        """Find fuzzy match in exact keywords."""
        norm_term = normalize_text(str(term))
        if not norm_term:
            return None, 0.0
        if norm_term in self.exact_keywords:
            return norm_term, 1.0
        term_tokens = get_tokens(norm_term)
        candidates = set()
        for t in term_tokens:
            if t in self.token_index:
                candidates.update(self.token_index[t])
        if not candidates:
            return None, 0.0
        best_match = None
        best_score = 0.0
        for cand in candidates:
            score = difflib.SequenceMatcher(None, norm_term, cand).ratio()
            if score > best_score:
                best_score = score
                best_match = cand
        if best_score >= threshold:
            return best_match, best_score
        return None, 0.0

# ==========================================
# 4. OPTIMIZER LOGIC - REFACTORED
# ==========================================

def load_and_map_columns(file_content) -> tuple[pd.DataFrame | None, str | None]:
    """Load file and map columns to standard names."""
    try:
        if file_content.name.endswith('.xlsx'):
            df = pd.read_excel(file_content)
        else:
            df = pd.read_csv(file_content)
    except Exception as e:
        return None, f"Error reading file: {e}"

    col_map = SmartMapper.map_columns(df)
    required = ["Impressions", "Clicks", "Spend", "Sales", "Term", "Campaign"]
    missing = [req for req in required if col_map[req] is None]
    if missing:
        return None, f"❌ Missing columns: {', '.join(missing)}"

    df = df.rename(columns={v: k for k, v in col_map.items() if v is not None})
    return df, None

def validate_and_prepare_data(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame | None, str | None]:
    """
    Validate required columns and prepare base metrics.
    Handles dynamic attribution window (7-day vs 14-day).
    """
    # Convert numeric columns
    for col in ["Impressions", "Clicks", "Spend", "Sales", "Orders", "CPC"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = safe_numeric(df[col])

    # Handle ID columns
    for id_col in ["CampaignId", "AdGroupId", "KeywordId", "TargetingId"]:
        if id_col not in df.columns:
            df[id_col] = ""

    # DYNAMIC ATTRIBUTION WINDOW
    attribution_window = config.get("ATTRIBUTION_WINDOW", 7)
    
    if attribution_window == 14:
        # Check if 14-day columns exist
        col_map = SmartMapper.map_columns(df)
        if col_map.get("Sales14") and col_map.get("Orders14"):
            df = df.rename(columns={
                col_map["Sales14"]: "Sales_Attributed",
                col_map["Orders14"]: "Orders_Attributed"
            })
            df["Sales_Attributed"] = safe_numeric(df["Sales_Attributed"])
            df["Orders_Attributed"] = safe_numeric(df["Orders_Attributed"])
            df["Attribution_Window"] = "14-day"
        else:
            # Fallback to 7-day
            df["Sales_Attributed"] = df["Sales"]
            df["Orders_Attributed"] = df["Orders"]
            df["Attribution_Window"] = "7-day (14-day requested but unavailable)"
    else:
        df["Sales_Attributed"] = df["Sales"]
        df["Orders_Attributed"] = df["Orders"]
        df["Attribution_Window"] = "7-day"

    # Standard field mapping
    df["Campaign Name"] = df["Campaign"]
    df["Ad Group Name"] = df.get("AdGroup", "")
    df["Customer Search Term"] = df["Term"]
    df["Match Type"] = df.get("Match", "broad").fillna("broad").astype(str)
    df["Cost Per Click (CPC)"] = df["CPC"]
    
    # Targeting field construction
    if "Keyword" in df.columns: 
        df["Targeting"] = df["Keyword"].replace("", np.nan)
    else:
        df["Targeting"] = pd.Series([np.nan]*len(df))
        
    if "TargetingExpression" in df.columns:
        df["Targeting"] = df["Targeting"].fillna(df["TargetingExpression"])
    
    df["Targeting"] = df["Targeting"].fillna(df["Customer Search Term"]).fillna("").astype(str)

    # Calculate base metrics using attributed sales
    df["CTR"] = np.where(df["Impressions"] > 0, df["Clicks"] / df["Impressions"], 0.0)
    df["ROAS"] = np.where(df["Spend"] > 0, df["Sales_Attributed"] / df["Spend"], 0.0)
    df["CVR"] = np.where(df["Clicks"] > 0, df["Orders_Attributed"] / df["Clicks"], 0.0)
    df["Campaign Median ROAS"] = df.groupby("Campaign Name")["ROAS"].transform("median").fillna(0.0)

    return df, None

def calculate_quality_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate multi-factor quality score for harvest prioritization.
    """
    df = df.copy()
    
    # Campaign-level benchmarks
    df["Campaign_Avg_CTR"] = df.groupby("Campaign Name")["CTR"].transform("mean")
    
    campaign_cvr = df.groupby("Campaign Name").apply(
        lambda x: x["Orders_Attributed"].sum() / x["Clicks"].sum() if x["Clicks"].sum() > 0 else 0
    ).to_dict()
    df["Campaign_Avg_CVR"] = df["Campaign Name"].map(campaign_cvr)
    
    # Factor 1: CTR Performance (0-40 points)
    df["CTR_Factor"] = np.where(
        df["Campaign_Avg_CTR"] > 0,
        np.clip((df["CTR"] / df["Campaign_Avg_CTR"]) * 20, 0, 40),
        20
    )
    
    # Factor 2: Conversion Rate (0-30 points)
    df["CVR_Factor"] = np.where(
        df["Campaign_Avg_CVR"] > 0,
        np.clip((df["CVR"] / df["Campaign_Avg_CVR"]) * 15, 0, 30),
        15
    )
    
    # Factor 3: Volume/Confidence (0-20 points)
    df["Volume_Factor"] = np.clip(np.log1p(df["Clicks"]) * 3, 0, 20)
    
    # Factor 4: ROAS Performance (0-10 points)
    df["ROAS_Factor"] = np.where(
        (df["Campaign Median ROAS"] > 0) & (df["ROAS"] > 0),
        np.clip((df["ROAS"] / df["Campaign Median ROAS"]) * 5, 0, 10),
        5
    )
    
    # Combined Quality Score (0-100)
    df["Quality_Score"] = (
        df["CTR_Factor"] + 
        df["CVR_Factor"] + 
        df["Volume_Factor"] + 
        df["ROAS_Factor"]
    ).round(1)
    
    return df

# ==========================================
# PART 2: CONTINUATION OF S2C LAUNCHPAD
# Append this to Part 1
# ==========================================

def identify_harvest_candidates(
    df: pd.DataFrame, 
    config: dict, 
    matcher: ExactMatcher
) -> pd.DataFrame:
    """
    Filter discovery campaigns for high-performing terms.
    Apply click/spend/impression thresholds.
    Check against existing exact match keywords.
    Add quality scoring for prioritization.
    """
    auto_pattern = r'close-match|loose-match|substitutes|complements|category=|asin|b0'
    discovery_mask = (
        (~df["Match Type"].str.contains("exact", case=False, na=False)) | 
        (df["Targeting"].str.contains(auto_pattern, case=False, na=False))
    )
    discovery_df = df[discovery_mask].copy()
    
    avg_cpc = discovery_df["Cost Per Click (CPC)"].replace(0, np.nan).mean() or 0.5
    
    readable_mask = (
        (discovery_df["Clicks"] >= config["CLICK_THRESHOLD"]) &
        (discovery_df["Spend"] >= config["SPEND_MULTIPLIER"] * avg_cpc) &
        (discovery_df["Impressions"] >= config["IMPRESSION_THRESHOLD"])
    )
    readable_df = discovery_df[readable_mask].copy()
    
    # Determine promotion eligibility
    readable_df["Action"] = readable_df.apply(
        lambda r: "Promote" if (
            r["Sales_Attributed"] > 0 and 
            r["ROAS"] >= 1.2 * (r["Campaign Median ROAS"] or 2.5)
        ) else "Stable", 
        axis=1
    )
    
    promote_df = readable_df[readable_df["Action"] == "Promote"].copy()
    
    # Deduplicate against existing exact keywords
    survivors = []
    for _, row in promote_df.iterrows():
        matched, _ = matcher.find_match(row["Customer Search Term"], config["DEDUPE_SIMILARITY"])
        if not matched:
            survivors.append(row)
        
    survivors_df = pd.DataFrame(survivors)
    
    if not survivors_df.empty:
        # Add quality scores
        survivors_df = calculate_quality_score(survivors_df)
        survivors_df["New Bid"] = survivors_df["Cost Per Click (CPC)"] * 1.1
        
        # Sort by quality score (high to low)
        survivors_df = survivors_df.sort_values("Quality_Score", ascending=False)
    
    return survivors_df

def identify_negative_candidates(
    df: pd.DataFrame, 
    config: dict,
    harvest_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Find bleeders (0 sales, high clicks/spend) with statistical confidence.
    Generate isolation negatives for harvested terms.
    """
    negatives = []
    
    # Add isolation negatives for harvested terms
    if not harvest_df.empty:
        for _, row in harvest_df.iterrows():
            negatives.append({
                "Type": "Isolation",
                "Campaign Name": row["Campaign Name"],
                "CampaignId": row.get("CampaignId"),
                "AdGroupId": row.get("AdGroupId"),
                "Term": row["Customer Search Term"],
                "Match Type": "Exact Negative",
                "Impressions": row.get("Impressions"),
                "Clicks": row.get("Clicks"),
                "Spend": row.get("Spend"),
                "Sales": row.get("Sales_Attributed"),
                "Orders": row.get("Orders_Attributed"),
                "CTR": row.get("CTR"),
                "Cost Per Click (CPC)": row.get("Cost Per Click (CPC)"),
                "ROAS": row.get("ROAS")
            })
    
    # ENHANCED BLEEDER LOGIC with statistical confidence
    campaign_cvr = df.groupby("Campaign Name").apply(
        lambda x: x["Orders_Attributed"].sum() / x["Clicks"].sum() 
        if x["Clicks"].sum() > 0 else 0
    ).to_dict()
    
    df["Campaign_CVR"] = df["Campaign Name"].map(campaign_cvr)
    df["Expected_Orders"] = df["Clicks"] * df["Campaign_CVR"]
    
    # Only flag terms with statistical confidence
    bleeder_mask = (
        (df["Sales_Attributed"] == 0) & 
        (df["Clicks"] >= config["NEGATIVE_CLICKS_THRESHOLD"]) &
        (df["Spend"] >= config["NEGATIVE_SPEND_THRESHOLD"]) &
        (df["Impressions"] >= config["NEGATIVE_IMPRESSION_THRESHOLD"]) &
        (df["Expected_Orders"] >= 1.0) &
        (~df["Match Type"].str.contains("exact", case=False, na=False))
    )
    
    for _, row in df[bleeder_mask].iterrows():
        negatives.append({
            "Type": "Performance",
            "Campaign Name": row["Campaign Name"],
            "CampaignId": row.get("CampaignId"),
            "AdGroupId": row.get("AdGroupId"),
            "Term": row["Customer Search Term"],
            "Match Type": "Exact Negative",
            "Impressions": row.get("Impressions"),
            "Clicks": row.get("Clicks"),
            "Spend": row.get("Spend"),
            "Sales": row.get("Sales_Attributed"),
            "Orders": row.get("Orders_Attributed"),
            "CTR": row.get("CTR"),
            "Cost Per Click (CPC)": row.get("Cost Per Click (CPC)"),
            "ROAS": row.get("ROAS"),
            "Expected Orders": row["Expected_Orders"]
        })
    
    return pd.DataFrame(negatives)

def calculate_optimal_bid(
    row: pd.Series, 
    alpha: float, 
    policy: str, 
    max_change: float, 
    low_vol_boost: float = 0.0
) -> tuple[float, str]:
    """Calculate optimal bid based on ROAS performance."""
    current_cpc = float(row.get("Cost Per Click (CPC)", 0) or 0)
    target_roas = row.get("Campaign Median ROAS", 2.5)
    actual_roas = row.get("ROAS", 0)
    clicks = row.get("Clicks", 0)
    
    if current_cpc <= 0:
        return 0.5, "Default (No CPC Data)"
    if target_roas <= 0:
        target_roas = 2.5 
    
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

# ==========================================
# REPLACE THE calculate_bid_adjustments FUNCTION IN PART 2 WITH THIS
# This is the exact v2 logic
# ==========================================

def calculate_bid_adjustments(
    df: pd.DataFrame, 
    config: dict,
    harvested_terms: set
) -> dict:
    """
    Calculate bid adjustments for all targeting types.
    V2 LOGIC: Separates into Keywords, Product Targeting, and Auto/Category.
    Excludes harvested terms from bid updates.
    """
    outputs = {}
    auto_pattern = r'close-match|loose-match|substitutes|complements|category=|asin|b0'
    
    # Filter out harvested terms
    df_opt = df[~df["Customer Search Term"].str.lower().isin(harvested_terms)].copy()
    
    # EXCLUSION: Get harvested term IDs to exclude from Bids
    # (This was already in v2 - we're keeping it)
    
    # ==========================================
    # GROUP 1: KEYWORDS (Direct - No Aggregation)
    # ==========================================
    kw_mask = (
        df_opt["Match Type"].str.contains("exact|phrase|broad", case=False, na=False) & 
        ~df_opt["Targeting"].str.contains(auto_pattern, case=False, na=False)
    )
    df_kw = df_opt[kw_mask].copy()
    
    if not df_kw.empty:
        # Apply appropriate alpha based on match type
        results = df_kw.apply(
            lambda r: calculate_optimal_bid(
                r, 
                config["ALPHA_EXACT_PT"] if "exact" in r["Match Type"].lower() else config["ALPHA_BROAD_PHRASE"],
                "Rule Based",
                config["MAX_BID_CHANGE"],
                0
            ), 
            axis=1
        )
        df_kw["New Bid"] = results.apply(lambda x: x[0])
        df_kw["Reason"] = results.apply(lambda x: x[1])
        outputs['Keyword_Bids'] = df_kw
    else:
        outputs['Keyword_Bids'] = pd.DataFrame()

    # ==========================================
    # GROUP 2: PRODUCT TARGETING (Direct - No Aggregation)
    # ==========================================
    # V2 LOGIC: PT is identified by asin|b0 in Targeting, excluding category
    pt_mask = (
        df_opt["Targeting"].str.contains(r"asin|b0", case=False, regex=True, na=False) & 
        ~df_opt["Targeting"].str.contains(r"category", case=False, na=False)
    )
    df_pt = df_opt[pt_mask].copy()
    
    if not df_pt.empty:
        results = df_pt.apply(
            lambda r: calculate_optimal_bid(
                r, 
                config["ALPHA_EXACT_PT"], 
                "Rule Based", 
                config["MAX_BID_CHANGE"], 
                0
            ), 
            axis=1
        )
        df_pt["New Bid"] = results.apply(lambda x: x[0])
        df_pt["Reason"] = results.apply(lambda x: x[1])
        outputs['PT_Bids'] = df_pt
    else:
        outputs['PT_Bids'] = pd.DataFrame()

    # ==========================================
    # GROUP 3: AUTO & CATEGORY (Aggregated by TargetingId)
    # ==========================================
    # V2 LOGIC: Auto/Category identified by specific patterns in Targeting column
    ac_mask = df_opt["Targeting"].str.contains(
        r"close-match|loose-match|substitutes|complements|category=", 
        case=False, 
        na=False
    )
    df_ac = df_opt[ac_mask].copy()
    
    if not df_ac.empty:
        # Aggregation columns
        agg_cols = {
            "Impressions": "sum", 
            "Clicks": "sum", 
            "Spend": "sum", 
            "Sales_Attributed": "sum", 
            "Orders_Attributed": "sum", 
            "Cost Per Click (CPC)": "mean", 
            "Campaign Median ROAS": "mean"
        }
        
        # Metadata columns to keep
        meta_cols = {
            c: 'first' for c in [
                "Campaign Name", "Ad Group Name", "Targeting", 
                "CampaignId", "AdGroupId", "Match Type", "TargetingId"
            ]
        }
        
        # V2 LOGIC: Group by TargetingId if available, otherwise skip
        if "TargetingId" in df_ac.columns and df_ac["TargetingId"].notna().any():
            # Remove rows with empty Target ID as we can't update them
            df_ac = df_ac[df_ac["TargetingId"] != ""].copy()
            
            if not df_ac.empty:
                grouped_ac = df_ac.groupby("TargetingId", as_index=False).agg({**agg_cols, **meta_cols})
                
                # Recalculate ROAS with aggregated data
                grouped_ac["ROAS"] = np.where(
                    grouped_ac["Spend"] > 0, 
                    grouped_ac["Sales_Attributed"] / grouped_ac["Spend"], 
                    0.0
                )
                
                # Apply bid optimization
                results = grouped_ac.apply(
                    lambda r: calculate_optimal_bid(
                        r, 
                        config["ALPHA_BROAD_PHRASE"], 
                        "Rule Based", 
                        config["MAX_BID_CHANGE"], 
                        0
                    ), 
                    axis=1
                )
                grouped_ac["New Bid"] = results.apply(lambda x: x[0])
                grouped_ac["Reason"] = results.apply(lambda x: x[1])
                outputs['Auto_Category_Bids'] = grouped_ac
            else:
                outputs['Auto_Category_Bids'] = pd.DataFrame()
        else:
            outputs['Auto_Category_Bids'] = pd.DataFrame()
    else:
        outputs['Auto_Category_Bids'] = pd.DataFrame()

    # ==========================================
    # COMBINE FOR DISPLAY (V2 Logic)
    # ==========================================
    outputs['Exact_PT_Bids'] = pd.concat([
        outputs['Keyword_Bids'], 
        outputs['PT_Bids']
    ])
    
    outputs['Broad_Phrase_Bids'] = outputs['Auto_Category_Bids']

    return outputs


# ==========================================
# REPLACE THE discover_clusters FUNCTION IN PART 2 WITH THIS
# This adds logic to select the best-performing campaign for each cluster
# ==========================================

def discover_clusters(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Token-based clustering of auto/broad terms.
    Now selects best-performing campaign (by ROAS + Orders) as the representative campaign.
    """
    auto_mask_clust = df["Targeting"].str.contains(
        r"close-match|loose-match|substitutes|complements|category=", case=False, na=False
    ) | df["Match Type"].str.contains("broad", case=False, na=False)
    auto_df = df[auto_mask_clust].copy()
    
    if auto_df.empty:
        return pd.DataFrame()
    
    no_asin_mask = ~auto_df["Customer Search Term"].apply(is_asin)
    text_df = auto_df[no_asin_mask].copy()
    
    if text_df.empty:
        return pd.DataFrame()
        
    text_df["Cluster_Key"] = text_df["Customer Search Term"].apply(lambda x: tokens_sorted_string(x))
    text_df = text_df[text_df["Cluster_Key"].str.len() > 0]
    
    # Find best representative term per cluster (most clicks)
    term_stats = text_df.groupby(["Cluster_Key", "Customer Search Term"])["Clicks"].sum().reset_index()
    best_terms = term_stats.sort_values("Clicks", ascending=False).drop_duplicates("Cluster_Key")
    key_map = dict(zip(best_terms["Cluster_Key"], best_terms["Customer Search Term"]))
    text_df["Human_Cluster"] = text_df["Cluster_Key"].map(key_map)
    
    # ==========================================
    # NEW LOGIC: Find best-performing campaign per cluster
    # ==========================================
    
    # First, aggregate campaign performance within each cluster
    campaign_performance = text_df.groupby(["Cluster_Key", "Campaign Name"]).agg({
        "Clicks": "sum",
        "Spend": "sum",
        "Sales_Attributed": "sum",
        "Orders_Attributed": "sum",
        "Cost Per Click (CPC)": "mean"
    }).reset_index()
    
    # Calculate ROAS per campaign per cluster
    campaign_performance["ROAS"] = np.where(
        campaign_performance["Spend"] > 0,
        campaign_performance["Sales_Attributed"] / campaign_performance["Spend"],
        0.0
    )
    
    # Create composite score: ROAS (70%) + Orders (30% normalized)
    # Normalize Orders to 0-1 scale within each cluster
    campaign_performance["Orders_Normalized"] = campaign_performance.groupby("Cluster_Key")["Orders_Attributed"].transform(
        lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() > x.min() else 0
    )
    
    # Composite Score = (ROAS * 0.7) + (Orders_Normalized * ROAS * 0.3)
    # This ensures campaigns with both high ROAS AND volume win
    campaign_performance["Performance_Score"] = (
        campaign_performance["ROAS"] * 0.7 + 
        campaign_performance["Orders_Normalized"] * campaign_performance["ROAS"] * 0.3
    )
    
    # Get best campaign per cluster
    best_campaigns = campaign_performance.sort_values(
        "Performance_Score", ascending=False
    ).drop_duplicates("Cluster_Key")
    
    # Create mapping: Cluster_Key -> Best Campaign Name
    cluster_to_best_campaign = dict(zip(best_campaigns["Cluster_Key"], best_campaigns["Campaign Name"]))
    
    # Add best campaign info to original data
    text_df["Best_Campaign"] = text_df["Cluster_Key"].map(cluster_to_best_campaign)
    
    # ==========================================
    # Aggregate cluster statistics (keeping original logic)
    # ==========================================
    clusters = text_df.groupby("Human_Cluster").agg({
        "Customer Search Term": "count", 
        "Clicks": "sum", 
        "Spend": "sum", 
        "Sales_Attributed": "sum", 
        "Cost Per Click (CPC)": "mean",
        "Best_Campaign": "first",  # NEW: Use best-performing campaign
        "Campaign Name": lambda x: ", ".join(sorted(list(set(x))))[:100]  # Keep for reference
    }).rename(columns={
        "Customer Search Term": "Term Count",
        "Best_Campaign": "Campaign Name",  # This becomes the primary campaign
        "Campaign Name": "Source Campaigns"  # Original becomes secondary info
    }).reset_index()
    
    clusters["ROAS"] = np.where(clusters["Spend"] > 0, clusters["Sales_Attributed"] / clusters["Spend"], 0.0)
    
    result = clusters[(clusters["Term Count"] > 1) & (clusters["Spend"] >= 10)].sort_values("Spend", ascending=False)
    if not result.empty:
        result["New Bid"] = result["Cost Per Click (CPC)"] * 1.15
        result = result.rename(columns={"Human_Cluster": "Customer Search Term"})
    
    return result

def compile_optimization_stats(
    total_rows: int,
    harvest_df: pd.DataFrame,
    negatives_df: pd.DataFrame,
    bids_outputs: dict,
    clusters_df: pd.DataFrame,
    df_original: pd.DataFrame
) -> dict:
    """Calculate summary statistics for dashboard."""
    h_count = len(harvest_df)
    n_count = len(negatives_df)
    
    bids_updated_count = 0
    bids_hold_count = 0
    
    for key in ['Keyword_Bids', 'PT_Bids', 'Auto_Category_Bids']:
        if key in bids_outputs and not bids_outputs[key].empty:
            df = bids_outputs[key]
            bids_hold_count += len(df[df["Reason"].astype(str).str.contains("Hold", case=False)])
            bids_updated_count += len(df[~df["Reason"].astype(str).str.contains("Hold", case=False)])
        
    c_count = len(clusters_df)
    
    optimized_rows = h_count + n_count + bids_updated_count
    untouched = max(0, total_rows - optimized_rows)
    
    return {
        "total_rows": total_rows,
        "total_spend": df_original["Spend"].sum(),
        "harvest_count": h_count,
        "negative_count": n_count,
        "bid_update_count": bids_updated_count,
        "bid_hold_count": bids_hold_count,
        "cluster_count": c_count,
        "untouched_rows": untouched,
        "untouched_pct": (untouched / total_rows) * 100 if total_rows > 0 else 0
    }

@st.cache_data
def run_optimizer_logic(file_content, config):
    """
    Main orchestration function for optimizer.
    Coordinates all sub-functions to produce optimized outputs.
    """
    # Step 1: Load and map columns
    df, error = load_and_map_columns(file_content)
    if error:
        return None, error, None, None
    
    # Step 2: Validate and prepare data
    df, error = validate_and_prepare_data(df, config)
    if error:
        return None, error, None, None
    
    # Step 3: Create exact matcher
    matcher = ExactMatcher(df)
    
    # Step 4: Identify harvest candidates
    harvest_df = identify_harvest_candidates(df, config, matcher)
    
    # Step 5: Identify negatives
    negatives_df = identify_negative_candidates(df, config, harvest_df)
    
    # Step 6: Calculate bid adjustments
    harvested_terms = set(harvest_df['Customer Search Term'].str.lower()) if not harvest_df.empty else set()
    bids_dict = calculate_bid_adjustments(df, config, harvested_terms)
    
    # Step 7: Discover clusters
    clusters_df = discover_clusters(df, config)
    
    # Compile outputs
    outputs = {
        'Survivors': harvest_df,
        'Negatives': negatives_df,
        **bids_dict,
        'Clusters': clusters_df
    }
    
    # Step 8: Compile stats
    stats = compile_optimization_stats(len(df), harvest_df, negatives_df, bids_dict, clusters_df, df)
    
    return outputs, None, df, stats

def get_llm_analysis(stats: dict) -> str:
    """Generate AI analysis of optimization results."""
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
        
        DATA UNTOUCHED:
        - {stats['untouched_rows']:,} rows ({stats['untouched_pct']:.1f}%) remain unchanged (Long tail/Low data).
        
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

# ==========================================
# 5. CAMPAIGN CREATOR (HARVEST)
# ==========================================

def map_skus_from_file(harvest_df: pd.DataFrame, campaigns_file) -> tuple[pd.DataFrame, str]:
    """Map SKUs from campaign report to harvest DataFrame."""
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
                if c_name in sku_map:
                    return sku_map[c_name]
                
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

def generate_bulk_from_harvest(
    df_harvest: pd.DataFrame, 
    portfolio_id: str, 
    total_daily_budget: float, 
    launch_date: datetime
) -> pd.DataFrame:
    """
    Generate bulk upload file for harvest campaigns.
    Uses simplified V2 logic (Campaign per SKU/Match Type).
    """
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
            if col not in df_out.columns:
                df_out[col] = ""
        df_out = df_out[BULK_COLUMNS_UNIVERSAL]
        
    return df_out

# ==========================================
# PART 3: UI/STREAMLIT IMPLEMENTATION
# Append this to Part 2
# ==========================================

# --- SIDEBAR NAVIGATION ---
st.sidebar.markdown("## **S2C LaunchPad**")

# Update: Add the Readme button here
if st.sidebar.button("📖 Readme / Guide", use_container_width=True):
    navigate_to('readme')

if st.sidebar.button("🏠 Home", use_container_width=True):
    navigate_to('home')
st.sidebar.markdown("---")
if st.sidebar.button("📊 Optimizer (Single File)", use_container_width=True):
    navigate_to('optimizer')
if st.sidebar.button("🚀 Creator (Harvest)", use_container_width=True):
    navigate_to('creator')

# ==========================================
# HOME MODULE
# ==========================================

if st.session_state['current_module'] == 'home':
    st.markdown("<div class='main-header'><h1>S2C LaunchPad 🚀</h1><p>Single-File Amazon PPC Optimization</p></div>", unsafe_allow_html=True)
    st.info("ℹ️ **Update:** Now supports **3-Way Bid Segmentation** (Keyword, PT, Auto/Cat Aggregated) with **Harvest Exclusion**, plus V3 enhancements.")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### ✨ New Features")
        st.markdown("""
        - **3-Way Segmentation**: Distinct logic for Keywords, PT, and Aggregated Auto/Cat.
        - **Quality Score System**: 4-factor scoring for harvest prioritization.
        - **Attribution Windows**: Choose 7-day or 14-day attribution.
        - **Statistical Confidence**: Improved bleeder detection.
        """)
    with col2:
        st.markdown("### 🎯 How It Works")
        st.markdown("""
        1. Upload your Search Term Report
        2. Configure optimization rules
        3. Review harvests, negatives, and segmented bid changes
        4. Download bulk upload files
        5. Upload to Amazon Ads
        """)

# ==========================================
# OPTIMIZER MODULE
# ==========================================

elif st.session_state['current_module'] == 'optimizer':
    st.title("📊 PPC Optimizer")
    
    with st.sidebar.expander("⚙️ Rules", expanded=False):
        # Attribution Window Selection
        attribution = st.radio(
            "Attribution Window",
            options=[7, 14],
            index=0,
            help="Use 14-day if your report includes it (better for high-AOV products)"
        )
        
        st.divider()
        
        # Harvest Thresholds
        st.markdown("**Harvest Thresholds**")
        click_thresh = st.slider("Min Clicks", 5, 50, 10)
        impression_thresh = st.number_input("Min Impressions", value=250, step=50)
        
        st.divider()
        
        # Negative Keywords
        st.markdown("**Negative Keywords**")
        neg_click = st.number_input("Bleeder Clicks", value=15, step=1)
        neg_spend = st.number_input("Bleeder Spend ($)", value=5.0, step=0.5)
        neg_impression = st.number_input("Min Impressions for Negative", value=500, step=50)
        
        st.divider()
        
        # Bid Optimization
        st.markdown("**Bid Optimization**")
        alpha_exact = st.slider("Alpha (Exact/PT)", 0.05, 0.5, 0.2, step=0.05)
        alpha_broad = st.slider("Alpha (Broad/Phrase)", 0.05, 0.5, 0.15, step=0.05)
        max_bid_change = st.slider("Max Change %", 0.05, 0.5, 0.15, step=0.05)
        explore_boost = st.number_input("Explore Boost %", value=0.05, step=0.01)

    config = {
        "ATTRIBUTION_WINDOW": attribution,
        "CLICK_THRESHOLD": click_thresh,
        "SPEND_MULTIPLIER": 3,
        "IMPRESSION_THRESHOLD": impression_thresh,
        "DEDUPE_SIMILARITY": 0.9,
        "NEGATIVE_CLICKS_THRESHOLD": neg_click,
        "NEGATIVE_SPEND_THRESHOLD": neg_spend,
        "NEGATIVE_IMPRESSION_THRESHOLD": neg_impression,
        "ALPHA_EXACT_PT": alpha_exact,
        "ALPHA_BROAD_PHRASE": alpha_broad,
        "MAX_BID_CHANGE": max_bid_change,
        "EXPLORE_BOOST": explore_boost
    }

    upl = st.file_uploader("Upload 'SP Search Term Report' (from Bulk Download)", type=["csv", "xlsx"])
    
    if upl:
        with st.spinner("Processing Segmentation & Aggregation..."):
            outputs, err, df_opt, stats = run_optimizer_logic(upl, config)
        
        if err:
            st.error(err)
        else:
            t1, t2, t3, t4, t5, t6 = st.tabs([
                "📊 Dashboard", 
                "💎 Harvest", 
                "🛑 Negatives", 
                "💰 Bids", 
                "🧠 Clusters", 
                "🚀 Actions & Export"
            ])
            
            # ==========================================
            # TAB 1: DASHBOARD
            # ==========================================
            with t1:
                st.markdown("### 🔍 Account Overview")
                
                # Attribution Window Info
                if 'Attribution_Window' in df_opt.columns:
                    st.info(f"📊 Using **{df_opt['Attribution_Window'].iloc[0]}** attribution data")
                
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
                total_sales = df_opt['Sales_Attributed'].sum() if 'Sales_Attributed' in df_opt.columns else df_opt['Sales'].sum()
                m1.metric("Spend", f"${stats['total_spend']:,.0f}")
                m2.metric("Sales", f"${total_sales:,.0f}")
                m3.metric("ROAS", f"{total_sales/stats['total_spend']:.2f}x" if stats['total_spend'] > 0 else "0.00x")
                m4.metric("ACOS", f"{(stats['total_spend']/total_sales)*100:.1f}%" if total_sales > 0 else "0.0%")
                
                m5, m6, m7, m8 = st.columns(4)
                m5.metric("Impressions", f"{df_opt['Impressions'].sum():,.0f}")
                m6.metric("Clicks", f"{df_opt['Clicks'].sum():,.0f}")
                m7.metric("CTR", f"{(df_opt['Clicks'].sum()/df_opt['Impressions'].sum())*100:.2f}%" if df_opt['Impressions'].sum() > 0 else "0.00%")
                m8.metric("CPC", f"${stats['total_spend']/df_opt['Clicks'].sum():.2f}" if df_opt['Clicks'].sum() > 0 else "$0.00")

                st.divider()
                
                if st.button("🤖 Generate AI Explanation"):
                    with st.spinner("Analyzing logic..."):
                        analysis = get_llm_analysis(stats)
                        st.markdown(analysis)

            # ==========================================
            # TAB 2: HARVEST
            # ==========================================
            with t2:
                st.subheader("💎 Harvest Candidates")
                st.markdown(f"""
                <div class="tab-description">
                <b>Purpose:</b> Identify high-performing search terms from Auto/Broad/Phrase campaigns to launch as Exact Match.<br>
                <ul>
                    <li>Filters for terms with > {click_thresh} clicks and profitable ROAS.</li>
                    <li>Deduplicates terms that already exist as Exact Match.</li>
                    <li><b>NOTE:</b> These terms are automatically excluded from the 'Bids' tab calculations.</li>
                </ul>
                </div>
                """, unsafe_allow_html=True)
                
                survivors_df = outputs.get('Survivors', pd.DataFrame())
                
                if not survivors_df.empty:
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Harvest Opportunities", f"{stats['harvest_count']}")
                    c2.metric("Avg Quality Score", f"{survivors_df['Quality_Score'].mean():.1f}/100")
                    c3.metric("High Quality (>70)", f"{(survivors_df['Quality_Score'] > 70).sum()}")
                    
                    # Quality Score Filter
                    min_quality = st.slider(
                        "Filter by Min Quality Score", 
                        0, 100, 50, 
                        help="Higher scores = more confident winners"
                    )
                    filtered_survivors = survivors_df[survivors_df['Quality_Score'] >= min_quality]
                    
                    st.dataframe(filtered_survivors[[
                        "Customer Search Term", "Quality_Score", "ROAS", "CTR", "CVR",
                        "Clicks", "Spend", "Sales_Attributed", "New Bid"
                    ]])
                    
                    if st.button("📦 Prepare for Campaign Creator", key="btn_harvest_prep"):
                        prep_df = filtered_survivors.copy()
                        prep_df["Match Type"] = "EXACT"
                        
                        if 'harvest_payload' in st.session_state:
                            st.session_state['harvest_payload'] = pd.concat([
                                st.session_state['harvest_payload'], prep_df
                            ]).drop_duplicates(subset=["Customer Search Term"])
                        else:
                            st.session_state['harvest_payload'] = prep_df
                            
                        st.toast(f"✅ {len(filtered_survivors)} harvest terms sent to Actions tab!", icon="✅")
                else:
                    st.info("No harvest opportunities found with current thresholds.")

            # ==========================================
            # TAB 3: NEGATIVES
            # ==========================================
            with t3:
                st.subheader("🛑 Negative Candidates")
                st.markdown(f"""
                <div class="tab-description">
                <b>Purpose:</b> Reduce wasted ad spend by negating non-performing terms.<br>
                <ul>
                    <li><b>Bleeders:</b> Terms with ≥ {neg_click} clicks, ≥ {neg_impression} impressions, and 0 sales.</li>
                    <li><b>Confidence:</b> Uses statistical check based on campaign CVR.</li>
                </ul>
                </div>
                """, unsafe_allow_html=True)
                
                neg_df = outputs.get('Negatives', pd.DataFrame())
                
                c1, c2 = st.columns(2)
                c1.metric("Negative Keywords Found", f"{stats['negative_count']}")
                c2.metric("Wasted Spend to Cut", f"${neg_df['Spend'].sum():,.2f}" if not neg_df.empty else "$0.00")
                
                if not neg_df.empty:
                    st.dataframe(neg_df)
                    
                    if "CampaignId" in neg_df.columns:
                        neg_bulk = generate_negatives_direct(neg_df)
                        st.download_button(
                            "📥 Download Negatives Bulk File", 
                            to_excel_download(neg_bulk, "negatives.xlsx"), 
                            "negatives.xlsx"
                        )
                    else:
                        st.warning("⚠️ Missing 'CampaignId'. Cannot create bulk file.")
                else:
                    st.info("No negative keyword opportunities found.")

            # ==========================================
            # TAB 4: BIDS (SEGMENTED)
            # ==========================================
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
                
                # We combine PT and AC for the Targeting File (as they are both Product Targeting entities usually)
                final_kw_rows, skip_kw = generate_bids_direct(kw_df)
                final_pt_rows_pt, skip_pt = generate_bids_direct(pt_df)
                final_ac_rows, skip_ac = generate_bids_direct(ac_df)
                
                final_pt_combined = pd.concat([final_pt_rows_pt, final_ac_rows])

                c1, c2 = st.columns(2)
                c1.download_button("📥 Download Keyword Bids", to_excel_download(final_kw_rows, "kw_bids.xlsx"), "kw_bids.xlsx")
                c2.download_button("📥 Download Targeting Bids (ASIN/Auto/Cat)", to_excel_download(final_pt_combined, "pt_bids.xlsx"), "pt_bids.xlsx")
                
                total_skipped = skip_kw + skip_pt + skip_ac
                if total_skipped > 0:
                    st.info(f"ℹ️ Note: {total_skipped} targets marked as 'Hold' were excluded from download files.")

            # ==========================================
            # TAB 5: CLUSTERS
            # ==========================================
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
                
                if not clust_df.empty:
                    st.dataframe(clust_df)
                    
                    if st.button("📦 Prepare for Campaign Creator (Phrase Match)", key="btn_cluster_prep"):
                        clust_prep = clust_df.copy()
                        clust_prep["Match Type"] = "PHRASE"
                        
                        # No renaming needed anymore - discover_clusters returns "Campaign Name"
                        
                        if 'harvest_payload' in st.session_state:
                            st.session_state['harvest_payload'] = pd.concat([
                                st.session_state['harvest_payload'], clust_prep
                            ]).drop_duplicates(subset=["Customer Search Term"])
                        else:
                            st.session_state['harvest_payload'] = clust_prep
                            
                        st.toast(f"✅ {len(clust_df)} clusters sent to Actions tab as Phrase Match!", icon="🧠")
                else:
                    st.info("No clusters found with current thresholds.")

            # ==========================================
            # TAB 6: ACTIONS & EXPORT
            # ==========================================
            with t6:
                st.subheader("🚀 Harvest Enrichment & Export")
                
                if 'harvest_payload' in st.session_state:
                    h_df = st.session_state['harvest_payload']
                    
                    if "Match Type" in h_df.columns:
                        st.write("**Harvest Composition:**")
                        st.write(h_df["Match Type"].value_counts())
                    
                    st.markdown("#### 1. Map SKUs (Optional)")
                    st.caption("Upload 'Sponsored Products Campaigns.csv' (Bulk) or 'Purchased Product Report' to fill SKUs.")
                    camp_file = st.file_uploader("Upload Campaigns File", type=["csv", "xlsx"], key="enrich_upload")
                    
                    if camp_file:
                        h_df, msg = map_skus_from_file(h_df, camp_file)
                        st.session_state['harvest_payload'] = h_df
                        st.success(msg)
                    
                    if "New Bid" not in h_df.columns and "Cost Per Click (CPC)" in h_df.columns:
                        h_df["New Bid"] = h_df["Cost Per Click (CPC)"] * 1.1

                    if "Advertised SKU" not in h_df.columns:
                        h_df["Advertised SKU"] = "SKU_NEEDED"
                    
                    display_cols = ["Campaign Name", "Customer Search Term", "Advertised SKU", "New Bid"]
                    if "Match Type" in h_df.columns:
                        display_cols.append("Match Type")
                    st.dataframe(h_df[display_cols])
                    
                    st.markdown("#### 2. Configure & Generate")
                    col1, col2 = st.columns(2)
                    with col1:
                        budget = st.number_input("Total Daily Budget ($)", 10.0, value=50.0, step=5.0)
                    with col2:
                        pid = st.text_input("Portfolio ID (Optional)")
                    
                    if st.button("🎯 Generate Harvest Bulk File"):
                        res = generate_bulk_from_harvest(
                            h_df, 
                            pid, 
                            budget, 
                            datetime.now()
                        )
                        
                        st.download_button(
                            "📥 Download Harvest Bulk File", 
                            to_excel_download(res, "harvest_creation.xlsx"), 
                            "harvest_creation.xlsx"
                        )
                        
                        with st.expander("👁️ Preview Harvest Bulk File"):
                            st.dataframe(res.head(20))
                else:
                    st.warning("⚠️ No harvest data pending. Go to '💎 Harvest' or '🧠 Clusters' tab and click 'Prepare'.")

# ==========================================
# README / GUIDE MODULE (Updated with Creator Guide)
# ==========================================

elif st.session_state['current_module'] == 'readme':
    st.title("📖 S2C LaunchPad User Guide")
    
    st.markdown("""
    Welcome to the **S2C LaunchPad Suite**. This tool is designed to audit, optimize, and scale your Amazon PPC accounts 
    using advanced data segmentation and statistical analysis.
    """)

    # --- Quick Start Section ---
    with st.expander("⚡ Quick Start Guide", expanded=True):
        st.markdown("""
        **For Optimization (Existing Campaigns):**
        1. **Download:** 'SP Search Term Report' (Last 60 days) from Amazon.
        2. **Upload:** Go to **📊 Optimizer**, upload the file.
        3. **Action:** Review Harvest/Negatives tabs and download bulk files.

        **For Creation (New Launches):**
        1. **Go to:** **🚀 Creator** tab.
        2. **Input:** Your SKU, Price, and Target ACoS.
        3. **Upload:** A list of target keywords (CSV/Excel).
        4. **Generate:** Download a ready-to-upload Campaign Bulk file.
        """)

    # --- Detailed Tabs ---
    # UPDATE: Added "🚀 Creator Guide" to the list
    tab_overview, tab_logic, tab_creator, tab_faq = st.tabs(["🔍 Optimizer Logic", "🧠 V3 Segmentation", "🚀 Creator Guide", "❓ FAQ"])

    with tab_overview:
        st.markdown("### Optimizer Core Functions")
        
        st.info("#### 💎 Harvest (Scale)")
        st.markdown("""
        Identifies high-performing search terms from Auto/Broad/Phrase campaigns.
        - **Action:** Creates new Single-Keyword Campaigns (SKCs) or ad groups.
        - **Benefit:** Moves winners to a controllable environment.
        """)
        
        st.error("#### 🛑 Negatives (Cut Waste)")
        st.markdown("""
        Finds "Bleeders"—search terms spending money without generating sales.
        - **Action:** Adds these terms as **Negative Exact** to the original campaign.
        - **Benefit:** Immediately improves ROAS by cutting inefficient spend.
        """)

    with tab_logic:
        st.markdown("### Optimizer V3 Segmentation")
        st.markdown("The Optimizer uses a **3-Way Segmentation Strategy** for bids:")
        
        st.markdown("""
        | Segment | Description | Logic Used |
        | :--- | :--- | :--- |
        | **1. Keywords** | Standard Manual Keywords | Direct performance analysis per keyword. |
        | **2. Product Targeting** | ASIN targeting (`b0...`) | Direct performance analysis per Target ID. |
        | **3. Auto/Category** | `close-match`, `loose-match`, etc. | **Aggregated Analysis.** Sums data by Target ID to calculate one optimal bid for the whole group. |
        """)

    # --- NEW SECTION: CREATOR INSTRUCTIONS ---
    with tab_creator:
        st.markdown("### 🚀 Campaign Creator Logic")
        st.caption("Located in the 'Creator' Sidebar Menu")
        
        st.markdown("#### 1. The 'Smart' Base Bid")
        st.markdown("""
        Instead of guessing a bid, the tool calculates it based on your economics:
        $$ Bid = Price \\times CVR \\times ACoS $$
        *Example: 100 AED Price × 10% Conv. Rate × 20% Target ACoS = **2.00 AED Bid***
        """)

        st.markdown("#### 2. Weighted Budget Split")
        st.markdown("""
        The tool splits your **Total Daily Budget** across your selected tactics based on priority. 
        If you select 3 tactics (Auto, Keywords, ASIN), the first one gets the most budget, and the last gets the least.
        """)

        st.markdown("#### 3. Keyword Cascading")
        st.markdown("""
        When you upload a list of keywords, the tool sorts them (top to bottom) and splits them into match types to avoid internal competition:
        1. **Exact Match:** The top N keywords (User defined, default 5). Bid = Base Bid × 1.2
        2. **Phrase Match:** The next M keywords (User defined, default 7). Bid = Base Bid × 1.0
        3. **Broad Match:** All remaining keywords. Bid = Base Bid × 0.8
        """)

    with tab_faq:
        st.markdown("### Frequently Asked Questions")
        st.markdown("**Q: Why are some bids marked as 'Hold'?**")
        st.markdown("A: If a keyword doesn't have enough clicks (default < 5), the tool holds the bid to avoid premature optimization.")
        st.markdown("**Q: What is the 'Attribution Window'?**")
        st.markdown("A: Use 14-day if you sell expensive items where customers take longer to decide.")

# ==========================================
# CREATOR MODULE (Standalone Integration)
# ==========================================

elif st.session_state['current_module'] == 'creator':
    # --- Local CSS for Creator Module ---
    st.markdown("""
    <style>
        .creator-container { background-color: #f0f2f6; padding: 20px; border-radius: 10px; }
        .stButton>button { background-color: #0ea5a4; color: white; }
    </style>
    """, unsafe_allow_html=True)

    st.title("🚀 LaunchPad: Campaign Creator")
    st.markdown("Generate Bulk Campaigns • Smart Budget Split • Standalone Tool")
    st.markdown("---")

    # ---------------- INPUT PANEL ----------------
    c1,c2,c3 = st.columns([3,3,2])
    with c1:
        sku_input = st.text_input("Advertised SKU(s) (comma separated)", key="c_sku")
    with c2:
        asin_input = st.text_input("Competitor ASINs (comma separated)", key="c_asin")
    with c3:
        uploaded_kw = st.file_uploader("Keyword List (1st Col used)", type=['csv','xlsx'], key="c_kw")

    c4,c5,c6,c7 = st.columns(4)
    with c4:
        price = st.number_input("Product Price", min_value=0.1, value=99.0, step=0.5)
    with c5:
        acos = st.slider("Target ACoS %", 5, 40, 20)
    with c6:
        cvr = st.selectbox("Conv. Rate % (est)", [6,9,12,15,20], index=2)
    with c7:
        total_daily_budget = st.number_input("Total Daily Budget", min_value=1.0, value=200.0)

    # ---------------- ADVANCED ----------------
    with st.expander("⚙️ Advanced Options", expanded=False):
        at1, at2 = st.columns([2,1])
        with at1:
            campaign_types = st.multiselect(
                "Campaign Tactics (Ordered by Budget Priority)",
                ["Auto","Manual: Keywords","Manual: ASIN/Product","Category"],
                default=["Auto","Manual: Keywords","Manual: ASIN/Product"]
            )
            bid_mode = st.selectbox("Bidding Strategy", ["Dynamic bids - down only","Dynamic bids - up and down","Fixed bids"], index=0)
        with at2:
            use_auto_pt = st.checkbox("Enable Auto PT types", value=True)
            auto_pt_choices = st.multiselect("Auto PT types", list(AUTO_PT_MULTIPLIERS.keys()), default=list(AUTO_PT_MULTIPLIERS.keys()))
            exact_top = st.number_input("Top N Exact", 1, 100, DEFAULT_EXACT_TOP)
            phrase_next = st.number_input("Next M Phrase", 1, 100, DEFAULT_PHRASE_NEXT)

    run_btn = st.button("⚡ Run Analysis & Generate File", type="primary")

    # ---------------- LOGIC ----------------
    if run_btn:
        skus = parse_skus(sku_input)
        if not skus:
            st.error("❌ Please provide at least one Advertised SKU.")
        else:
            keywords = parse_keywords_creator(uploaded_kw)
            asins = parse_asins(asin_input)
            base_bid = calc_base_bid(price, acos, cvr)
            
            st.success(f"✅ Strategic Base Bid: {base_bid:.2f}")

            if not campaign_types: campaign_types = ["Auto","Manual: Keywords"]
            
            allocation = allocate_budget_by_priority(total_daily_budget, campaign_types)
            st.info("Budget Split: " + ", ".join([f"{k}: {v:.2f}" for k,v in allocation.items()]))

            entities = []
            ts = datetime.now().strftime("%Y%m%d")

            for tactic in campaign_types:
                advertised_sku = skus[0]
                campaign_id = f"ZEN_{advertised_sku}_{tactic.replace(' ','')}"
                adgroup_id = f"{campaign_id}_AG"

                # Campaign Row
                append_row_dict(entities, {
                    "Product":"Sponsored Products","Entity":"Campaign","Operation":"Create",
                    "Campaign ID":campaign_id,"Campaign Name":campaign_id,
                    "Start Date":ts,"Targeting Type": "Auto" if tactic == "Auto" else "Manual",
                    "State":"enabled","Daily Budget":f"{allocation.get(tactic, 0.0):.2f}",
                    "Bidding Strategy": bid_mode, "Tactic":tactic
                })

                # Ad Group Row
                append_row_dict(entities, {
                    "Product":"Sponsored Products","Entity":"Ad Group","Operation":"Create",
                    "Campaign ID":campaign_id,"Ad Group ID":adgroup_id,
                    "Campaign Name":campaign_id,"Ad Group Name":adgroup_id,
                    "Start Date":ts,"State":"enabled","Ad Group Default Bid":f"{base_bid:.2f}",
                    "Tactic":tactic
                })

                # Product Ad Row
                append_row_dict(entities, {
                    "Product":"Sponsored Products","Entity":"Product Ad","Operation":"Create",
                    "Campaign ID":campaign_id,"Ad Group ID":adgroup_id,
                    "Campaign Name":campaign_id,"Ad Group Name":adgroup_id,
                    "SKU":advertised_sku,"State":"enabled","Tactic":tactic
                })

                # Targeting Logic
                if tactic == "Auto":
                    chosen_pt = auto_pt_choices if (use_auto_pt and auto_pt_choices) else (list(AUTO_PT_MULTIPLIERS.keys()) if use_auto_pt else [])
                    if chosen_pt:
                        for pt in chosen_pt:
                            bid_val = round(base_bid * AUTO_PT_MULTIPLIERS.get(pt, 1.0), 2)
                            append_row_dict(entities, {
                                "Product":"Sponsored Products","Entity":"Product Targeting","Operation":"Create",
                                "Campaign ID":campaign_id,"Ad Group ID":adgroup_id,
                                "Campaign Name":campaign_id,"Ad Group Name":adgroup_id,
                                "Bid":f"{bid_val:.2f}","Product Targeting Expression": pt,
                                "Tactic":f"Auto-{pt}"
                            })
                    else:
                        append_row_dict(entities, {
                            "Product":"Sponsored Products","Entity":"Auto","Operation":"Create",
                            "Campaign ID":campaign_id,"Ad Group ID":adgroup_id,
                            "Campaign Name":campaign_id,"Ad Group Name":adgroup_id,
                            "State":"enabled","Tactic":"Auto"
                        })

                elif tactic == "Manual: Keywords":
                    kw_list = keywords if keywords else ["coffee mug","travel mug"]
                    idx = 0
                    # Exact
                    for i in range(min(int(exact_top), len(kw_list))):
                        append_row_dict(entities, {
                            "Product":"Sponsored Products","Entity":"Keyword","Operation":"Create",
                            "Campaign ID":campaign_id,"Ad Group ID":adgroup_id,
                            "Campaign Name":campaign_id,"Ad Group Name":adgroup_id,
                            "Bid":f"{round(base_bid*1.2,2):.2f}","Keyword Text":kw_list[idx],
                            "Match Type":"exact","Tactic":"Manual-Keywords"
                        })
                        idx += 1
                    # Phrase
                    for i in range(min(int(phrase_next), max(0, len(kw_list)-idx))):
                        append_row_dict(entities, {
                            "Product":"Sponsored Products","Entity":"Keyword","Operation":"Create",
                            "Campaign ID":campaign_id,"Ad Group ID":adgroup_id,
                            "Campaign Name":campaign_id,"Ad Group Name":adgroup_id,
                            "Bid":f"{round(base_bid*1.0,2):.2f}","Keyword Text":kw_list[idx],
                            "Match Type":"phrase","Tactic":"Manual-Keywords"
                        })
                        idx += 1
                    # Broad (Remaining)
                    while idx < len(kw_list):
                        append_row_dict(entities, {
                            "Product":"Sponsored Products","Entity":"Keyword","Operation":"Create",
                            "Campaign ID":campaign_id,"Ad Group ID":adgroup_id,
                            "Campaign Name":campaign_id,"Ad Group Name":adgroup_id,
                            "Bid":f"{round(base_bid*0.8,2):.2f}","Keyword Text":kw_list[idx],
                            "Match Type":"broad","Tactic":"Manual-Keywords"
                        })
                        idx += 1

                elif tactic == "Manual: ASIN/Product":
                    targets = asins if asins else ["B0XXXXXX"]
                    for a in targets:
                        append_row_dict(entities, {
                            "Product":"Sponsored Products","Entity":"Product Targeting","Operation":"Create",
                            "Campaign ID":campaign_id,"Ad Group ID":adgroup_id,
                            "Campaign Name":campaign_id,"Ad Group Name":adgroup_id,
                            "Bid":f"{round(base_bid*1.1,2):.2f}",
                            "Product Targeting Expression":f'ASIN="{a}"',"Tactic":"Manual-ASIN"
                        })

                elif tactic == "Category":
                    append_row_dict(entities, {
                        "Product":"Sponsored Products","Entity":"Product Targeting","Operation":"Create",
                        "Campaign ID":campaign_id,"Ad Group ID":adgroup_id,
                        "Campaign Name":campaign_id,"Ad Group Name":adgroup_id,
                        "Bid":f"{base_bid:.2f}","Product Targeting Expression":'category="ID"',
                        "Tactic":"Category"
                    })

            # Finalize
            df_bulk = pd.DataFrame(entities, columns=COLUMN_ORDER_CREATOR)
            df_bulk['SKU'] = df_bulk.apply(lambda r: r['SKU'] if str(r['Entity']).strip().lower() == 'product ad' else "", axis=1)

            st.dataframe(df_bulk.head(100))

            metadata = {
                "advertised_skus": ",".join(skus),
                "base_bid": base_bid,
                "total_budget": total_daily_budget
            }
            
            st.download_button(
                "📥 Download Bulk File", 
                to_excel_with_metadata(df_bulk, metadata), 
                file_name=f"launchpad_campaigns_{ts}.xlsx"
            )
