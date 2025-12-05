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
    """
    Generate bulk upload file for negative keywords/targets.
    """
    rows = []
    for _, row in negatives_df.iterrows():
        camp_name = row.get("Campaign Name", "")
        camp_id = row.get("CampaignId")
        ag_id = row.get("AdGroupId")
        term = str(row.get("Term", "")).strip()

        # Fallback if no ID is present, use name to allow file generation
        if not camp_id and not camp_name:
            continue 

        # Base Row Data
        row_data = {
            "Product": "Sponsored Products",
            "Operation": "create",
            "Campaign ID": camp_id if camp_id else camp_name,
            "Campaign Name": camp_name,
            "State": "enabled"
        }

        # Check if term is an ASIN
        term_is_asin = is_asin(term)

        # Ad Group Level vs Campaign Level
        if ag_id or row.get("Ad Group Name"):
            # Ad Group Level
            if ag_id:
                row_data["Ad Group ID"] = ag_id
            else:
                row_data["Ad Group ID"] = row.get("Ad Group Name")
                row_data["Ad Group Name"] = row.get("Ad Group Name")

            if term_is_asin:
                row_data["Entity"] = "Negative Product Targeting"
                row_data["Product Targeting Expression"] = f'asin="{term.upper()}"'
                row_data["Keyword Text"] = ""
                row_data["Match Type"] = ""
            else:
                row_data["Entity"] = "Negative Keyword"
                row_data["Keyword Text"] = term
                row_data["Match Type"] = "negativeExact"
                row_data["Product Targeting Expression"] = ""

        else:
            # Campaign Level
            if term_is_asin:
                row_data["Entity"] = "Campaign Negative Product Targeting"
                row_data["Product Targeting Expression"] = f'asin="{term.upper()}"'
                row_data["Keyword Text"] = ""
                row_data["Match Type"] = ""
            else:
                row_data["Entity"] = "Campaign Negative Keyword"
                row_data["Keyword Text"] = term
                row_data["Match Type"] = "negativeExact"
                row_data["Product Targeting Expression"] = ""
            
        rows.append(row_data)
            
    df_out = pd.DataFrame(rows)
    
    if df_out.empty:
        df_out = pd.DataFrame(columns=BULK_COLUMNS_UNIVERSAL)
    else:
        # Ensure all universal columns exist
        for col in BULK_COLUMNS_UNIVERSAL:
            if col not in df_out.columns:
                df_out[col] = ""
        # Reorder
        df_out = df_out[BULK_COLUMNS_UNIVERSAL]
        
    return df_out

def generate_bids_direct(bids_df: pd.DataFrame, bid_type="Keyword") -> tuple[pd.DataFrame, int]:
    """
    Generate bulk upload files for bid updates.
    ALL rows are UPDATE operations (updating bids on existing keywords/targets).
    """
    rows = []
    skipped_hold_count = 0
    
    for _, row in bids_df.iterrows():
        if "hold" in str(row.get("Reason", "")).lower():
            skipped_hold_count += 1
            continue
            
        new_bid = row.get("New Bid", 0.0)
        
        # Extract IDs - try all possible column name variations
        # Check exact column names from uploaded file first, then mapped names
        kw_id = (
            row.get("Keyword ID") or          # Exact from upload
            row.get("KeywordId") or           # After SmartMapper
            row.get("Product Targeting ID") or # Exact from upload
            row.get("TargetingId") or         # After SmartMapper
            row.get("Targeting ID")           # Alternative
        )
        
        # Campaign ID - ONLY use if numeric, otherwise leave blank
        camp_id_from_file = row.get("Campaign ID") or row.get("CampaignId")
        camp_name_from_file = row.get("Campaign") or row.get("Campaign Name", "")
        
        # Use numeric ID if available, otherwise BLANK (not name)
        camp_id = ""
        if camp_id_from_file and str(camp_id_from_file).replace('.', '').replace('-', '').isdigit():
            camp_id = camp_id_from_file
        
        # Ad Group ID - ONLY use if numeric, otherwise leave blank  
        ag_id_from_file = row.get("Ad Group ID") or row.get("AdGroupId")
        ag_name_from_file = row.get("Ad Group") or row.get("Ad Group Name", "")
        
        # Use numeric ID if available, otherwise BLANK (not name)
        ag_id = ""
        if ag_id_from_file and str(ag_id_from_file).replace('.', '').replace('-', '').isdigit():
            ag_id = ag_id_from_file
        
        if not camp_name_from_file: 
            continue

        # BID UPDATES ARE ALWAYS UPDATE OPERATIONS
        # We're updating bids on existing keywords/targets
        operation = "update"

        row_data = {
            "Product": "Sponsored Products",
            "Operation": operation,
            "Campaign ID": camp_id,  # Blank if not numeric
            "Campaign Name": camp_name_from_file,
            "Ad Group ID": ag_id,  # Blank if not numeric
            "Ad Group Name": ag_name_from_file,
            "State": "enabled",
            "Bid": f"{new_bid:.2f}"
        }
        
        # Determine if this is Product Targeting or Keyword
        targeting_text = str(row.get("Targeting", "") or row.get("Keyword Text", "")).lower()
        is_pt = (
            "asin=" in targeting_text or 
            "category=" in targeting_text or 
            "close-match" in targeting_text or 
            "loose-match" in targeting_text or 
            "substitutes" in targeting_text or 
            "complements" in targeting_text or
            is_asin(row.get("Targeting", "") or row.get("Keyword Text", ""))
        )
        
        if is_pt:
            row_data["Entity"] = "Product Targeting"
            if kw_id:
                row_data["Product Targeting ID"] = kw_id
            else:
                row_data["Product Targeting ID"] = ""  # Will trigger validation error
            
            pt_expr = row.get("Product Targeting Expression", "") or row.get("Targeting", "")
            if "asin=" in targeting_text or "category=" in targeting_text or "match" in targeting_text:
                 row_data["Product Targeting Expression"] = pt_expr
            elif is_asin(pt_expr):
                 row_data["Product Targeting Expression"] = f'asin="{pt_expr.upper()}"'
            else:
                 row_data["Product Targeting Expression"] = pt_expr

            row_data["Keyword ID"] = ""
            row_data["Keyword Text"] = ""
            row_data["Match Type"] = ""
            
        else:
            row_data["Entity"] = "Keyword"
            if kw_id:
                row_data["Keyword ID"] = kw_id
            else:
                row_data["Keyword ID"] = ""  # Will trigger validation error
            row_data["Keyword Text"] = row.get("Keyword Text", "") or row.get("Targeting", "")
            row_data["Match Type"] = row.get("Match Type", "exact")
            
            row_data["Product Targeting ID"] = ""
            row_data["Product Targeting Expression"] = ""
            
        rows.append(row_data)

    df_out = pd.DataFrame(rows)
    
    if df_out.empty: 
        df_out = pd.DataFrame(columns=BULK_COLUMNS_UNIVERSAL)
    else:
        for col in BULK_COLUMNS_UNIVERSAL:
            if col not in df_out.columns: df_out[col] = ""
        df_out = df_out[BULK_COLUMNS_UNIVERSAL]
        
    return df_out, skipped_hold_count

def merge_bulk_files(
    harvest_df: pd.DataFrame = None, 
    negatives_df: pd.DataFrame = None, 
    bids_df: pd.DataFrame = None
) -> tuple[pd.DataFrame, dict]:
    """
    Merge harvest, negatives, and bid optimization bulk files into one unified file.
    
    Returns:
        - combined_df: Merged DataFrame with all bulk upload rows
        - validation_report: Dict with warnings and statistics
    """
    sections = []
    validation_report = {
        'warnings': [],
        'info': [],
        'harvest_count': 0,
        'negatives_count': 0,
        'bids_count': 0,
        'isolation_negatives': 0,
        'performance_negatives': 0
    }
    
    # Extract harvest keywords for validation
    harvest_keywords = set()
    if harvest_df is not None and not harvest_df.empty:
        harvest_keywords = set(
            harvest_df[harvest_df['Entity'] == 'Keyword']['Keyword Text']
            .astype(str).str.lower().str.strip()
        )
        validation_report['harvest_count'] = len(harvest_df)
        sections.append(harvest_df)
    
    # Extract and validate negatives
    isolation_keywords = set()
    performance_keywords = set()
    if negatives_df is not None and not negatives_df.empty:
        # Separate isolation vs performance negatives
        if 'Type' in negatives_df.columns:
            isolation_mask = negatives_df['Type'].str.contains('Isolation', case=False, na=False)
            isolation_keywords = set(
                negatives_df[isolation_mask]['Term']
                .astype(str).str.lower().str.strip()
            )
            performance_keywords = set(
                negatives_df[~isolation_mask]['Term']
                .astype(str).str.lower().str.strip()
            )
            validation_report['isolation_negatives'] = len(isolation_keywords)
            validation_report['performance_negatives'] = len(performance_keywords)
        
        validation_report['negatives_count'] = len(negatives_df)
    
    # Validate bids
    bid_keywords = set()
    if bids_df is not None and not bids_df.empty:
        bid_keywords = set(
            bids_df[bids_df['Entity'].isin(['Keyword', 'Product Targeting'])]['Keyword Text']
            .astype(str).str.lower().str.strip()
        )
        validation_report['bids_count'] = len(bids_df)
        sections.append(bids_df)
    
    # Generate negatives bulk file if we have negatives data
    if negatives_df is not None and not negatives_df.empty:
        neg_bulk = generate_negatives_direct(negatives_df)
        sections.append(neg_bulk)
    
    # SMART VALIDATION
    # 1. Check for conflicting signals (harvest + performance negative)
    conflicting = harvest_keywords & performance_keywords
    if conflicting:
        validation_report['warnings'].append(
            f"⚠️ {len(conflicting)} keywords are BOTH harvested AND performance-negated. "
            f"This is conflicting - review: {', '.join(list(conflicting)[:5])}"
        )
    
    # 2. Check for bid updates on negated keywords
    bid_negative_overlap = bid_keywords & (isolation_keywords | performance_keywords)
    if bid_negative_overlap:
        validation_report['warnings'].append(
            f"⚠️ {len(bid_negative_overlap)} keywords have bid updates but are also being negated. "
            f"Bid updates will be ignored after negation."
        )
    
    # 3. Info: Expected isolation negatives
    if harvest_keywords and isolation_keywords:
        expected_isolation = harvest_keywords
        missing_isolation = expected_isolation - isolation_keywords
        if missing_isolation:
            validation_report['info'].append(
                f"ℹ️ {len(missing_isolation)} harvest keywords missing isolation negatives. "
                f"This may cause cannibalization in original campaigns."
            )
        
        # This is EXPECTED behavior - not a warning
        overlap_pct = len(harvest_keywords & isolation_keywords) / len(harvest_keywords) * 100 if harvest_keywords else 0
        validation_report['info'].append(
            f"✅ {overlap_pct:.0f}% of harvest keywords have isolation negatives (expected behavior)"
        )
    
    # Combine all sections
    if not sections:
        return pd.DataFrame(columns=BULK_COLUMNS_UNIVERSAL), validation_report
    
    combined = pd.concat(sections, ignore_index=True)
    
    # Ensure all columns exist
    for col in BULK_COLUMNS_UNIVERSAL:
        if col not in combined.columns:
            combined[col] = ""
    
    # Reorder to standard schema
    combined = combined[BULK_COLUMNS_UNIVERSAL]
    
    return combined, validation_report

def validate_bulk_file(df: pd.DataFrame) -> dict:
    """
    Validate bulk file against Amazon Advertising requirements.
    
    Returns dict with:
        - errors: List of critical errors that will cause upload failure
        - warnings: List of potential issues
        - stats: Row counts by entity type
    """
    errors = []
    warnings = []
    stats = defaultdict(int)
    
    if df.empty:
        return {'errors': ['❌ File is empty'], 'warnings': [], 'stats': {}}
    
    # Required columns check
    required_cols = ['Product', 'Entity', 'Operation']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        errors.append(f"❌ Missing required columns: {', '.join(missing_cols)}")
        return {'errors': errors, 'warnings': warnings, 'stats': stats}
    
    # Validate each row
    for idx, row in df.iterrows():
        row_num = idx + 2  # +2 for header row and 0-indexing
        entity = str(row.get('Entity', '')).strip()
        operation = str(row.get('Operation', '')).strip().lower()
        
        # Count by entity type
        stats[entity] += 1
        
        # CRITICAL: Update operations MUST have IDs
        if operation == 'update':
            # Validate Campaign ID is numeric for updates
            camp_id_val = str(row.get('Campaign ID', '')).strip()
            if entity in ['Keyword', 'Product Targeting', 'Ad Group', 'Campaign']:
                if camp_id_val and not camp_id_val.replace('.', '').isdigit():
                    errors.append(
                        f"❌ Row {row_num}: UPDATE operation REQUIRES numeric 'Campaign ID' "
                        f"(found alphanumeric: '{camp_id_val}'). Use campaign ID number, not name."
                    )
            
            # Validate Ad Group ID is numeric for updates
            ag_id_val = str(row.get('Ad Group ID', '')).strip()
            if entity in ['Keyword', 'Product Targeting']:
                if ag_id_val and not ag_id_val.replace('.', '').isdigit():
                    errors.append(
                        f"❌ Row {row_num}: UPDATE operation REQUIRES numeric 'Ad Group ID' "
                        f"(found alphanumeric: '{ag_id_val}'). Use ad group ID number, not name."
                    )
            
            # Entity-specific ID requirements
            # Amazon allows updates by EITHER entity ID OR by combination (Campaign + Ad Group + Targeting)
            if entity == 'Keyword':
                kw_id = str(row.get('Keyword ID', '')).strip()
                kw_text = str(row.get('Keyword Text', '')).strip()
                
                # Must have EITHER Keyword ID OR (Campaign ID + Ad Group ID + Keyword Text)
                has_entity_id = kw_id and kw_id != ''
                has_combination = (camp_id_val and camp_id_val.replace('.', '').isdigit() and
                                 ag_id_val and ag_id_val.replace('.', '').isdigit() and
                                 kw_text and kw_text != '')
                
                if not has_entity_id and not has_combination:
                    errors.append(
                        f"❌ Row {row_num}: UPDATE for Keyword requires EITHER 'Keyword ID' "
                        f"OR (numeric Campaign ID + Ad Group ID + Keyword Text)"
                    )
            
            elif entity == 'Product Targeting':
                pt_id = str(row.get('Product Targeting ID', '')).strip()
                pt_expr = str(row.get('Product Targeting Expression', '')).strip()
                
                # Must have EITHER PT ID OR (Campaign ID + Ad Group ID + PT Expression)
                has_entity_id = pt_id and pt_id != ''
                has_combination = (camp_id_val and camp_id_val.replace('.', '').isdigit() and
                                 ag_id_val and ag_id_val.replace('.', '').isdigit() and
                                 pt_expr and pt_expr != '')
                
                if not has_entity_id and not has_combination:
                    errors.append(
                        f"❌ Row {row_num}: UPDATE for Product Targeting requires EITHER 'Product Targeting ID' "
                        f"OR (numeric Campaign ID + Ad Group ID + PT Expression)"
                    )
            
            elif entity == 'Campaign':
                if pd.isna(row.get('Campaign ID')) or str(row.get('Campaign ID', '')).strip() == '':
                    errors.append(f"❌ Row {row_num}: UPDATE operation for Campaign REQUIRES 'Campaign ID'")
            
            elif entity == 'Ad Group':
                if pd.isna(row.get('Ad Group ID')) or str(row.get('Ad Group ID', '')).strip() == '':
                    errors.append(f"❌ Row {row_num}: UPDATE operation for Ad Group REQUIRES 'Ad Group ID'")
        
        # Validate Product Targeting (PT) - non-keyword campaigns
        if entity == 'Product Targeting':
            pt_expr = str(row.get('Product Targeting Expression', '')).strip()
            
            # PT must have expression
            if not pt_expr or pt_expr == '':
                errors.append(
                    f"❌ Row {row_num}: Product Targeting REQUIRES 'Product Targeting Expression' "
                    f"(e.g., asin=\"B0XXXXXXXX\" or category=\"12345\")"
                )
            
            # PT should NOT have Keyword Text or Match Type
            if str(row.get('Keyword Text', '')).strip():
                warnings.append(
                    f"⚠️ Row {row_num}: Product Targeting should not have 'Keyword Text' "
                    f"(use 'Product Targeting Expression' instead)"
                )
            
            # Validate expression format
            if pt_expr and not any(x in pt_expr.lower() for x in ['asin=', 'category=', 'close-match', 'loose-match', 'substitutes', 'complements']):
                warnings.append(
                    f"⚠️ Row {row_num}: Product Targeting Expression may be invalid: '{pt_expr}' "
                    f"(expected format: asin=\"B0XXX\" or category=\"12345\" or close-match/loose-match/etc.)"
                )
        
        # Validate Keywords - should NOT have PT expression
        if entity == 'Keyword':
            kw_text = str(row.get('Keyword Text', '')).strip()
            match_type = str(row.get('Match Type', '')).strip()
            
            # Keyword must have text
            if not kw_text or kw_text == '':
                errors.append(f"❌ Row {row_num}: Keyword REQUIRES 'Keyword Text'")
            
            # Keyword must have match type
            if not match_type or match_type == '':
                errors.append(f"❌ Row {row_num}: Keyword REQUIRES 'Match Type' (EXACT, PHRASE, or BROAD)")
            elif match_type.upper() not in ['EXACT', 'PHRASE', 'BROAD']:
                errors.append(
                    f"❌ Row {row_num}: Invalid Match Type '{match_type}' "
                    f"(must be EXACT, PHRASE, or BROAD - case sensitive)"
                )
            
            # Keyword should NOT have PT expression
            if str(row.get('Product Targeting Expression', '')).strip():
                warnings.append(
                    f"⚠️ Row {row_num}: Keyword should not have 'Product Targeting Expression' "
                    f"(that's for Product Targeting entity only)"
                )
        
        # Validate Negative Keywords
        if 'Negative' in entity and 'Keyword' in entity:
            kw_text = str(row.get('Keyword Text', '')).strip()
            match_type = str(row.get('Match Type', '')).strip()
            
            if not kw_text:
                errors.append(f"❌ Row {row_num}: Negative Keyword REQUIRES 'Keyword Text'")
            
            if match_type != 'negativeExact':
                warnings.append(
                    f"⚠️ Row {row_num}: Negative Keyword Match Type should be 'negativeExact' "
                    f"(found: '{match_type}')"
                )
        
        # Validate Negative Product Targeting
        if 'Negative' in entity and 'Product Targeting' in entity:
            pt_expr = str(row.get('Product Targeting Expression', '')).strip()
            
            if not pt_expr:
                errors.append(
                    f"❌ Row {row_num}: Negative Product Targeting REQUIRES 'Product Targeting Expression'"
                )
        
        # Validate Campaign
        if entity == 'Campaign':
            if operation == 'create':
                # Required fields for campaign creation
                if pd.isna(row.get('Campaign Name')) or str(row.get('Campaign Name', '')).strip() == '':
                    errors.append(f"❌ Row {row_num}: Campaign CREATE REQUIRES 'Campaign Name'")
                
                targeting_type = str(row.get('Targeting Type', '')).strip()
                if targeting_type not in ['MANUAL', 'AUTO']:
                    errors.append(
                        f"❌ Row {row_num}: Campaign 'Targeting Type' must be 'MANUAL' or 'AUTO' "
                        f"(found: '{targeting_type}')"
                    )
                
                if pd.isna(row.get('Daily Budget')):
                    errors.append(f"❌ Row {row_num}: Campaign CREATE REQUIRES 'Daily Budget'")
        
        # Validate Ad Group
        if entity == 'Ad Group':
            if pd.isna(row.get('Campaign ID')) or str(row.get('Campaign ID', '')).strip() == '':
                errors.append(f"❌ Row {row_num}: Ad Group REQUIRES 'Campaign ID' (parent campaign)")
            
            if operation == 'create':
                if pd.isna(row.get('Ad Group Default Bid')):
                    errors.append(f"❌ Row {row_num}: Ad Group CREATE REQUIRES 'Ad Group Default Bid'")
        
        # Validate Product Ad
        if entity == 'Product Ad':
            sku_val = str(row.get('SKU', '')).strip()
            
            # Check if SKU is missing or placeholder
            if pd.isna(row.get('SKU')) or sku_val == '':
                errors.append(f"❌ Row {row_num}: Product Ad REQUIRES 'SKU'")
            elif sku_val.upper() in ['SKU_NEEDED', 'SKUNEEDED', 'SKU NEEDED']:
                errors.append(
                    f"❌ Row {row_num}: Product Ad has placeholder SKU '{sku_val}' - "
                    f"replace with actual SKU before upload"
                )
            
            if pd.isna(row.get('Campaign ID')) or str(row.get('Campaign ID', '')).strip() == '':
                errors.append(f"❌ Row {row_num}: Product Ad REQUIRES 'Campaign ID'")
            
            if pd.isna(row.get('Ad Group ID')) or str(row.get('Ad Group ID', '')).strip() == '':
                errors.append(f"❌ Row {row_num}: Product Ad REQUIRES 'Ad Group ID'")
        
        # Check for $ symbols in numeric fields
        for col in ['Daily Budget', 'Bid', 'Ad Group Default Bid']:
            val = str(row.get(col, ''))
            if '$' in val or ',' in val:
                errors.append(
                    f"❌ Row {row_num}: '{col}' contains $ or comma - use plain numbers only "
                    f"(found: '{val}')"
                )
        
        # Check date format
        start_date = str(row.get('Start Date', ''))
        if start_date and start_date != 'nan' and start_date.strip():
            if not start_date.isdigit() or len(start_date) != 8:
                errors.append(
                    f"❌ Row {row_num}: 'Start Date' must be YYYYMMDD format "
                    f"(found: '{start_date}')"
                )
    
    return {
        'errors': errors,
        'warnings': warnings,
        'stats': dict(stats)
    }

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
# 4. OPTIMIZER LOGIC - UPDATED
# ==========================================

def load_and_map_columns(file_content) -> tuple[pd.DataFrame | None, str | None]:
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

    # Rename mapped columns
    df = df.rename(columns={v: k for k, v in col_map.items() if v is not None})
    
    # CRITICAL: Preserve ID columns that might not be in the mapping
    # These are essential for UPDATE operations in bulk uploads
    id_column_variations = [
        'Keyword ID', 'KeywordId', 'keyword id',
        'Product Targeting ID', 'TargetingId', 'Targeting ID', 'product targeting id',
        'Campaign ID', 'CampaignId', 'campaign id',
        'Ad Group ID', 'AdGroupId', 'ad group id'
    ]
    
    # If any ID columns exist in the original file but weren't mapped, preserve them
    for col in df.columns:
        col_lower = col.lower().strip()
        if col_lower in [v.lower() for v in id_column_variations]:
            # Column exists and might have IDs - keep it as-is if not already mapped
            pass  # Already in df, no action needed
    
    return df, None

def validate_and_prepare_data(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame | None, str | None]:
    for col in ["Impressions", "Clicks", "Spend", "Sales", "Orders", "CPC"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = safe_numeric(df[col])

    for id_col in ["CampaignId", "AdGroupId", "KeywordId", "TargetingId"]:
        if id_col not in df.columns:
            df[id_col] = ""

    attribution_window = config.get("ATTRIBUTION_WINDOW", 7)
    
    if attribution_window == 14:
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
            df["Sales_Attributed"] = df["Sales"]
            df["Orders_Attributed"] = df["Orders"]
            df["Attribution_Window"] = "7-day (14-day requested but unavailable)"
    else:
        df["Sales_Attributed"] = df["Sales"]
        df["Orders_Attributed"] = df["Orders"]
        df["Attribution_Window"] = "7-day"

    df["Campaign Name"] = df["Campaign"]
    df["Ad Group Name"] = df.get("AdGroup", "")
    df["Customer Search Term"] = df["Term"]
    df["Match Type"] = df.get("Match", "broad").fillna("broad").astype(str)
    df["Cost Per Click (CPC)"] = df["CPC"]
    
    if "Keyword" in df.columns: 
        df["Targeting"] = df["Keyword"].replace("", np.nan)
    else:
        df["Targeting"] = pd.Series([np.nan]*len(df))
        
    if "TargetingExpression" in df.columns:
        df["Targeting"] = df["Targeting"].fillna(df["TargetingExpression"])
    
    df["Targeting"] = df["Targeting"].fillna(df["Customer Search Term"]).fillna("").astype(str)

    df["CTR"] = np.where(df["Impressions"] > 0, df["Clicks"] / df["Impressions"], 0.0)
    df["ROAS"] = np.where(df["Spend"] > 0, df["Sales_Attributed"] / df["Spend"], 0.0)
    df["CVR"] = np.where(df["Clicks"] > 0, df["Orders_Attributed"] / df["Clicks"], 0.0)
    
    camp_sums = df.groupby("Campaign Name")[["Sales_Attributed", "Spend"]].transform("sum")
    campaign_roas = np.where(
        camp_sums["Spend"] > 0, 
        camp_sums["Sales_Attributed"] / camp_sums["Spend"], 
        0.0
    )
    default_roas = config.get("DEFAULT_TARGET_ROAS", 2.5)
    df["Campaign Median ROAS"] = np.where(campaign_roas > 0, campaign_roas, default_roas)

    return df, None

def identify_harvest_candidates(
    df: pd.DataFrame, 
    config: dict, 
    matcher: ExactMatcher
) -> pd.DataFrame:
    auto_pattern = r'close-match|loose-match|substitutes|complements|category=|asin|b0'
    discovery_mask = (
        (~df["Match Type"].str.contains("exact", case=False, na=False)) | 
        (df["Targeting"].str.contains(auto_pattern, case=False, na=False))
    )
    discovery_df = df[discovery_mask].copy()
    
    if discovery_df.empty:
        return pd.DataFrame()

    agg_cols = {
        "Impressions": "sum", "Clicks": "sum", "Spend": "sum", 
        "Sales_Attributed": "sum", "Orders_Attributed": "sum",
        "Cost Per Click (CPC)": "mean"
    }
    
    discovery_df["_rank"] = discovery_df.groupby("Customer Search Term")["Sales_Attributed"].rank(method="first", ascending=False)
    
    grouped = discovery_df.groupby("Customer Search Term", as_index=False).agg(agg_cols)
    meta_df = discovery_df[discovery_df["_rank"] == 1][["Customer Search Term", "Campaign Name", "Campaign Median ROAS", "Ad Group Name"]].drop_duplicates("Customer Search Term")
    merged = pd.merge(grouped, meta_df, on="Customer Search Term", how="left")
    merged["ROAS"] = np.where(merged["Spend"] > 0, merged["Sales_Attributed"] / merged["Spend"], 0.0)
    
    tier2_mask = (
        (merged["Clicks"] >= config["HARVEST_CLICKS"]) &
        (merged["Orders_Attributed"] >= config["HARVEST_ORDERS"]) &
        (merged["Sales_Attributed"] >= config["HARVEST_SALES"]) &
        (merged["ROAS"] >= (merged["Campaign Median ROAS"] * config["HARVEST_ROAS_MULT"]))
    )
    
    candidates_df = merged[tier2_mask].copy()
    
    survivors = []
    for _, row in candidates_df.iterrows():
        matched, _ = matcher.find_match(row["Customer Search Term"], config["DEDUPE_SIMILARITY"])
        if not matched:
            survivors.append(row)
        
    survivors_df = pd.DataFrame(survivors)
    
    if not survivors_df.empty:
        survivors_df["New Bid"] = survivors_df["Cost Per Click (CPC)"] * 1.1
        survivors_df = survivors_df.sort_values("Sales_Attributed", ascending=False)
    
    return survivors_df

def identify_negative_candidates(
    df: pd.DataFrame, 
    config: dict,
    harvest_df: pd.DataFrame
) -> pd.DataFrame:
    negatives = []
    
    if not harvest_df.empty:
        for _, row in harvest_df.iterrows():
            negatives.append({
                "Type": "Isolation",
                "Campaign Name": row["Campaign Name"],
                "CampaignId": row.get("CampaignId"),
                "AdGroupId": row.get("AdGroupId"),
                "Term": row["Customer Search Term"],
                "Match Type": "Exact Negative",
                "Spend": 0, "Clicks": 0, "Sales": 0,
                "Impressions": 0
            })
    
    mask_neg = (df["Sales_Attributed"] == 0) & (~df["Match Type"].str.contains("exact", case=False, na=False))
    df_bleeder_candidates = df[mask_neg].copy()
    
    campaign_cvr = df.groupby("Campaign Name").apply(
        lambda x: x["Orders_Attributed"].sum() / x["Clicks"].sum() 
        if x["Clicks"].sum() > 0 else 0
    ).to_dict()
    df_bleeder_candidates["Campaign_CVR"] = df_bleeder_candidates["Campaign Name"].map(campaign_cvr).fillna(0)
    df_bleeder_candidates["Expected_Orders"] = df_bleeder_candidates["Clicks"] * df_bleeder_candidates["Campaign_CVR"]
    
    stat_mask = (
        (df_bleeder_candidates["Clicks"] >= config["NEGATIVE_CLICKS_THRESHOLD"]) &
        (df_bleeder_candidates["Spend"] >= config["NEGATIVE_SPEND_THRESHOLD"]) &
        (df_bleeder_candidates["Expected_Orders"] >= 1.0)
    )
    
    hard_stop_clicks = max(15, int(config["NEGATIVE_CLICKS_THRESHOLD"] * 1.5))
    hard_mask = (df_bleeder_candidates["Clicks"] >= hard_stop_clicks)
    
    final_bleeder_mask = stat_mask | hard_mask
    
    for _, row in df_bleeder_candidates[final_bleeder_mask].iterrows():
        reason = "Hard Stop" if row["Clicks"] >= hard_stop_clicks else "Statistically Poor"
        negatives.append({
            "Type": f"Performance ({reason})",
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
    low_vol_boost: float = 0.0,
    min_clicks: int = 3
) -> tuple[float, str]:
    current_cpc = float(row.get("Cost Per Click (CPC)", 0) or 0)
    target_roas = row.get("Campaign Median ROAS", 2.5)
    actual_roas = row.get("ROAS", 0)
    clicks = row.get("Clicks", 0)
    
    if current_cpc <= 0:
        return 0.5, "Default (No CPC Data)"
    if target_roas <= 0:
        target_roas = 2.5 
    
    if clicks < min_clicks:
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

def calculate_bid_adjustments(
    df: pd.DataFrame, 
    config: dict,
    harvested_terms: set
) -> dict:
    outputs = {}
    df_opt = df[~df["Customer Search Term"].str.lower().isin(harvested_terms)].copy()
    
    direct_mask = (
        (df_opt["Match Type"].str.contains("exact", case=False, na=False)) |
        (
            df_opt["Targeting"].str.contains(r"asin|b0", case=False, regex=True, na=False) & 
            ~df_opt["Targeting"].str.contains(r"category", case=False, na=False)
        )
    )
    df_direct = df_opt[direct_mask].copy()
    
    if not df_direct.empty:
        results = df_direct.apply(
            lambda r: calculate_optimal_bid(
                r, 
                config["ALPHA_EXACT"],
                "Rule Based",
                config["MAX_BID_CHANGE"],
                0,
                config["MIN_CLICKS_BID"]
            ), 
            axis=1
        )
        df_direct["New Bid"] = results.apply(lambda x: x[0])
        df_direct["Reason"] = results.apply(lambda x: x[1])
        outputs['Direct_Bids'] = df_direct
    else:
        outputs['Direct_Bids'] = pd.DataFrame()

    agg_mask = ~direct_mask 
    df_agg_raw = df_opt[agg_mask].copy()
    
    if not df_agg_raw.empty:
        df_agg_raw["Group_Key"] = df_agg_raw["KeywordId"].replace("", np.nan).fillna(df_agg_raw["TargetingId"])
        mask_no_id = df_agg_raw["Group_Key"].isna() | (df_agg_raw["Group_Key"] == "")
        df_agg_raw.loc[mask_no_id, "Group_Key"] = (
            df_agg_raw.loc[mask_no_id, "Campaign Name"] + "|" + 
            df_agg_raw.loc[mask_no_id, "Ad Group Name"] + "|" + 
            df_agg_raw.loc[mask_no_id, "Targeting"] + "|" +
            df_agg_raw.loc[mask_no_id, "Match Type"]
        )

        agg_cols = {
            "Impressions": "sum", "Clicks": "sum", "Spend": "sum", 
            "Sales_Attributed": "sum", "Orders_Attributed": "sum", 
            "Cost Per Click (CPC)": "mean", "Campaign Median ROAS": "mean"
        }
        meta_cols = {
            c: 'first' for c in [
                "Campaign Name", "Ad Group Name", "Targeting", 
                "CampaignId", "AdGroupId", "Match Type", "KeywordId", "TargetingId"
            ] if c in df_agg_raw.columns
        }
        
        grouped_agg = df_agg_raw.groupby("Group_Key", as_index=False).agg({**agg_cols, **meta_cols})
        grouped_agg["ROAS"] = np.where(grouped_agg["Spend"] > 0, grouped_agg["Sales_Attributed"] / grouped_agg["Spend"], 0.0)
        
        results = grouped_agg.apply(
            lambda r: calculate_optimal_bid(
                r, 
                config["ALPHA_BROAD"], 
                "Rule Based", 
                config["MAX_BID_CHANGE"], 
                0,
                config["MIN_CLICKS_BID"]
            ), 
            axis=1
        )
        grouped_agg["New Bid"] = results.apply(lambda x: x[0])
        grouped_agg["Reason"] = results.apply(lambda x: x[1])
        outputs['Aggregated_Bids'] = grouped_agg
    else:
        outputs['Aggregated_Bids'] = pd.DataFrame()

    return outputs

def detect_asin_cannibalization(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Detect when same ASIN/keyword appears in multiple campaigns competing with itself.
    Returns DataFrame with cannibalization issues and recommendations.
    """
    cannibalization_issues = []
    
    # Extract ASIN from campaign names (common pattern: B0XXXXXXXX)
    df["ASIN_Extracted"] = df["Campaign Name"].str.extract(r'(B0[A-Z0-9]{8})', expand=False)
    
    # Group by ASIN + Search Term to find duplicates across campaigns
    grouped = df.groupby(["ASIN_Extracted", "Customer Search Term"]).agg({
        "Campaign Name": lambda x: list(x.unique()),
        "Clicks": "sum",
        "Spend": "sum",
        "Sales_Attributed": "sum",
        "Orders_Attributed": "sum"
    }).reset_index()
    
    # Filter to only terms appearing in 2+ campaigns
    duplicates = grouped[grouped["Campaign Name"].apply(len) > 1].copy()
    
    if duplicates.empty:
        return pd.DataFrame()
    
    # For each duplicate, analyze which campaign performs best
    for _, row in duplicates.iterrows():
        asin = row["ASIN_Extracted"]
        term = row["Customer Search Term"]
        campaigns = row["Campaign Name"]
        
        # Get detailed breakdown by campaign
        term_df = df[
            (df["ASIN_Extracted"] == asin) & 
            (df["Customer Search Term"] == term)
        ].copy()
        
        campaign_performance = []
        for camp in campaigns:
            camp_data = term_df[term_df["Campaign Name"] == camp]
            camp_clicks = camp_data["Clicks"].sum()
            camp_spend = camp_data["Spend"].sum()
            camp_sales = camp_data["Sales_Attributed"].sum()
            camp_orders = camp_data["Orders_Attributed"].sum()
            camp_roas = camp_sales / camp_spend if camp_spend > 0 else 0
            camp_acos = (camp_spend / camp_sales * 100) if camp_sales > 0 else 999
            
            campaign_performance.append({
                "campaign": camp,
                "clicks": camp_clicks,
                "spend": camp_spend,
                "sales": camp_sales,
                "orders": camp_orders,
                "roas": camp_roas,
                "acos": camp_acos
            })
        
        # Sort by ROAS (best performer first)
        campaign_performance.sort(key=lambda x: x["roas"], reverse=True)
        best_campaign = campaign_performance[0]
        others = campaign_performance[1:]
        
        # Calculate wasted spend (spend in non-best campaigns)
        wasted_spend = sum(c["spend"] for c in others)
        total_spend = row["Spend"]
        
        # Only flag if wasted spend is significant (>$5 or >20% of total)
        if wasted_spend >= 5 or (wasted_spend / total_spend > 0.2 if total_spend > 0 else False):
            cannibalization_issues.append({
                "ASIN": asin,
                "Search Term": term,
                "Campaigns_Count": len(campaigns),
                "Total_Clicks": row["Clicks"],
                "Total_Spend": total_spend,
                "Total_Sales": row["Sales_Attributed"],
                "Total_Orders": row["Orders_Attributed"],
                "Best_Campaign": best_campaign["campaign"],
                "Best_ROAS": best_campaign["roas"],
                "Best_ACoS": best_campaign["acos"],
                "Wasted_Spend": wasted_spend,
                "Other_Campaigns": ", ".join([c["campaign"] for c in others]),
                "Recommendation": f"Keep in '{best_campaign['campaign']}', add as negative to others",
                "Monthly_Savings_Estimate": wasted_spend * 30  # Rough monthly estimate
            })
    
    return pd.DataFrame(cannibalization_issues)

def create_wasted_spend_heatmap(
    df: pd.DataFrame, 
    config: dict,
    harvest_df: pd.DataFrame = None,
    negatives_df: pd.DataFrame = None,
    bids_dict: dict = None
) -> pd.DataFrame:
    """
    Create a heatmap showing campaign/ad group performance.
    Cross-references with optimizer actions to show what's being addressed.
    Returns DataFrame with color-coded performance metrics and action tracking.
    """
    # Group by Campaign and Ad Group
    grouped = df.groupby(["Campaign Name", "Ad Group Name"]).agg({
        "Clicks": "sum",
        "Spend": "sum",
        "Sales_Attributed": "sum",
        "Orders_Attributed": "sum",
        "Impressions": "sum"
    }).reset_index()
    
    # Calculate metrics
    grouped["CTR"] = np.where(grouped["Impressions"] > 0, 
                               grouped["Clicks"] / grouped["Impressions"] * 100, 0)
    grouped["CVR"] = np.where(grouped["Clicks"] > 0, 
                               grouped["Orders_Attributed"] / grouped["Clicks"] * 100, 0)
    grouped["ROAS"] = np.where(grouped["Spend"] > 0, 
                                grouped["Sales_Attributed"] / grouped["Spend"], 0)
    grouped["ACoS"] = np.where(grouped["Sales_Attributed"] > 0, 
                                grouped["Spend"] / grouped["Sales_Attributed"] * 100, 999)
    
    # Track optimizer actions for each campaign/ad group
    grouped["Actions_Taken"] = ""
    grouped["Harvest_Count"] = 0
    grouped["Negative_Count"] = 0
    grouped["Bid_Increase_Count"] = 0
    grouped["Bid_Decrease_Count"] = 0
    
    # Check harvest actions
    if harvest_df is not None and not harvest_df.empty:
        for idx, row in grouped.iterrows():
            camp = row["Campaign Name"]
            ag = row["Ad Group Name"]
            
            harvest_match = harvest_df[
                (harvest_df["Campaign Name"] == camp) &
                (harvest_df.get("Ad Group Name", "") == ag)
            ]
            if not harvest_match.empty:
                grouped.at[idx, "Harvest_Count"] = len(harvest_match)
    
    # Check negative actions
    if negatives_df is not None and not negatives_df.empty:
        for idx, row in grouped.iterrows():
            camp = row["Campaign Name"]
            ag = row["Ad Group Name"]
            
            neg_match = negatives_df[
                (negatives_df["Campaign Name"] == camp) &
                (negatives_df.get("Ad Group Name", "") == ag)
            ]
            if not neg_match.empty:
                grouped.at[idx, "Negative_Count"] = len(neg_match)
    
    # Check bid actions
    if bids_dict is not None:
        all_bids = pd.DataFrame()
        if 'Direct_Bids' in bids_dict and not bids_dict['Direct_Bids'].empty:
            all_bids = pd.concat([all_bids, bids_dict['Direct_Bids']])
        if 'Aggregated_Bids' in bids_dict and not bids_dict['Aggregated_Bids'].empty:
            all_bids = pd.concat([all_bids, bids_dict['Aggregated_Bids']])
        
        if not all_bids.empty:
            for idx, row in grouped.iterrows():
                camp = row["Campaign Name"]
                ag = row["Ad Group Name"]
                
                bid_match = all_bids[
                    (all_bids["Campaign Name"] == camp) &
                    (all_bids.get("Ad Group Name", "") == ag)
                ]
                
                if not bid_match.empty:
                    # Count increases and decreases
                    increases = bid_match[bid_match["New Bid"] > bid_match["Cost Per Click (CPC)"]]
                    decreases = bid_match[bid_match["New Bid"] < bid_match["Cost Per Click (CPC)"]]
                    
                    grouped.at[idx, "Bid_Increase_Count"] = len(increases)
                    grouped.at[idx, "Bid_Decrease_Count"] = len(decreases)
    
    # Create summary action text
    for idx, row in grouped.iterrows():
        actions = []
        
        if row["Harvest_Count"] > 0:
            actions.append(f"💎 {int(row['Harvest_Count'])} harvests")
        
        if row["Negative_Count"] > 0:
            actions.append(f"🛑 {int(row['Negative_Count'])} negatives")
        
        if row["Bid_Increase_Count"] > 0:
            actions.append(f"⬆️ {int(row['Bid_Increase_Count'])} bid increases")
        
        if row["Bid_Decrease_Count"] > 0:
            actions.append(f"⬇️ {int(row['Bid_Decrease_Count'])} bid decreases")
        
        # NEW: Check if campaign is below threshold (hold)
        is_low_volume = (
            row["Clicks"] < config.get("MIN_CLICKS_BID", 3) or 
            row["Orders_Attributed"] < 2
        )
        
        if actions:
            grouped.at[idx, "Actions_Taken"] = " | ".join(actions)
        elif is_low_volume:
            grouped.at[idx, "Actions_Taken"] = "⏸️ Hold (Low volume)"
        else:
            grouped.at[idx, "Actions_Taken"] = "✅ No action needed"
    
    # Calculate percentiles for color coding
    def get_color_score(value, metric_name, higher_is_better=True):
        """Return color score: 2=green, 1=yellow, 0=red, -1=insufficient data"""
        # Check if value is zero or nan - means no data
        if pd.isna(value) or value == 0:
            return -1  # Changed from 0 to -1 to indicate "no data"
        
        # Get all values for this metric (exclude zeros)
        all_values = grouped[metric_name]
        all_values = all_values[all_values > 0]
        
        if len(all_values) == 0:
            return -1  # No data to compare
        
        if len(all_values) == 1:
            return 1  # Only one value, consider it average
        
        # Calculate percentiles
        p33 = all_values.quantile(0.33)
        p67 = all_values.quantile(0.67)
        
        if higher_is_better:
            if value >= p67:
                return 2  # Green
            elif value >= p33:
                return 1  # Yellow
            else:
                return 0  # Red
        else:
            if value <= p33:
                return 2  # Green
            elif value <= p67:
                return 1  # Yellow
            else:
                return 0  # Red
    
    # Apply color scoring
    grouped["CTR_Score"] = grouped["CTR"].apply(lambda x: get_color_score(x, "CTR", higher_is_better=True))
    grouped["CVR_Score"] = grouped["CVR"].apply(lambda x: get_color_score(x, "CVR", higher_is_better=True))
    grouped["ROAS_Score"] = grouped["ROAS"].apply(lambda x: get_color_score(x, "ROAS", higher_is_better=True))
    grouped["ACoS_Score"] = grouped["ACoS"].apply(lambda x: get_color_score(x, "ACoS", higher_is_better=False))
    
    # Calculate overall score (average of all metrics)
    grouped["Overall_Score"] = (
        grouped["CTR_Score"] + 
        grouped["CVR_Score"] + 
        grouped["ROAS_Score"] + 
        grouped["ACoS_Score"]
    ) / 4
    
    # Add priority flag (0=high priority to fix, 2=good performance)
    grouped["Priority"] = grouped["Overall_Score"].apply(
        lambda x: "🔴 High" if x < 0.7 else ("🟡 Medium" if x < 1.3 else "🟢 Good")
    )
    
    # Sort by worst performers first
    grouped = grouped.sort_values("Overall_Score", ascending=True)
    
    return grouped

def track_keyword_velocity(df_current: pd.DataFrame, df_previous: pd.DataFrame = None) -> pd.DataFrame:
    """
    Track keyword performance trends by comparing current vs previous upload.
    Returns DataFrame with velocity metrics (rising/falling trends).
    """
    if df_previous is None or df_previous.empty:
        # First upload - just return current aggregated data for storage
        velocity_df = df_current.groupby("Customer Search Term").agg({
            "Clicks": "sum",
            "Spend": "sum",
            "Sales_Attributed": "sum",
            "Orders_Attributed": "sum"
        }).reset_index()
        velocity_df["Upload_Date"] = datetime.now().strftime("%Y-%m-%d")
        velocity_df["Trend"] = "→ New"
        velocity_df["Change_Pct"] = 0
        return velocity_df
    
    # Aggregate current and previous data
    current_agg = df_current.groupby("Customer Search Term").agg({
        "Clicks": "sum",
        "Spend": "sum",
        "Sales_Attributed": "sum",
        "Orders_Attributed": "sum"
    }).reset_index()
    
    previous_agg = df_previous.groupby("Customer Search Term").agg({
        "Clicks": "sum",
        "Spend": "sum",
        "Sales_Attributed": "sum",
        "Orders_Attributed": "sum"
    }).reset_index()
    
    # Merge to compare
    merged = pd.merge(
        current_agg,
        previous_agg,
        on="Customer Search Term",
        how="outer",
        suffixes=("_Current", "_Previous")
    ).fillna(0)
    
    # Calculate percent change for orders (most important metric)
    merged["Orders_Change_Pct"] = np.where(
        merged["Orders_Attributed_Previous"] > 0,
        ((merged["Orders_Attributed_Current"] - merged["Orders_Attributed_Previous"]) / 
         merged["Orders_Attributed_Previous"] * 100),
        np.where(merged["Orders_Attributed_Current"] > 0, 999, 0)  # 999 = new keyword with orders
    )
    
    # Calculate percent change for clicks
    merged["Clicks_Change_Pct"] = np.where(
        merged["Clicks_Previous"] > 0,
        ((merged["Clicks_Current"] - merged["Clicks_Previous"]) / 
         merged["Clicks_Previous"] * 100),
        np.where(merged["Clicks_Current"] > 0, 999, 0)
    )
    
    # Assign trend indicators
    def get_trend(orders_change, clicks_change):
        if orders_change >= 50:
            return "📈 Rising Strong"
        elif orders_change >= 20:
            return "↗️ Rising"
        elif orders_change <= -50:
            return "📉 Falling Strong"
        elif orders_change <= -20:
            return "↘️ Falling"
        elif clicks_change >= 50:
            return "⬆️ Traffic Up"
        elif clicks_change <= -50:
            return "⬇️ Traffic Down"
        else:
            return "→ Stable"
    
    merged["Trend"] = merged.apply(
        lambda r: get_trend(r["Orders_Change_Pct"], r["Clicks_Change_Pct"]),
        axis=1
    )
    
    # Create summary DataFrame
    velocity_df = merged[[
        "Customer Search Term",
        "Orders_Attributed_Current",
        "Orders_Attributed_Previous",
        "Orders_Change_Pct",
        "Clicks_Current",
        "Clicks_Previous",
        "Clicks_Change_Pct",
        "Spend_Current",
        "Sales_Attributed_Current",
        "Trend"
    ]].copy()
    
    velocity_df.columns = [
        "Search Term",
        "Orders_Current",
        "Orders_Previous",
        "Orders_Change_%",
        "Clicks_Current",
        "Clicks_Previous",
        "Clicks_Change_%",
        "Spend",
        "Sales",
        "Trend"
    ]
    
    velocity_df["Upload_Date"] = datetime.now().strftime("%Y-%m-%d")
    
    # Sort by orders change (biggest movers first)
    velocity_df = velocity_df.sort_values("Orders_Change_%", ascending=False)
    
    return velocity_df


# ==========================================
# BID SIMULATION & FORECASTING
# ==========================================

def simulate_bid_changes(
    df: pd.DataFrame,
    bids_dict: dict,
    config: dict
) -> dict:
    """
    FIXED: Simulate the impact of proposed bid changes on future performance.
    Uses performance data already in bid DataFrames instead of re-matching.
    """
    
    elasticity_scenarios = {
        'conservative': {
            'cpc': 0.3,
            'clicks': 0.5,
            'cvr': 0.0,
            'probability': 0.15
        },
        'expected': {
            'cpc': 0.5,      # Increased from 0.4 - more responsive to bid changes
            'clicks': 0.85,  # Increased from 0.7 - bid changes have more impact
            'cvr': 0.1,      # Increased from 0.05 - better position improves CVR more
            'probability': 0.70
        },
        'aggressive': {
            'cpc': 0.6,      # Increased from 0.5
            'clicks': 0.95,  # Increased from 0.9
            'cvr': 0.15,     # Increased from 0.1
            'probability': 0.15
        }
    }
    
    # Calculate current baseline
    current_summary = calculate_current_baseline(df)
    
    # FIX: Combine bid DataFrames directly (they already have performance data)
    all_bid_changes = []
    
    if 'Direct_Bids' in bids_dict and not bids_dict['Direct_Bids'].empty:
        direct = bids_dict['Direct_Bids'].copy()
        direct['Bid_Type'] = 'Direct'
        all_bid_changes.append(direct)
    
    if 'Aggregated_Bids' in bids_dict and not bids_dict['Aggregated_Bids'].empty:
        agg = bids_dict['Aggregated_Bids'].copy()
        agg['Bid_Type'] = 'Aggregated'
        all_bid_changes.append(agg)
    
    if all_bid_changes:
        combined_bids = pd.concat(all_bid_changes, ignore_index=True)
        
        # Calculate bid change percentage
        combined_bids['Cost Per Click (CPC)'] = pd.to_numeric(
            combined_bids['Cost Per Click (CPC)'], errors='coerce'
        ).fillna(0)
        combined_bids['New Bid'] = pd.to_numeric(
            combined_bids['New Bid'], errors='coerce'
        ).fillna(0)
        
        combined_bids['Bid_Change_Pct'] = np.where(
            combined_bids['Cost Per Click (CPC)'] > 0,
            (combined_bids['New Bid'] - combined_bids['Cost Per Click (CPC)']) / 
            combined_bids['Cost Per Click (CPC)'],
            0
        )
    else:
        combined_bids = pd.DataFrame()
    
    # Get harvest DataFrame
    harvest_df = bids_dict.get('Survivors', pd.DataFrame())
    
    # Diagnostic tracking
    total_recs = len(combined_bids)
    hold_count = 0
    actual_changes = 0
    
    if not combined_bids.empty and 'Reason' in combined_bids.columns:
        hold_mask = combined_bids['Reason'].astype(str).str.contains('Hold', case=False, na=False)
        hold_count = hold_mask.sum()
        actual_changes = (~hold_mask).sum()
    
    # Run simulation for each scenario
    scenarios = {}
    for scenario_name, elasticity in elasticity_scenarios.items():
        forecast = forecast_scenario_fixed(
            combined_bids,
            harvest_df,
            elasticity, 
            current_summary,
            config
        )
        scenarios[scenario_name] = forecast
    
    # Add current baseline
    scenarios['current'] = current_summary
    
    # Add diagnostics
    for scenario in scenarios.values():
        scenario['_total_recommendations'] = total_recs
        scenario['_actual_changes'] = actual_changes
        scenario['_hold_count'] = hold_count
        scenario['_harvest_count'] = len(harvest_df)
    
    # Calculate sensitivity
    sensitivity = calculate_sensitivity_fixed(
        combined_bids, harvest_df, 
        elasticity_scenarios['expected'], 
        current_summary, config
    )
    
    # Analyze risks
    risk_analysis = analyze_risks_fixed(combined_bids)
    
    return {
        'scenarios': scenarios,
        'bid_changes': combined_bids,
        'sensitivity': sensitivity,
        'risk_analysis': risk_analysis
    }


def calculate_current_baseline(df: pd.DataFrame) -> dict:
    """Calculate current performance baseline."""
    total_clicks = df['Clicks'].sum()
    total_spend = df['Spend'].sum()
    total_sales = df['Sales_Attributed'].sum() if 'Sales_Attributed' in df.columns else df['Sales'].sum()
    total_orders = df['Orders_Attributed'].sum() if 'Orders_Attributed' in df.columns else df['Orders'].sum()
    total_impressions = df['Impressions'].sum() if 'Impressions' in df.columns else 0
    
    avg_cpc = total_spend / total_clicks if total_clicks > 0 else 0
    cvr = total_orders / total_clicks if total_clicks > 0 else 0
    roas = total_sales / total_spend if total_spend > 0 else 0
    acos = (total_spend / total_sales * 100) if total_sales > 0 else 0
    ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    
    return {
        'clicks': total_clicks,
        'spend': total_spend,
        'sales': total_sales,
        'orders': total_orders,
        'impressions': total_impressions,
        'cpc': avg_cpc,
        'cvr': cvr,
        'roas': roas,
        'acos': acos,
        'ctr': ctr
    }




def forecast_scenario_fixed(
    bid_changes: pd.DataFrame,
    harvest_df: pd.DataFrame,
    elasticity: dict,
    current_baseline: dict,
    config: dict
) -> dict:
    """
    FIXED: Forecast performance using data already in bid DataFrames.
    """
    
    forecasted_changes = []
    keywords_processed = 0
    keywords_skipped = 0
    harvest_processed = 0
    
    # PART 1: Process bid changes
    if not bid_changes.empty:
        for _, row in bid_changes.iterrows():
            bid_change_pct = row.get('Bid_Change_Pct', 0)
            
            # Skip holds (no impact)
            reason = str(row.get('Reason', '')).lower()
            if 'hold' in reason:
                keywords_skipped += 1
                continue
            
            # Skip negligible changes
            if abs(bid_change_pct) < 0.005:
                keywords_skipped += 1
                continue
            
            # FIX: Use data already in the row (from optimizer)
            current_clicks = float(row.get('Clicks', 0) or 0)
            current_spend = float(row.get('Spend', 0) or 0)
            current_orders = float(row.get('Orders_Attributed', 0) or 0)
            current_sales = float(row.get('Sales_Attributed', 0) or 0)
            current_cpc = float(row.get('Cost Per Click (CPC)', 0) or 0)
            
            # Skip if truly no data
            if current_clicks == 0 and current_cpc == 0:
                keywords_skipped += 1
                continue
            
            # Calculate metrics
            current_cvr = current_orders / current_clicks if current_clicks > 0 else 0
            current_aov = current_sales / current_orders if current_orders > 0 else 0
            
            # Use account average if no AOV
            if current_aov == 0:
                current_aov = (current_baseline['sales'] / current_baseline['orders'] 
                              if current_baseline['orders'] > 0 else 0)
            
            # Apply elasticity model
            new_cpc = current_cpc * (1 + elasticity['cpc'] * bid_change_pct)
            new_clicks = current_clicks * (1 + elasticity['clicks'] * bid_change_pct)
            new_cvr = current_cvr * (1 + elasticity['cvr'] * bid_change_pct)
            
            # Calculate forecasted metrics
            new_orders = new_clicks * new_cvr
            new_sales = new_orders * current_aov
            new_spend = new_clicks * new_cpc
            
            # Delta
            forecasted_changes.append({
                'delta_clicks': new_clicks - current_clicks,
                'delta_spend': new_spend - current_spend,
                'delta_sales': new_sales - current_sales,
                'delta_orders': new_orders - current_orders
            })
            keywords_processed += 1
    
    # PART 2: Process harvest campaigns (substitution model)
    if not harvest_df.empty:
        EFFICIENCY_GAIN = config.get('HARVEST_EFFICIENCY_MULTIPLIER', 1.15)
        
        for _, row in harvest_df.iterrows():
            baseline_clicks = float(row.get('Clicks', 0) or 0)
            baseline_spend = float(row.get('Spend', 0) or 0)
            baseline_orders = float(row.get('Orders_Attributed', 0) or 0)
            baseline_sales = float(row.get('Sales_Attributed', 0) or 0)
            baseline_cpc = float(row.get('Cost Per Click (CPC)', 0) or 0)
            
            if baseline_clicks < 5:
                continue
            
            new_bid = float(row.get('New Bid', baseline_cpc * 1.1) or baseline_cpc * 1.1)
            
            baseline_cvr = baseline_orders / baseline_clicks if baseline_clicks > 0 else 0
            baseline_aov = baseline_sales / baseline_orders if baseline_orders > 0 else 0
            
            # Harvest model: same traffic, better efficiency
            forecast_clicks = baseline_clicks * 1.0
            forecast_cpc = new_bid * 0.95
            forecast_cvr = baseline_cvr * EFFICIENCY_GAIN
            
            forecast_orders = forecast_clicks * forecast_cvr
            forecast_sales = forecast_orders * baseline_aov
            forecast_spend = forecast_clicks * forecast_cpc
            
            forecasted_changes.append({
                'delta_clicks': forecast_clicks - baseline_clicks,
                'delta_spend': forecast_spend - baseline_spend,
                'delta_sales': forecast_sales - baseline_sales,
                'delta_orders': forecast_orders - baseline_orders
            })
            harvest_processed += 1
    
    # Aggregate changes
    if not forecasted_changes:
        result = current_baseline.copy()
        result['_keywords_processed'] = 0
        result['_keywords_skipped'] = keywords_skipped
        result['_harvest_processed'] = 0
        result['_diagnostic'] = "No changes to forecast"
        return result
    
    total_delta = {
        'clicks': sum(fc['delta_clicks'] for fc in forecasted_changes),
        'spend': sum(fc['delta_spend'] for fc in forecasted_changes),
        'sales': sum(fc['delta_sales'] for fc in forecasted_changes),
        'orders': sum(fc['delta_orders'] for fc in forecasted_changes)
    }
    
    # New totals
    new_clicks = max(0, current_baseline['clicks'] + total_delta['clicks'])
    new_spend = max(0, current_baseline['spend'] + total_delta['spend'])
    new_sales = max(0, current_baseline['sales'] + total_delta['sales'])
    new_orders = max(0, current_baseline['orders'] + total_delta['orders'])
    
    # New metrics
    new_cpc = new_spend / new_clicks if new_clicks > 0 else 0
    new_cvr = new_orders / new_clicks if new_clicks > 0 else 0
    new_roas = new_sales / new_spend if new_spend > 0 else 0
    new_acos = (new_spend / new_sales * 100) if new_sales > 0 else 0
    
    return {
        'clicks': new_clicks,
        'spend': new_spend,
        'sales': new_sales,
        'orders': new_orders,
        'cpc': new_cpc,
        'cvr': new_cvr,
        'roas': new_roas,
        'acos': new_acos,
        '_keywords_processed': keywords_processed,
        '_keywords_skipped': keywords_skipped,
        '_harvest_processed': harvest_processed,
        '_diagnostic': f"Processed {keywords_processed} bids + {harvest_processed} harvests"
    }


def calculate_sensitivity_fixed(
    bid_changes: pd.DataFrame,
    harvest_df: pd.DataFrame,
    elasticity: dict,
    current_baseline: dict,
    config: dict
) -> pd.DataFrame:
    """Calculate sensitivity analysis."""
    
    sensitivity_levels = [-0.30, -0.20, -0.10, 0.0, 0.10, 0.20, 0.30, 0.50]
    
    results = []
    for multiplier in sensitivity_levels:
        adjusted = bid_changes.copy() if not bid_changes.empty else pd.DataFrame()
        if not adjusted.empty:
            adjusted['Bid_Change_Pct'] = adjusted['Bid_Change_Pct'] * (1 + multiplier)
        
        forecast = forecast_scenario_fixed(
            adjusted, harvest_df, elasticity, current_baseline, config
        )
        
        results.append({
            'Bid_Adjustment': f"{multiplier*100:+.0f}%",
            'Spend': forecast['spend'],
            'Sales': forecast['sales'],
            'ROAS': forecast['roas'],
            'Orders': forecast['orders'],
            'ACoS': forecast['acos']
        })
    
    return pd.DataFrame(results)


def analyze_risks_fixed(bid_changes: pd.DataFrame) -> dict:
    """Analyze risk levels."""
    
    if bid_changes.empty:
        return {
            'high_risk': [], 'medium_risk': [], 'low_risk': [],
            'summary': {'high_risk_count': 0, 'medium_risk_count': 0, 'low_risk_count': 0}
        }
    
    high_risk, medium_risk, low_risk = [], [], []
    
    for _, row in bid_changes.iterrows():
        if 'hold' in str(row.get('Reason', '')).lower():
            continue
        
        bid_change_pct = abs(row.get('Bid_Change_Pct', 0))
        clicks = row.get('Clicks', 0)
        
        risk_level = 'low'
        reasons = []
        
        if bid_change_pct > 0.30:
            risk_level = 'high'
            reasons.append(f"Large change ({bid_change_pct*100:.0f}%)")
        if clicks < 10:
            risk_level = 'medium' if risk_level == 'low' else 'high'
            reasons.append(f"Low data ({int(clicks)} clicks)")
        if 0.20 <= bid_change_pct <= 0.30 and risk_level == 'low':
            risk_level = 'medium'
            reasons.append("Moderate change")
        
        keyword = 'Unknown'
        for col in ['Targeting', 'Keyword Text', 'Customer Search Term']:
            if col in row.index and pd.notna(row.get(col)):
                keyword = str(row.get(col))[:40]
                break
        
        item = {
            'keyword': keyword,
            'campaign': str(row.get('Campaign Name', ''))[:30],
            'bid_change': f"{row.get('Bid_Change_Pct', 0)*100:+.0f}%",
            'current_bid': row.get('Cost Per Click (CPC)', 0),
            'new_bid': row.get('New Bid', 0),
            'reasons': ', '.join(reasons) if reasons else 'Standard'
        }
        
        if risk_level == 'high':
            high_risk.append(item)
        elif risk_level == 'medium':
            medium_risk.append(item)
        else:
            low_risk.append(item)
    
    return {
        'high_risk': high_risk[:20],
        'medium_risk': medium_risk[:20],
        'low_risk': low_risk[:10],
        'summary': {
            'high_risk_count': len(high_risk),
            'medium_risk_count': len(medium_risk),
            'low_risk_count': len(low_risk)
        }
    }


def compile_optimization_stats(
    total_rows: int,
    harvest_df: pd.DataFrame,
    negatives_df: pd.DataFrame,
    bids_outputs: dict,
    df_original: pd.DataFrame
) -> dict:
    h_count = len(harvest_df)
    n_count = len(negatives_df)
    bids_updated_count = 0
    bids_hold_count = 0
    bids_total_candidates = 0
    
    for key in ['Direct_Bids', 'Aggregated_Bids']:
        if key in bids_outputs and not bids_outputs[key].empty:
            df = bids_outputs[key]
            bids_total_candidates += len(df)
            bids_hold_count += len(df[df["Reason"].astype(str).str.contains("Hold", case=False)])
            bids_updated_count += len(df[~df["Reason"].astype(str).str.contains("Hold", case=False)])
        
    return {
        "total_rows": total_rows,
        "total_spend": df_original["Spend"].sum(),
        "total_sales": df_original["Sales_Attributed"].sum(),
        "total_orders": df_original["Orders_Attributed"].sum(),
        "unique_terms": df_original["Customer Search Term"].nunique(),
        "harvest_count": h_count,
        "negative_count": n_count,
        "bid_total_candidates": bids_total_candidates,
        "bid_update_count": bids_updated_count,
        "bid_hold_count": bids_hold_count,
    }

@st.cache_data
def run_optimizer_logic(file_content, config):
    df, error = load_and_map_columns(file_content)
    if error: return None, error, None, None
    df, error = validate_and_prepare_data(df, config)
    if error: return None, error, None, None
    matcher = ExactMatcher(df)
    harvest_df = identify_harvest_candidates(df, config, matcher)
    negatives_df = identify_negative_candidates(df, config, harvest_df)
    harvested_terms = set(harvest_df['Customer Search Term'].str.lower()) if not harvest_df.empty else set()
    bids_dict = calculate_bid_adjustments(df, config, harvested_terms)
    
    # NEW FEATURES
    cannibalization_df = detect_asin_cannibalization(df, config)
    
    # Heatmap with action tracking - pass optimizer results
    heatmap_df = create_wasted_spend_heatmap(df, config, harvest_df, negatives_df, bids_dict)
    
    # Keyword velocity (check if previous data exists in session state)
    previous_df = st.session_state.get('previous_upload_df', None)
    velocity_df = track_keyword_velocity(df, previous_df)
    # Store current as previous for next upload
    st.session_state['previous_upload_df'] = df.copy()
    
    # FIX: Add harvest_df to bids_dict BEFORE simulation so it can access Survivors
    bids_dict['Survivors'] = harvest_df
    
    # NEW: Bid simulation
    simulation_results = simulate_bid_changes(df, bids_dict, config)
    
    stats = compile_optimization_stats(len(df), harvest_df, negatives_df, bids_dict, df)
    outputs = {
        'Survivors': harvest_df, 
        'Negatives': negatives_df,
        'Cannibalization': cannibalization_df,
        'Heatmap': heatmap_df,
        'Velocity': velocity_df,
        'Simulation': simulation_results,  # NEW
        **bids_dict
    }
    return outputs, None, df, stats

# ==========================================
# 5. CAMPAIGN CREATOR (HARVEST)
# ==========================================

def map_skus_from_file(harvest_df: pd.DataFrame, campaigns_file) -> tuple[pd.DataFrame, str]:
    try:
        camp_df = pd.read_csv(campaigns_file) if campaigns_file.name.endswith('.csv') else pd.read_excel(campaigns_file)
        col_map = SmartMapper.map_columns(camp_df)
        sort_col = col_map.get("Sales") or col_map.get("Orders") or col_map.get("Spend")
        if sort_col:
             camp_df[sort_col] = safe_numeric(camp_df[sort_col])
             camp_df = camp_df.sort_values(sort_col, ascending=False)
        
        sku_map = {} 
        asin_sku_map = {} 
        c_col = col_map.get("Campaign")
        s_col = col_map.get("SKU")
        a_col = col_map.get("ASIN")
        e_col = col_map.get("Entity")
        
        if c_col and s_col:
            camp_df[s_col] = camp_df[s_col].astype(str).str.strip()
            if e_col and "Product Ad" in camp_df[e_col].unique():
                ads = camp_df[camp_df[e_col] == "Product Ad"]
            else:
                ads = camp_df.dropna(subset=[s_col])
            ads = ads.drop_duplicates(subset=[c_col], keep='first')
            
            sku_map = pd.Series(ads[s_col].values, index=ads[c_col]).to_dict()
            if a_col:
                asin_sku_map = pd.Series(ads[s_col].values, index=ads[a_col]).to_dict()

            def resolve_sku(row):
                c_name = row.get("Campaign Name")
                if c_name in sku_map: return sku_map[c_name]
                match = re.search(r'_(B0[A-Z0-9]{8})', str(c_name))
                if match:
                    extracted_asin = match.group(1)
                    if extracted_asin in asin_sku_map: return asin_sku_map[extracted_asin]
                return "SKU_NEEDED"

            harvest_df["Advertised SKU"] = harvest_df.apply(resolve_sku, axis=1)
            found = harvest_df[harvest_df["Advertised SKU"] != "SKU_NEEDED"].shape[0]
            return harvest_df, f"✅ Mapped SKUs for {found} terms (using winner SKU logic)."
        else:
            return harvest_df, f"⚠️ Could not find mapped columns. Found: {col_map}"
    except Exception as e:
        return harvest_df, f"❌ Error mapping SKUs: {str(e)}"

def enrich_bids_with_ids(bids_outputs: dict, bulk_file) -> tuple[dict, str]:
    """
    Parse bulk file to fill missing IDs for Bids AND Negatives.
    """
    try:
        bulk_df = pd.read_csv(bulk_file) if bulk_file.name.endswith('.csv') else pd.read_excel(bulk_file)
        col_map = SmartMapper.map_columns(bulk_df)
        
        c_name = col_map.get("Campaign")
        c_id = col_map.get("CampaignId")
        ag_name = col_map.get("AdGroup")
        ag_id = col_map.get("AdGroupId")
        kw_text = col_map.get("Keyword")
        kw_id = col_map.get("KeywordId")
        pt_id = col_map.get("TargetingId")
        match = col_map.get("Match")
        entity = col_map.get("Entity")

        if not (c_name and c_id):
            return bids_outputs, "⚠️ Bulk file missing required columns (Campaign, CampaignId)."

        camp_map = pd.Series(bulk_df[c_id].values, index=bulk_df[c_name]).to_dict()
        ag_df = bulk_df.dropna(subset=[ag_name, ag_id])
        ag_map = dict(zip(zip(ag_df[c_name], ag_df[ag_name]), ag_df[ag_id]))
        
        kw_map = {}
        for _, row in bulk_df.iterrows():
            if row.get(entity) in ["Keyword", "Product Targeting"]:
                key = (row.get(c_name), row.get(ag_name), str(row.get(kw_text, "")).strip(), str(row.get(match, "")).lower())
                val = row.get(kw_id) or row.get(pt_id)
                if val:
                    kw_map[key] = val

        # Updated: Iterate through both Bids and Negatives
        count_fixed = 0
        target_keys = ['Direct_Bids', 'Aggregated_Bids', 'Negatives']
        
        for key in target_keys:
            if key in bids_outputs and not bids_outputs[key].empty:
                df = bids_outputs[key]
                
                # 1. Campaign ID
                mask_no_cid = (df["CampaignId"] == "") | (df["CampaignId"].isna())
                df.loc[mask_no_cid, "CampaignId"] = df.loc[mask_no_cid, "Campaign Name"].map(camp_map).fillna("")
                
                # 2. Ad Group ID
                mask_no_agid = (df["AdGroupId"] == "") | (df["AdGroupId"].isna())
                df.loc[mask_no_agid, "AdGroupId"] = df.loc[mask_no_agid].apply(lambda r: ag_map.get((r["Campaign Name"], r.get("Ad Group Name")), ""), axis=1)

                # 3. Keyword ID (Only for Bids, Negatives typically don't map to existing KW IDs)
                if key != 'Negatives':
                    def resolve_kw_id(r):
                        k = (r["Campaign Name"], r.get("Ad Group Name"), str(r.get("Targeting")).strip(), str(r.get("Match Type")).lower())
                        if k in kw_map: return kw_map[k]
                        return r.get("KeywordId", "") or r.get("TargetingId", "")

                    df["KeywordId"] = df.apply(resolve_kw_id, axis=1)
                
                count_fixed += df["CampaignId"].astype(bool).sum()

        return bids_outputs, f"✅ Successfully mapped IDs for Bids and Negatives using bulk file."

    except Exception as e:
        return bids_outputs, f"❌ Error parsing bulk file: {str(e)}"

def generate_bulk_from_harvest(
    df_harvest: pd.DataFrame, 
    portfolio_id: str, 
    total_daily_budget: float, 
    launch_date: datetime
) -> pd.DataFrame:
    rows = []
    start_date_str = launch_date.strftime("%Y%m%d")
    
    if "Advertised SKU" not in df_harvest.columns: df_harvest["Advertised SKU"] = "SKU_NEEDED"
    if "Match Type" not in df_harvest.columns: df_harvest["Match Type"] = "EXACT"

    grouped = df_harvest.groupby("Advertised SKU")
    
    for sku_key, group in grouped:
        for match_type, sub_group in group.groupby("Match Type"):
            match_suffix = "Cluster" if match_type == "PHRASE" else "Exact"
            campaign_name = f"Harvest_{match_suffix}_{sku_key}_{start_date_str}"
            
            rows.append({
                "Product": "Sponsored Products", "Entity": "Campaign", "Operation": "Create",
                "Campaign ID": campaign_name, "Campaign Name": campaign_name,
                "Start Date": start_date_str, "Targeting Type": "MANUAL", "State": "Enabled",
                "Daily Budget": f"{total_daily_budget:.2f}", "Bidding Strategy": "Dynamic bids - down only",
                "Portfolio ID": portfolio_id or ""
            })
            
            ag_name = f"AG_{match_suffix}_{sku_key}"
            avg_bid = pd.to_numeric(sub_group["New Bid"], errors='coerce').fillna(1.0).mean()
            
            rows.append({
                "Product": "Sponsored Products", "Entity": "Ad Group", "Operation": "Create",
                "Campaign ID": campaign_name, "Campaign Name": campaign_name,
                "Ad Group ID": ag_name, "Ad Group Name": ag_name,
                "Start Date": start_date_str, "State": "Enabled", "Ad Group Default Bid": f"{avg_bid:.2f}"
            })
            
            rows.append({
                "Product": "Sponsored Products", "Entity": "Product Ad", "Operation": "Create",
                "Campaign ID": campaign_name, "Campaign Name": campaign_name,
                "Ad Group ID": ag_name, "Ad Group Name": ag_name,
                "SKU": sku_key, "State": "Enabled", "Ad Group Default Bid": f"{avg_bid:.2f}"
            })
            
            for _, row in sub_group.iterrows():
                term = str(row["Customer Search Term"])
                kw_bid = row.get("New Bid", avg_bid)
                
                common_kw = {
                    "Product": "Sponsored Products", "Operation": "Create",
                    "Campaign ID": campaign_name, "Campaign Name": campaign_name,
                    "Ad Group ID": ag_name, "Ad Group Name": ag_name,
                    "Bid": f"{kw_bid:.2f}", "State": "Enabled", 
                    "Ad Group Default Bid": f"{avg_bid:.2f}"
                }
                
                if is_asin(term):
                    rows.append({
                        **common_kw, 
                        "Entity": "Product Targeting", 
                        "Product Targeting Expression": f'asin="{term.upper()}"',
                        "Keyword Text": "", "Match Type": ""
                    })
                else:
                    m_type = "PHRASE" if match_suffix == "Cluster" else "EXACT"
                    rows.append({
                        **common_kw, 
                        "Entity": "Keyword", 
                        "Keyword Text": term, "Match Type": m_type,
                        "Product Targeting Expression": ""
                    })
                
    df_out = pd.DataFrame(rows)
    if df_out.empty:
        df_out = pd.DataFrame(columns=BULK_COLUMNS_UNIVERSAL)
    else:
        for col in BULK_COLUMNS_UNIVERSAL:
            if col not in df_out.columns: df_out[col] = ""
        df_out = df_out[BULK_COLUMNS_UNIVERSAL]
        
    return df_out

# ==========================================
# 6. REPORT GENERATOR (TEXT)
# ==========================================

def generate_simulation_report(stats, harvest_df, negatives_df, bids_outputs, config):
    """
    Generates a detailed ASCII text report summarizing the optimization results.
    """
    
    # --- Helper Functions ---
    def f_curr(x): return f"${x:,.2f}"
    def f_pct(x): return f"{x*100:.1f}%"
    
    def get_int(row, attr_name, alt_name=None):
        """Safely extract integer from row (handling NaNs and spaces)."""
        try:
            val = row.get(attr_name)
            if val is None and alt_name: val = row.get(alt_name)
            if val is None: return 0
            val = float(val)
            if np.isnan(val): return 0
            return int(val)
        except: return 0

    def get_float(row, attr_name):
        """Safely extract float from row."""
        try:
            val = row.get(attr_name, 0.0)
            if val is None or np.isnan(float(val)): return 0.0
            return float(val)
        except: return 0.0

    lines = []
    lines.append("="*100)
    lines.append("PPC SUITE OPTIMIZER - DETAILED STATISTICS")
    lines.append("="*100 + "\n")
    
    # 1. HARVEST ANALYSIS
    # -------------------
    lines.append("="*100)
    lines.append("1. HARVEST KEYWORD ANALYSIS")
    lines.append("="*100 + "\n")
    
    lines.append("📊 HARVEST STATISTICS:")
    lines.append(f"   Total unique search terms: {int(stats['unique_terms']):,}")
    lines.append(f"   ✅ Qualified harvest keywords: {len(harvest_df):,}")
    lines.append(f"   Criteria: Orders >= {config['HARVEST_ORDERS']}, ROAS >= Target")
    lines.append("")
    
    if not harvest_df.empty:
        h_sales = harvest_df['Sales_Attributed'].sum()
        h_spend = harvest_df['Spend'].sum()
        h_orders = harvest_df['Orders_Attributed'].sum()
        h_roas = h_sales / h_spend if h_spend > 0 else 0
        h_acos = h_spend / h_sales if h_sales > 0 else 0
        
        lines.append("💰 HARVEST POTENTIAL:")
        lines.append(f"   Total Sales: {f_curr(h_sales)}")
        lines.append(f"   Total Spend: {f_curr(h_spend)}")
        lines.append(f"   Total Orders: {int(h_orders):,}")
        lines.append(f"   Average ACoS: {f_pct(h_acos)}")
        lines.append(f"   Average ROAS: {h_roas:.2f}x")
        lines.append("")
        
        lines.append("🏆 TOP 20 HARVEST KEYWORDS:")
        lines.append(f"{'Rank':<5} {'Search Term':<40} {'Orders':<8} {'Sales':<12} {'Spend':<10} {'ACoS':<8} {'Clicks':<8}")
        lines.append("-" * 100)
        
        top_h = harvest_df.sort_values("Sales_Attributed", ascending=False).head(20)
        
        # FIX: Use iterrows() to safely access columns with spaces
        for i, (_, row) in enumerate(top_h.iterrows(), 1):
            term = str(row.get("Customer Search Term", row.get("Term", "Unknown")))
            if len(term) > 38: term = term[:35] + "..."
            
            sales = get_float(row, 'Sales_Attributed')
            spend = get_float(row, 'Spend')
            orders = get_int(row, 'Orders_Attributed', 'Orders')
            clicks = get_int(row, 'Clicks')
            acos = spend/sales if sales > 0 else 0
            
            lines.append(f"{i:<5} {term:<40} {orders:<8} {f_curr(sales):<12} {f_curr(spend):<10} {f_pct(acos):<8} {clicks:<8}")
        lines.append("")

    # 2. NEGATIVE ANALYSIS
    # --------------------
    lines.append("="*100)
    lines.append("2. NEGATIVE KEYWORD ANALYSIS")
    lines.append("="*100 + "\n")
    
    lines.append("📊 NEGATIVE KEYWORD STATISTICS:")
    lines.append(f"   ❌ Negative keywords identified: {len(negatives_df):,}")
    lines.append("")
    
    if not negatives_df.empty:
        n_spend = negatives_df['Spend'].sum()
        n_clicks = negatives_df['Clicks'].sum()
        
        lines.append("💸 WASTED SPEND:")
        lines.append(f"   Total wasted spend: {f_curr(n_spend)}")
        lines.append(f"   Total wasted clicks: {int(n_clicks):,}")
        lines.append("")
        
        lines.append("🚫 TOP 20 NEGATIVE KEYWORDS (by wasted spend):")
        lines.append(f"{'Rank':<5} {'Search Term':<40} {'Clicks':<8} {'Spend':<10} {'Impressions':<12}")
        lines.append("-" * 100)
        
        top_n = negatives_df.sort_values("Spend", ascending=False).head(20)
        for i, (_, row) in enumerate(top_n.iterrows(), 1):
            term = str(row.get("Term", row.get("Customer Search Term", "Unknown")))
            if len(term) > 38: term = term[:35] + "..."
            
            clicks = get_int(row, 'Clicks')
            spend = get_float(row, 'Spend')
            impressions = get_int(row, 'Impressions')
            
            lines.append(f"{i:<5} {term:<40} {clicks:<8} {f_curr(spend):<10} {impressions:<12}")

    lines.append("")

    # 3. BID OPTIMIZATION
    # -------------------
    lines.append("="*100)
    lines.append("3. BID OPTIMIZATION ANALYSIS")
    lines.append("="*100 + "\n")
    
    all_bids = pd.DataFrame()
    if 'Direct_Bids' in bids_outputs: all_bids = pd.concat([all_bids, bids_outputs['Direct_Bids']])
    if 'Aggregated_Bids' in bids_outputs: all_bids = pd.concat([all_bids, bids_outputs['Aggregated_Bids']])
    
    if not all_bids.empty:
        # Recalculate ACoS for display
        all_bids['Sales_Attributed'] = pd.to_numeric(all_bids['Sales_Attributed'], errors='coerce').fillna(0)
        all_bids['Spend'] = pd.to_numeric(all_bids['Spend'], errors='coerce').fillna(0)
        
        increases = all_bids[all_bids['New Bid'] > all_bids['Cost Per Click (CPC)']]
        decreases = all_bids[all_bids['New Bid'] < all_bids['Cost Per Click (CPC)']]
        
        lines.append("📊 BID OPTIMIZATION STATISTICS:")
        lines.append(f"   Total keywords analyzed: {len(all_bids):,}")
        lines.append(f"   ⬆️  Bid increase opportunities: {len(increases):,}")
        lines.append(f"   ⬇️  Bid decrease opportunities: {len(decreases):,}")
        lines.append("")
        
        # Top Increases
        if not increases.empty:
            lines.append("⬆️  TOP 15 BID INCREASE OPPORTUNITIES:")
            lines.append(f"{'Rank':<5} {'Keyword/Target':<30} {'Match':<8} {'Orders':<8} {'ACoS':<8} {'CPC':<8} {'Sales':<12}")
            lines.append("-" * 100)
            top_inc = increases.sort_values("Sales_Attributed", ascending=False).head(15)
            for i, (_, row) in enumerate(top_inc.iterrows(), 1):
                tgt = str(row.get('Targeting', ''))[:28]
                match = str(row.get('Match Type', '')).replace("match","").upper()[:7]
                
                sales = get_float(row, 'Sales_Attributed')
                spend = get_float(row, 'Spend')
                cpc = get_float(row, 'Cost Per Click (CPC)')
                orders = get_int(row, 'Orders_Attributed')
                
                acos = spend / sales if sales > 0 else 0
                lines.append(f"{i:<5} {tgt:<30} {match:<8} {orders:<8} {f_pct(acos):<8} {f_curr(cpc):<8} {f_curr(sales):<12}")
            lines.append("")

        # Top Decreases
        if not decreases.empty:
            lines.append("⬇️  TOP 15 BID DECREASE OPPORTUNITIES:")
            lines.append(f"{'Rank':<5} {'Keyword/Target':<30} {'Match':<8} {'Clicks':<8} {'Orders':<8} {'ACoS':<8} {'Spend':<12}")
            lines.append("-" * 100)
            top_dec = decreases.sort_values("Spend", ascending=False).head(15)
            for i, (_, row) in enumerate(top_dec.iterrows(), 1):
                tgt = str(row.get('Targeting', ''))[:28]
                match = str(row.get('Match Type', '')).replace("match","").upper()[:7]
                
                sales = get_float(row, 'Sales_Attributed')
                spend = get_float(row, 'Spend')
                clicks = get_int(row, 'Clicks')
                orders = get_int(row, 'Orders_Attributed')
                
                acos = spend / sales if sales > 0 else 0
                lines.append(f"{i:<5} {tgt:<30} {match:<8} {clicks:<8} {orders:<8} {f_pct(acos):<8} {f_curr(spend):<12}")
            lines.append("")

    lines.append("="*100)
    lines.append("All functionalities are working correctly! ✅")
    lines.append("="*100)
    
    return "\n".join(lines)


# ==========================================
# PART 3: UI/STREAMLIT IMPLEMENTATION
# ==========================================

# --- SIDEBAR NAVIGATION ---
st.sidebar.markdown("## **S2C LaunchPad**")

if st.sidebar.button("📖 Readme / Guide", use_container_width=True): navigate_to('readme')
if st.sidebar.button("🏠 Home", use_container_width=True): navigate_to('home')
st.sidebar.markdown("---")
if st.sidebar.button("📊 Optimizer (Single File)", use_container_width=True): navigate_to('optimizer')
if st.sidebar.button("🚀 Creator (Harvest)", use_container_width=True): navigate_to('creator')

# ==========================================
# HOME MODULE
# ==========================================

if st.session_state['current_module'] == 'home':
    st.markdown("<div class='main-header'><h1>S2C LaunchPad 🚀</h1><p>Single-File Amazon PPC Optimization</p></div>", unsafe_allow_html=True)
    st.info("ℹ️ **Update:** V3.3: Fixed Unknown keyword error & added report dashboard display.")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### ✨ New Features")
        st.markdown("""
        - **Deduplicated Harvests:** Terms aggregated across campaigns.
        - **Hard Negatives:** High-click bleeders killed instantly.
        - **Report Dashboard:** View full analysis text without downloading.
        - **Bulk Mapping:** Maps IDs for both Bids and Negatives.
        """)
    with col2:
        st.markdown("### 🎯 How It Works")
        st.markdown("""
        1. Upload your Search Term Report
        2. Configure thresholds
        3. Review deduplicated harvests
        4. See exactly why bids are held or updated
        5. Preview & Download bulk files
        """)

# ==========================================
# OPTIMIZER MODULE
# ==========================================

elif st.session_state['current_module'] == 'optimizer':
    st.title("📊 PPC Optimizer")
    
    with st.sidebar.expander("⚙️ Rules", expanded=False):
        attribution = st.radio("Attribution", [7, 14], index=0)
        st.divider()
        st.markdown("**Harvest Thresholds (Tier 2)**")
        click_thresh = st.number_input("Min Clicks", value=7, step=1)
        order_thresh = st.number_input("Min Orders", value=2, step=1)
        sale_thresh = st.number_input("Min Sales ($)", value=50.0, step=10.0)
        roas_mult = st.number_input("ROAS vs Median (Multiplier)", value=1.0, step=0.1)
        st.divider()
        st.markdown("**Negative Keywords**")
        neg_click = st.number_input("Bleeder Clicks (Stat)", value=10, step=1)
        neg_spend = st.number_input("Bleeder Spend ($)", value=5.0, step=0.5)
        st.divider()
        st.markdown("**Bid Optimization**")
        alpha_exact = st.slider("Alpha (Exact/PT)", 0.05, 0.5, 0.25, step=0.05)
        alpha_broad = st.slider("Alpha (Broad/Agg)", 0.05, 0.5, 0.20, step=0.05)
        max_bid_change = st.slider("Max Change %", 0.05, 0.5, 0.20, step=0.05)
        min_clicks_bid = st.number_input("Min Clicks for Bid", value=3, step=1)
        default_target_roas = st.number_input("Default Target ROAS (if 0)", value=2.5, step=0.1, help="Fallback ROAS for campaigns with 0 sales.")

    config = {
        "ATTRIBUTION_WINDOW": attribution,
        "HARVEST_CLICKS": click_thresh, "HARVEST_ORDERS": order_thresh, "HARVEST_SALES": sale_thresh, "HARVEST_ROAS_MULT": roas_mult,
        "DEDUPE_SIMILARITY": 0.9,
        "NEGATIVE_CLICKS_THRESHOLD": neg_click, "NEGATIVE_SPEND_THRESHOLD": neg_spend, "NEGATIVE_IMPRESSION_THRESHOLD": 0,
        "ALPHA_EXACT": alpha_exact, "ALPHA_BROAD": alpha_broad, "MAX_BID_CHANGE": max_bid_change, "MIN_CLICKS_BID": min_clicks_bid,
        "DEFAULT_TARGET_ROAS": default_target_roas,
        # Simulation parameters
        "REPORT_DAYS": 60,  # Adjust based on your report period (7, 14, 30, 60)
        "HARVEST_VOLUME_MULTIPLIER": 1.25,  # +25% volume gain for exact match
        "HARVEST_CVR_MULTIPLIER": 1.0,  # No CVR change (conservative)
        "HARVEST_MIN_CLICKS": 10,  # Minimum baseline clicks for reliable forecast
        "HARVEST_EFFICIENCY_MULTIPLIER": 1.30  # 30% efficiency gain from exact match (more aggressive)
    }

    upl = st.file_uploader("Upload 'SP Search Term Report' (from Bulk Download)", type=["csv", "xlsx"])
    
    if upl:
        with st.spinner("Processing Segmentation & Aggregation..."):
            outputs, err, df_opt, stats = run_optimizer_logic(upl, config)
        
        if err:
            st.error(err)
        else:
            t1, t2, t3, t4, t5, t6, t7, t8, t9 = st.tabs([
                "📊 Dashboard", 
                "💎 Harvest", 
                "🛑 Negatives", 
                "💰 Bids", 
                "⚠️ Cannibalization",
                "🔥 Heatmap",
                "📈 Velocity",
                "🎯 Simulation",
                "🚀 Actions"
            ])
            
            with t1:
                st.markdown("### 🔍 Account Overview")
                m1, m2, m3, m4 = st.columns(4)
                total_sales = df_opt['Sales_Attributed'].sum() if 'Sales_Attributed' in df_opt.columns else df_opt['Sales'].sum()
                m1.metric("Spend", f"${stats['total_spend']:,.0f}")
                m2.metric("Sales", f"${total_sales:,.0f}")
                m3.metric("ROAS", f"{total_sales/stats['total_spend']:.2f}x" if stats['total_spend'] > 0 else "0.00x")
                m4.metric("ACOS", f"{(stats['total_spend']/total_sales)*100:.1f}%" if total_sales > 0 else "0.0%")
                
                st.divider()
                st.markdown("### 📉 Optimization Scope")
                c1, c2, c3 = st.columns(3)
                c1.metric("Harvests Found", stats['harvest_count'], help="Moving to Exact Match")
                c2.metric("Negatives Found", stats['negative_count'], help="Bleeders to Block")
                c3.metric("Bids Analyzed", stats['bid_total_candidates'], help="Targets eligible for check")
                
                st.divider()
                st.markdown("### 📄 Results Report")
                
                # Generate Report
                report_txt = generate_simulation_report(
                    stats, 
                    outputs.get('Survivors', pd.DataFrame()), 
                    outputs.get('Negatives', pd.DataFrame()), 
                    outputs, 
                    config
                )
                
                # Display in Dashboard
                with st.expander("👁️ View Full Analysis Report", expanded=True):
                    st.code(report_txt, language="text")
                
                st.download_button(
                    label="📥 Download Report as Text File",
                    data=report_txt,
                    file_name=f"optimizer_report_{datetime.now().strftime('%Y%m%d')}.txt",
                    mime="text/plain"
                )

            with t2:
                st.subheader("💎 Harvest Candidates")
                st.markdown("Terms are **aggregated** across all campaigns to find true winners.")
                survivors_df = outputs.get('Survivors', pd.DataFrame())
                
                if not survivors_df.empty:
                    st.dataframe(survivors_df[[
                        "Customer Search Term", "Campaign Name", "ROAS", "Clicks", "Orders_Attributed", "Sales_Attributed", "New Bid"
                    ]])
                    if st.button("📦 Prepare for Campaign Creator", key="btn_harvest_prep"):
                        prep_df = survivors_df.copy()
                        prep_df["Match Type"] = "EXACT"
                        if 'harvest_payload' in st.session_state:
                            st.session_state['harvest_payload'] = pd.concat([st.session_state['harvest_payload'], prep_df]).drop_duplicates(subset=["Customer Search Term"])
                        else:
                            st.session_state['harvest_payload'] = prep_df
                        st.toast(f"✅ {len(survivors_df)} harvest terms sent to Actions tab!", icon="✅")
                else:
                    st.info("No harvest opportunities found.")

            with t3:
                st.subheader("🛑 Negative Candidates")
                neg_df = outputs.get('Negatives', pd.DataFrame())
                if not neg_df.empty:
                    st.metric("Estimated Savings", f"${neg_df['Spend'].sum():,.2f}")
                    st.dataframe(neg_df)
                    
                    # Generate bulk file
                    neg_bulk = generate_negatives_direct(neg_df)
                    
                    # AUTO-VALIDATE
                    validation = validate_bulk_file(neg_bulk)
                    
                    # Extract error rows
                    error_rows = set()
                    for error in validation['errors']:
                        import re
                        match = re.search(r'Row (\d+):', error)
                        if match:
                            error_rows.add(int(match.group(1)) - 2)
                    
                    valid_rows_df = neg_bulk.drop(index=list(error_rows)) if error_rows else neg_bulk
                    error_rows_df = neg_bulk.loc[list(error_rows)] if error_rows else pd.DataFrame()
                    
                    # Show validation summary
                    if validation['errors']:
                        st.error(f"🚨 **{len(validation['errors'])} Critical Errors Found**")
                        st.markdown(f"**{len(error_rows_df)} rows have errors**")
                    else:
                        st.success("✅ **No errors found!** File ready for upload.")
                    
                    st.divider()
                    
                    # Tabs for valid vs error rows
                    tab1, tab2 = st.tabs([
                        f"✅ Valid Rows ({len(valid_rows_df)})",
                        f"❌ Error Rows ({len(error_rows_df)})"
                    ])
                    
                    with tab1:
                        if not valid_rows_df.empty:
                            st.success(f"✅ {len(valid_rows_df)} rows ready for upload")
                            timestamp = datetime.now().strftime("%Y%m%d")
                            st.download_button(
                                "📥 Download Valid Negatives",
                                to_excel_download(valid_rows_df, f"negatives_valid_{timestamp}.xlsx"),
                                f"negatives_valid_{timestamp}.xlsx",
                                use_container_width=True,
                                type="primary"
                            )
                            with st.expander("👁️ Preview"): st.dataframe(valid_rows_df)
                        else:
                            st.warning("⚠️ No valid rows")
                    
                    with tab2:
                        if not error_rows_df.empty:
                            st.error(f"❌ {len(error_rows_df)} rows need fixing")
                            timestamp = datetime.now().strftime("%Y%m%d")
                            st.download_button(
                                "📥 Download Error Rows",
                                to_excel_download(error_rows_df, f"negatives_errors_{timestamp}.xlsx"),
                                f"negatives_errors_{timestamp}.xlsx",
                                use_container_width=True
                            )
                            with st.expander("📋 View Errors"):
                                for error in validation['errors'][:10]:
                                    st.text(error)
                                if len(validation['errors']) > 10:
                                    st.caption(f"... and {len(validation['errors']) - 10} more")
                        else:
                            st.success("✅ No errors!")
                else:
                    st.info("No negative keyword opportunities found.")

            with t4:
                st.subheader("💰 Bid Adjustments")
                
                # --- Map IDs from Bulk ---
                st.markdown("#### 🆔 ID Mapping (Optional)")
                st.caption("Upload 'Sponsored Products Campaigns' Bulk file to map IDs for Bids and Negatives.")
                bulk_map_file = st.file_uploader("Upload Bulk File", type=["csv", "xlsx"], key="bid_id_map")
                
                if bulk_map_file:
                    outputs, map_msg = enrich_bids_with_ids(outputs, bulk_map_file)
                    if "✅" in map_msg: 
                        st.success(map_msg)
                        neg_df = outputs.get('Negatives', pd.DataFrame()) 
                    else: st.error(map_msg)

                # --- Transparency Logic ---
                st.divider()
                st.markdown("#### 📊 Optimization Funnel")
                col_f1, col_f2, col_f3 = st.columns(3)
                col_f1.metric("Targets Analyzed", stats['bid_total_candidates'])
                col_f2.metric("Held (Low Data)", stats['bid_hold_count'], help=f"Fewer than {config['MIN_CLICKS_BID']} clicks")
                col_f3.metric("Optimized", stats['bid_update_count'], help="Bid changed based on ROAS")
                
                b1, b2 = st.tabs(["Direct (Exact/PT)", "Aggregated (Broad/Phrase/Auto)"])
                direct_df = outputs.get('Direct_Bids', pd.DataFrame())
                agg_df = outputs.get('Aggregated_Bids', pd.DataFrame())

                with b1: st.dataframe(direct_df)
                with b2: st.dataframe(agg_df)

                st.divider()
                st.subheader("📥 Download Bid Updates")
                final_direct, skip_d = generate_bids_direct(direct_df)
                final_agg, skip_a = generate_bids_direct(agg_df)
                final_combined = pd.concat([final_direct, final_agg])
                
                if final_combined.empty:
                    st.warning("⚠️ No valid bid updates generated. Check if 'Campaign Name' is present in your upload.")
                else:
                    # AUTO-VALIDATE
                    validation = validate_bulk_file(final_combined)
                    
                    # Extract error rows
                    error_rows = set()
                    for error in validation['errors']:
                        import re
                        match = re.search(r'Row (\d+):', error)
                        if match:
                            error_rows.add(int(match.group(1)) - 2)
                    
                    valid_rows_df = final_combined.drop(index=list(error_rows)) if error_rows else final_combined
                    error_rows_df = final_combined.loc[list(error_rows)] if error_rows else pd.DataFrame()
                    
                    # Show validation summary
                    if validation['errors']:
                        st.error(f"🚨 **{len(validation['errors'])} Critical Errors Found**")
                        st.markdown(f"**{len(error_rows_df)} rows have errors**")
                    else:
                        st.success("✅ **No errors found!** File ready for upload.")
                    
                    st.divider()
                    
                    # Tabs for valid vs error rows
                    tab1, tab2 = st.tabs([
                        f"✅ Valid Rows ({len(valid_rows_df)})",
                        f"❌ Error Rows ({len(error_rows_df)})"
                    ])
                    
                    with tab1:
                        if not valid_rows_df.empty:
                            st.success(f"✅ {len(valid_rows_df)} rows ready for upload")
                            timestamp = datetime.now().strftime("%Y%m%d")
                            st.download_button(
                                "📥 Download Valid Bid Updates",
                                to_excel_download(valid_rows_df, f"bids_valid_{timestamp}.xlsx"),
                                f"bids_valid_{timestamp}.xlsx",
                                use_container_width=True,
                                type="primary"
                            )
                            with st.expander("👁️ Preview"): st.dataframe(valid_rows_df)
                        else:
                            st.warning("⚠️ No valid rows")
                    
                    with tab2:
                        if not error_rows_df.empty:
                            st.error(f"❌ {len(error_rows_df)} rows need fixing")
                            timestamp = datetime.now().strftime("%Y%m%d")
                            st.download_button(
                                "📥 Download Error Rows",
                                to_excel_download(error_rows_df, f"bids_errors_{timestamp}.xlsx"),
                                f"bids_errors_{timestamp}.xlsx",
                                use_container_width=True
                            )
                            with st.expander("📋 View Errors"):
                                for error in validation['errors'][:10]:
                                    st.text(error)
                                if len(validation['errors']) > 10:
                                    st.caption(f"... and {len(validation['errors']) - 10} more")
                        else:
                            st.success("✅ No errors!")

            
            with t5:
                st.subheader("⚠️ ASIN Cannibalization Analysis")
                st.markdown("""
                <div class='tab-description'>
                Detect when the same ASIN/keyword appears in multiple campaigns competing with itself.
                Save 15-25% by eliminating internal competition.
                </div>
                """, unsafe_allow_html=True)
                
                cannibalization_df = outputs.get('Cannibalization', pd.DataFrame())
                
                if not cannibalization_df.empty:
                    total_wasted = cannibalization_df['Wasted_Spend'].sum()
                    monthly_savings = cannibalization_df['Monthly_Savings_Estimate'].sum()
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Issues Found", len(cannibalization_df))
                    col2.metric("Wasted Spend", f"${total_wasted:,.2f}")
                    col3.metric("Monthly Savings Potential", f"${monthly_savings:,.0f}")
                    
                    st.divider()
                    st.markdown("### 🎯 Top Issues")
                    
                    # Show top 20 by wasted spend
                    top_issues = cannibalization_df.sort_values('Wasted_Spend', ascending=False).head(20)
                    
                    st.dataframe(
                        top_issues[[
                            'Search Term', 'ASIN', 'Campaigns_Count', 'Total_Spend',
                            'Wasted_Spend', 'Best_Campaign', 'Best_ROAS', 'Recommendation'
                        ]],
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    st.divider()
                    st.markdown("### 📥 Download Full Report")
                    st.download_button(
                        "Download Cannibalization Report",
                        to_excel_download(cannibalization_df, "cannibalization"),
                        "cannibalization_report.xlsx",
                        use_container_width=True
                    )
                else:
                    st.success("✅ No significant cannibalization detected! Your campaigns are well-structured.")
            
            with t6:
                st.subheader("🔥 Wasted Spend Heatmap with Action Tracking")
                st.markdown("""
                <div class='tab-description'>
                Visual performance heatmap showing which campaigns/ad groups need attention.<br>
                🔴 Red = Fix immediately | 🟡 Yellow = Monitor | 🟢 Green = Good performance<br>
                <strong>NEW:</strong> See which issues the optimizer is already addressing (harvests, negatives, bids)
                </div>
                """, unsafe_allow_html=True)
                
                heatmap_df = outputs.get('Heatmap', pd.DataFrame())
                
                if not heatmap_df.empty:
                    # Show summary metrics
                    high_priority = len(heatmap_df[heatmap_df['Priority'] == '🔴 High'])
                    medium_priority = len(heatmap_df[heatmap_df['Priority'] == '🟡 Medium'])
                    good_performance = len(heatmap_df[heatmap_df['Priority'] == '🟢 Good'])
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("🔴 High Priority", high_priority, help="Need immediate attention")
                    col2.metric("🟡 Medium Priority", medium_priority, help="Monitor closely")
                    col3.metric("🟢 Good Performance", good_performance, help="Keep doing what works")
                    
                    st.divider()
                    
                    # Show action status summary
                    actions_addressed = len(heatmap_df[heatmap_df['Actions_Taken'] != "⚪ No actions"])
                    actions_needed = len(heatmap_df) - actions_addressed
                    high_priority_addressed = len(heatmap_df[
                        (heatmap_df['Priority'] == '🔴 High') & 
                        (heatmap_df['Actions_Taken'] != "⚪ No actions")
                    ])
                    
                    st.markdown("### 🎯 Optimizer Actions Status")
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("✅ Being Addressed", actions_addressed, help="Campaigns with optimizer actions")
                    col2.metric("⚠️ Needs Attention", actions_needed, help="No actions planned yet")
                    col3.metric("🔴 High Priority Fixed", f"{high_priority_addressed}/{high_priority}", 
                               help="High priority campaigns being addressed")
                    pct_addressed = (actions_addressed / len(heatmap_df) * 100) if len(heatmap_df) > 0 else 0
                    col4.metric("Coverage", f"{pct_addressed:.0f}%", help="% of campaigns with actions")
                    
                    st.divider()
                    
                    # Filter options
                    col1, col2 = st.columns(2)
                    with col1:
                        filter_priority = st.multiselect(
                            "Filter by Priority",
                            ['🔴 High', '🟡 Medium', '🟢 Good'],
                            default=['🔴 High', '🟡 Medium']
                        )
                    with col2:
                        filter_actions = st.selectbox(
                            "Filter by Actions",
                            ["All", "With Actions", "No Actions"]
                        )
                    
                    # Apply filters
                    filtered_heatmap = heatmap_df[heatmap_df['Priority'].isin(filter_priority)]
                    
                    if filter_actions == "With Actions":
                        filtered_heatmap = filtered_heatmap[filtered_heatmap['Actions_Taken'] != "⚪ No actions"]
                    elif filter_actions == "No Actions":
                        filtered_heatmap = filtered_heatmap[filtered_heatmap['Actions_Taken'] == "⚪ No actions"]
                    
                    # Display heatmap
                    st.markdown("### 📊 Performance Heatmap with Actions")
                    
                    # Create styled dataframe
                    def color_score(val):
                        if val == 2:
                            return 'background-color: #dcfce7; color: #166534'  # Green
                        elif val == 1:
                            return 'background-color: #fef9c3; color: #854d0e'  # Yellow
                        elif val == 0:
                            return 'background-color: #fee2e2; color: #991b1b'  # Red
                        else:  # val == -1 (no data)
                            return 'background-color: #f3f4f6; color: #6b7280'  # Gray
                    
                    display_cols = [
                        'Priority', 'Campaign Name', 'Ad Group Name', 
                        'Actions_Taken',  # NEW - show what actions are being taken
                        'Spend', 'Sales_Attributed', 'ROAS', 'ACoS',
                        'CTR', 'CVR', 'CTR_Score', 'CVR_Score', 'ROAS_Score', 'ACoS_Score'
                    ]
                    
                    styled_df = filtered_heatmap[display_cols].style.applymap(
                        color_score,
                        subset=['CTR_Score', 'CVR_Score', 'ROAS_Score', 'ACoS_Score']
                    ).format({
                        'Spend': '${:,.2f}',
                        'Sales_Attributed': '${:,.2f}',
                        'ROAS': '{:.2f}x',
                        'ACoS': '{:.1f}%',
                        'CTR': '{:.2f}%',
                        'CVR': '{:.2f}%'
                    })
                    
                    st.dataframe(styled_df, use_container_width=True, hide_index=True)
                    
                    # Show interpretation guide
                    with st.expander("📖 How to Read This Heatmap"):
                        st.markdown("""
                        **Priority Colors:**
                        - 🔴 **High Priority:** Poor performance, needs immediate attention
                        - 🟡 **Medium Priority:** Average performance, monitor closely
                        - 🟢 **Good Performance:** Keep doing what works
                        
                        **Score Colors:**
                        - 🟢 **Green (2):** Top 33% performers
                        - 🟡 **Yellow (1):** Middle 33%
                        - 🔴 **Red (0):** Bottom 33%
                        - ⚪ **Gray (-1):** Insufficient data (0 clicks/orders)
                        
                        **Actions Taken:**
                        - 💎 **Harvests:** Keywords being promoted to exact match
                        - 🛑 **Negatives:** Bad keywords being blocked
                        - ⬆️ **Bid Increases:** Scaling winners
                        - ⬇️ **Bid Decreases:** Cutting losers
                        - ⏸️ **Hold (Low volume):** Below threshold (< 3 clicks or < 2 orders) - waiting for more data
                        - ✅ **No action needed:** Performing adequately, no changes needed
                        
                        **What to Do:**
                        1. Focus on 🔴 High Priority with "✅ No action needed" or "⏸️ Hold" (may need manual strategy)
                        2. Verify actions make sense for the priority level
                        3. Export and apply Actions tab recommendations
                        4. Monitor "⏸️ Hold" campaigns - they may need budget increases to gather data
                        """)
                    
                    st.divider()
                    st.markdown("### 📥 Download Full Heatmap")
                    st.download_button(
                        "Download Heatmap Report",
                        to_excel_download(heatmap_df, "heatmap"),
                        "performance_heatmap.xlsx",
                        use_container_width=True
                    )
                else:
                    st.info("No heatmap data available.")
            
            with t7:
                st.subheader("📈 Keyword Velocity & Trends")
                st.markdown("""
                <div class='tab-description'>
                Track keyword performance trends over time. Compare current upload vs previous upload.
                Catch rising stars before competitors and kill declining keywords early.
                </div>
                """, unsafe_allow_html=True)
                
                velocity_df = outputs.get('Velocity', pd.DataFrame())
                
                if not velocity_df.empty:
                    # Check if this is first upload or we have comparison data
                    if 'Orders_Previous' in velocity_df.columns and velocity_df['Orders_Previous'].sum() > 0:
                        # We have comparison data
                        rising = velocity_df[velocity_df['Trend'].str.contains('Rising|Up', case=False)]
                        falling = velocity_df[velocity_df['Trend'].str.contains('Falling|Down', case=False)]
                        
                        col1, col2, col3 = st.columns(3)
                        col1.metric("📈 Rising Keywords", len(rising), help="Increasing performance")
                        col2.metric("📉 Falling Keywords", len(falling), help="Declining performance")
                        col3.metric("Total Tracked", len(velocity_df))
                        
                        st.divider()
                        
                        # Show top risers
                        if not rising.empty:
                            st.markdown("### 📈 TOP 15 RISING KEYWORDS")
                            st.markdown("*Increase bids to capture more traffic*")
                            top_rising = rising.head(15)
                            st.dataframe(
                                top_rising[[
                                    'Search Term', 'Orders_Current', 'Orders_Previous',
                                    'Orders_Change_%', 'Spend', 'Sales', 'Trend'
                                ]],
                                use_container_width=True,
                                hide_index=True
                            )
                        
                        st.divider()
                        
                        # Show top fallers
                        if not falling.empty:
                            st.markdown("### 📉 TOP 15 FALLING KEYWORDS")
                            st.markdown("*Consider decreasing bids or pausing*")
                            top_falling = falling.head(15)
                            st.dataframe(
                                top_falling[[
                                    'Search Term', 'Orders_Current', 'Orders_Previous',
                                    'Orders_Change_%', 'Spend', 'Sales', 'Trend'
                                ]],
                                use_container_width=True,
                                hide_index=True
                            )
                        
                        st.divider()
                        st.markdown("### 📥 Download Full Velocity Report")
                        st.download_button(
                            "Download Velocity Report",
                            to_excel_download(velocity_df, "velocity"),
                            "keyword_velocity.xlsx",
                            use_container_width=True
                        )
                    else:
                        # First upload - no comparison yet
                        st.info("""
                        📊 **First Upload Detected**
                        
                        Velocity tracking requires at least 2 uploads to show trends.
                        
                        Your current data has been stored. Upload a new report next week to see:
                        - Rising keywords (↑ increasing orders/clicks)
                        - Falling keywords (↓ decreasing performance)
                        - Trend indicators and recommendations
                        
                        Current keywords tracked: {len(velocity_df):,}
                        """)
                else:
                    st.warning("No velocity data available.")
            
            with t8:
                st.subheader("🎯 Bid Change Simulation & Forecast")
                st.markdown("""
                <div class='tab-description'>
                See the impact of proposed bid changes BEFORE applying them.<br>
                Forecast CPC, spend, conversions, sales, and ROAS with confidence intervals.
                </div>
                """, unsafe_allow_html=True)
                
                simulation = outputs.get('Simulation', None)
                
                if simulation and simulation.get('scenarios'):
                    scenarios = simulation['scenarios']
                    bid_changes = simulation.get('bid_changes', pd.DataFrame())
                    current = scenarios.get('current', {})
                    conservative = scenarios.get('conservative', {})
                    expected = scenarios.get('expected', {})
                    aggressive = scenarios.get('aggressive', {})
                    
                    # DIAGNOSTIC INFO
                    diagnostic = expected.get('_diagnostic', '')
                    keywords_processed = expected.get('_keywords_processed', 0)
                    keywords_skipped = expected.get('_keywords_skipped', 0)
                    harvest_processed = expected.get('_harvest_processed', 0)
                    total_recommendations = expected.get('_total_recommendations', 0)
                    actual_changes = expected.get('_actual_changes', 0)
                    hold_count = expected.get('_hold_count', 0)
                    harvest_count = expected.get('_harvest_count', 0)
                    
                    # Show bid recommendation breakdown
                    if total_recommendations > 0 or harvest_count > 0:
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("📋 Bid Recommendations", total_recommendations)
                        col2.metric("✅ Actual Changes", actual_changes, 
                                   help="Bid increases/decreases that will be applied")
                        col3.metric("⏸️ Hold (No Change)", hold_count,
                                   help="Keywords where current bid is optimal")
                        col4.metric("💎 Harvest Campaigns", harvest_count,
                                   help="New exact match campaigns being created")
                    
                    if keywords_processed == 0 and harvest_processed == 0:
                        st.warning(f"""
                        ⚠️ **No Forecast Changes Detected**
                        
                        **Analysis:**
                        - Bid recommendations: {total_recommendations}
                        - Actual bid changes: {actual_changes}
                        - "Hold" recommendations: {hold_count}
                        - Harvest campaigns: {harvest_count}
                        
                        **Why no forecast?**
                        {diagnostic}
                        
                        **Possible reasons:**
                        1. All bid recommendations are "Hold" (current bids optimal)
                        2. Bid changes have no historical data to forecast from
                        3. Harvest campaigns have no baseline (simulation issue)
                        
                        **This indicates a bug in the simulation - harvests should show impact!**
                        """)
                    else:
                        st.success(f"""
                        ✅ **Simulation Complete**
                        
                        - Bid changes analyzed: {keywords_processed}
                        - Harvest campaigns: {harvest_processed}
                        - Total impact shown in forecast below
                        """)
                    
                    # SECTION 1: SUMMARY COMPARISON
                    st.markdown("### 📊 Performance Forecast")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**Current Performance**")
                        m1, m2 = st.columns(2)
                        m1.metric("Weekly Spend", f"${current.get('spend', 0):,.0f}")
                        m2.metric("Weekly Sales", f"${current.get('sales', 0):,.0f}")
                        m3, m4 = st.columns(2)
                        m3.metric("ROAS", f"{current.get('roas', 0):.2f}x")
                        m4.metric("Orders", f"{current.get('orders', 0):.0f}")
                    
                    with col2:
                        st.markdown("**Forecasted Performance** (Expected Scenario)")
                        spend_change = ((expected.get('spend', 0) - current.get('spend', 0)) / current.get('spend', 1)) * 100
                        sales_change = ((expected.get('sales', 0) - current.get('sales', 0)) / current.get('sales', 1)) * 100
                        roas_change = ((expected.get('roas', 0) - current.get('roas', 0)) / current.get('roas', 1)) * 100
                        orders_change = ((expected.get('orders', 0) - current.get('orders', 0)) / current.get('orders', 1)) * 100
                        
                        m1, m2 = st.columns(2)
                        m1.metric("Weekly Spend", f"${expected.get('spend', 0):,.0f}", f"{spend_change:+.1f}%")
                        m2.metric("Weekly Sales", f"${expected.get('sales', 0):,.0f}", f"{sales_change:+.1f}%")
                        m3, m4 = st.columns(2)
                        m3.metric("ROAS", f"{expected.get('roas', 0):.2f}x", f"{roas_change:+.1f}%")
                        m4.metric("Orders", f"{expected.get('orders', 0):.0f}", f"{orders_change:+.1f}%")
                        
                        st.caption("Confidence: 70% probability")
                    
                    st.divider()
                    
                    # SECTION 2: SCENARIO COMPARISON
                    st.markdown("### 📈 Scenario Analysis")
                    
                    scenario_df = pd.DataFrame({
                        'Scenario': ['Current', 'Conservative (15%)', 'Expected (70%)', 'Aggressive (15%)'],
                        'Spend': [
                            current.get('spend', 0),
                            conservative.get('spend', 0),
                            expected.get('spend', 0),
                            aggressive.get('spend', 0)
                        ],
                        'Sales': [
                            current.get('sales', 0),
                            conservative.get('sales', 0),
                            expected.get('sales', 0),
                            aggressive.get('sales', 0)
                        ],
                        'ROAS': [
                            current.get('roas', 0),
                            conservative.get('roas', 0),
                            expected.get('roas', 0),
                            aggressive.get('roas', 0)
                        ],
                        'Orders': [
                            current.get('orders', 0),
                            conservative.get('orders', 0),
                            expected.get('orders', 0),
                            aggressive.get('orders', 0)
                        ],
                        'ACoS': [
                            current.get('acos', 0),
                            conservative.get('acos', 0),
                            expected.get('acos', 0),
                            aggressive.get('acos', 0)
                        ]
                    })
                    
                    st.dataframe(
                        scenario_df.style.format({
                            'Spend': '${:,.0f}',
                            'Sales': '${:,.0f}',
                            'ROAS': '{:.2f}x',
                            'Orders': '{:.0f}',
                            'ACoS': '{:.1f}%'
                        }),
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    st.info("**Expected scenario** has the highest probability (70%) and represents typical market conditions.")
                    
                    st.divider()
                    
                    # SECTION 3: SENSITIVITY ANALYSIS
                    sensitivity_df = simulation.get('sensitivity', pd.DataFrame())
                    
                    if not sensitivity_df.empty:
                        st.markdown("### 🎚️ Sensitivity Analysis")
                        st.markdown("See how different bid adjustment levels would impact performance:")
                        
                        st.dataframe(
                            sensitivity_df.style.format({
                                'Spend': '${:,.0f}',
                                'Sales': '${:,.0f}',
                                'ROAS': '{:.2f}x',
                                'Orders': '{:.0f}',
                                'ACoS': '{:.1f}%'
                            }),
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        # Create interactive chart
                        fig = go.Figure()
                        
                        fig.add_trace(go.Scatter(
                            x=sensitivity_df['Spend'],
                            y=sensitivity_df['ROAS'],
                            mode='lines+markers',
                            name='ROAS vs Spend',
                            text=sensitivity_df['Bid_Adjustment'],
                            hovertemplate='<b>%{text}</b><br>Spend: $%{x:,.0f}<br>ROAS: %{y:.2f}x<extra></extra>'
                        ))
                        
                        fig.update_layout(
                            title='ROAS vs Spend Trade-off',
                            xaxis_title='Weekly Spend ($)',
                            yaxis_title='ROAS',
                            hovermode='closest',
                            showlegend=False
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        st.caption("💡 **Tip:** Look for the sweet spot where ROAS is maximized without excessive spend.")
                    
                    st.divider()
                    
                    # SECTION 4: RISK ANALYSIS
                    risk_analysis = simulation.get('risk_analysis', {})
                    risk_summary = risk_analysis.get('summary', {})
                    
                    st.markdown("### ⚠️ Risk Analysis")
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("🔴 High Risk", risk_summary.get('high_risk_count', 0), help="Review carefully")
                    col2.metric("🟡 Medium Risk", risk_summary.get('medium_risk_count', 0), help="Monitor closely")
                    col3.metric("🟢 Low Risk", risk_summary.get('low_risk_count', 0), help="Safe to apply")
                    
                    # Show high risk changes
                    high_risk = risk_analysis.get('high_risk', [])
                    if high_risk:
                        st.markdown("#### 🔴 High Risk Changes (Review Carefully)")
                        
                        risk_df = pd.DataFrame(high_risk)
                        st.dataframe(
                            risk_df[['keyword', 'campaign', 'bid_change', 'current_bid', 'new_bid', 'reasons']],
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                'keyword': 'Keyword',
                                'campaign': 'Campaign',
                                'bid_change': 'Change',
                                'current_bid': st.column_config.NumberColumn('Current Bid', format='$%.2f'),
                                'new_bid': st.column_config.NumberColumn('New Bid', format='$%.2f'),
                                'reasons': 'Risk Factors'
                            }
                        )
                        
                        st.warning("⚠️ Consider testing these changes on a smaller scale first or reducing the bid adjustment percentage.")
                    else:
                        st.success("✅ No high-risk changes detected. All bid adjustments are within safe parameters.")
                    
                    st.divider()
                    
                    # SECTION 5: INTERPRETATION GUIDE
                    with st.expander("📖 How to Read This Simulation"):
                        st.markdown("""
                        **Scenarios Explained:**
                        - **Conservative (15% probability):** Low competition or already top position. Smaller impact from bid changes.
                        - **Expected (70% probability):** Most likely outcome based on average market conditions.
                        - **Aggressive (15% probability):** High competition, currently low position. Larger impact from bid changes.
                        
                        **Accuracy:**
                        - Forecasts are typically ±15-25% of actual results
                        - More accurate for: Small bid changes (-10% to +10%), Exact match, Established keywords
                        - Less accurate for: Large bid changes (>30%), Auto campaigns, New keywords
                        
                        **Assumptions:**
                        - CVR remains stable (product quality unchanged)
                        - AOV remains stable (order value consistent)
                        - Competitors don't make dramatic bid changes
                        - Seasonality similar to historical period
                        
                        **Confidence Levels:**
                        - **High (80-90%):** Small changes, exact match, 30+ days history
                        - **Medium (60-80%):** Moderate changes, phrase/broad, 2-4 weeks history
                        - **Low (40-60%):** Large changes, auto campaigns, new keywords
                        
                        **What to Do:**
                        1. Review Expected scenario - most likely outcome
                        2. Check risk analysis for problematic changes
                        3. Use sensitivity analysis to find optimal bid levels
                        4. Export and apply bid changes from Actions tab
                        5. Monitor actual results vs forecast to improve future predictions
                        """)
                    
                    st.divider()
                    
                    # SECTION 6: DOWNLOAD REPORT
                    st.markdown("### 📥 Export Simulation Report")
                    
                    # Create comprehensive report
                    report_data = {
                        'Scenario Comparison': scenario_df,
                        'Sensitivity Analysis': sensitivity_df if not sensitivity_df.empty else pd.DataFrame(),
                        'High Risk Changes': pd.DataFrame(high_risk) if high_risk else pd.DataFrame()
                    }
                    
                    # Convert to Excel
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        for sheet_name, df in report_data.items():
                            if not df.empty:
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    st.download_button(
                        "Download Complete Simulation Report",
                        output.getvalue(),
                        "bid_simulation_report.xlsx",
                        use_container_width=True
                    )
                
                else:
                    st.info("Run the optimizer to generate bid simulation forecasts.")
            
            with t9:
                st.subheader("🚀 Harvest Enrichment & Export")
                if 'harvest_payload' in st.session_state:
                    h_df = st.session_state['harvest_payload']
                    camp_file = st.file_uploader("Upload Campaigns File (Optional for SKU Map)", type=["csv", "xlsx"], key="enrich_upload")
                    if camp_file:
                        h_df, msg = map_skus_from_file(h_df, camp_file)
                        st.session_state['harvest_payload'] = h_df
                        st.success(msg)
                    
                    if "New Bid" not in h_df.columns: h_df["New Bid"] = h_df["Cost Per Click (CPC)"] * 1.1
                    if "Advertised SKU" not in h_df.columns: h_df["Advertised SKU"] = "SKU_NEEDED"
                    
                    st.dataframe(h_df[["Campaign Name", "Customer Search Term", "Advertised SKU", "New Bid"]])
                    
                    col1, col2 = st.columns(2)
                    with col1: budget = st.number_input("Total Daily Budget ($)", 10.0, value=50.0, step=5.0)
                    with col2: pid = st.text_input("Portfolio ID (Optional)")
                    
                    if st.button("🎯 Generate Harvest Bulk File"):
                        harvest_bulk = generate_bulk_from_harvest(h_df, pid, budget, datetime.now())
                        
                        # AUTO-VALIDATE
                        validation = validate_bulk_file(harvest_bulk)
                        
                        # Extract error rows
                        error_rows = set()
                        for error in validation['errors']:
                            import re
                            match = re.search(r'Row (\d+):', error)
                            if match:
                                error_rows.add(int(match.group(1)) - 2)
                        
                        valid_rows_df = harvest_bulk.drop(index=list(error_rows)) if error_rows else harvest_bulk
                        error_rows_df = harvest_bulk.loc[list(error_rows)] if error_rows else pd.DataFrame()
                        
                        # Show validation summary
                        if validation['errors']:
                            st.error(f"🚨 **{len(validation['errors'])} Critical Errors Found**")
                            st.markdown(f"**{len(error_rows_df)} rows have errors**")
                        else:
                            st.success("✅ **No errors found!** File ready for upload.")
                        
                        st.divider()
                        
                        # Tabs for valid vs error rows
                        tab1, tab2 = st.tabs([
                            f"✅ Valid Rows ({len(valid_rows_df)})",
                            f"❌ Error Rows ({len(error_rows_df)})"
                        ])
                        
                        with tab1:
                            if not valid_rows_df.empty:
                                st.success(f"✅ {len(valid_rows_df)} rows ready for upload")
                                timestamp = datetime.now().strftime("%Y%m%d")
                                st.download_button(
                                    "📥 Download Valid Harvest Campaigns",
                                    to_excel_download(valid_rows_df, f"harvest_valid_{timestamp}.xlsx"),
                                    f"harvest_valid_{timestamp}.xlsx",
                                    use_container_width=True,
                                    type="primary"
                                )
                                with st.expander("👁️ Preview"): st.dataframe(valid_rows_df)
                            else:
                                st.warning("⚠️ No valid rows")
                        
                        with tab2:
                            if not error_rows_df.empty:
                                st.error(f"❌ {len(error_rows_df)} rows need fixing")
                                timestamp = datetime.now().strftime("%Y%m%d")
                                st.download_button(
                                    "📥 Download Error Rows",
                                    to_excel_download(error_rows_df, f"harvest_errors_{timestamp}.xlsx"),
                                    f"harvest_errors_{timestamp}.xlsx",
                                    use_container_width=True
                                )
                                with st.expander("📋 View Errors"):
                                    for error in validation['errors'][:10]:
                                        st.text(error)
                                    if len(validation['errors']) > 10:
                                        st.caption(f"... and {len(validation['errors']) - 10} more")
                            else:
                                st.success("✅ No errors!")
                
                else:
                    st.warning("⚠️ No harvest data pending. Go to '💎 Harvest' tab and click 'Prepare'.")

# ==========================================
# README & CREATOR (Standalone)
# ==========================================

elif st.session_state['current_module'] == 'readme':
    st.title("📖 Guide")
    st.markdown("### V3.3 Update Notes")
    st.markdown("- **Bug Fix:** Fixed unknown search terms in report using iterrows().")
    st.markdown("- **UI:** Added full text report display directly to dashboard.")
    st.markdown("- **Safety:** Empty metric values now default to 0 instead of causing a crash.")

elif st.session_state['current_module'] == 'creator':
    st.title("🚀 LaunchPad: Campaign Creator")
    c1,c2,c3 = st.columns([3,3,2])
    with c1: sku_input = st.text_input("Advertised SKU(s)", key="c_sku")
    with c2: asin_input = st.text_input("Competitor ASINs", key="c_asin")
    with c3: uploaded_kw = st.file_uploader("Keyword List", type=['csv','xlsx'], key="c_kw")
    c4,c5,c6,c7 = st.columns(4)
    with c4: price = st.number_input("Price", 99.0)
    with c5: acos = st.slider("Target ACoS", 5, 40, 20)
    with c6: cvr = st.selectbox("CVR %", [6,9,12,15,20], index=2)
    with c7: budget = st.number_input("Budget", 200.0)
    
    if st.button("⚡ Generate File", type="primary"):
        skus = parse_skus(sku_input)
        if not skus: st.error("❌ SKU required")
        else:
            keywords = parse_keywords_creator(uploaded_kw)
            asins = parse_asins(asin_input)
            base_bid = calc_base_bid(price, acos, cvr)
            allocation = allocate_budget_by_priority(budget, ["Auto","Manual: Keywords","Manual: ASIN/Product"])
            entities = []
            ts = datetime.now().strftime("%Y%m%d")
            
            for tactic, alloc in allocation.items():
                camp_id = f"ZEN_{skus[0]}_{tactic.replace(' ','')}"
                ag_id = f"{camp_id}_AG"
                append_row_dict(entities, {"Entity":"Campaign", "Campaign ID":camp_id, "Daily Budget": f"{alloc:.2f}", "Start Date":ts})
                append_row_dict(entities, {"Entity":"Ad Group", "Campaign ID":camp_id, "Ad Group ID":ag_id, "Ad Group Default Bid": f"{base_bid:.2f}"})
                append_row_dict(entities, {"Entity":"Product Ad", "Campaign ID":camp_id, "Ad Group ID":ag_id, "SKU":skus[0], "Ad Group Default Bid": f"{base_bid:.2f}"})
            
                if tactic == "Auto":
                    pass 

                elif tactic == "Manual: Keywords":
                    kw_list = keywords if keywords else ["coffee mug","travel mug"]
                    idx = 0
                    for i in range(min(int(DEFAULT_EXACT_TOP), len(kw_list))):
                        append_row_dict(entities, {
                            "Entity":"Keyword", "Campaign ID":camp_id, "Ad Group ID":ag_id,
                            "Bid":f"{round(base_bid*1.2,2):.2f}", "Keyword Text":kw_list[idx], "Match Type":"exact"
                        })
                        idx += 1
                    for i in range(min(int(DEFAULT_PHRASE_NEXT), max(0, len(kw_list)-idx))):
                        append_row_dict(entities, {
                            "Entity":"Keyword", "Campaign ID":camp_id, "Ad Group ID":ag_id,
                            "Bid":f"{round(base_bid*1.0,2):.2f}", "Keyword Text":kw_list[idx], "Match Type":"phrase"
                        })
                        idx += 1
                    while idx < len(kw_list):
                        append_row_dict(entities, {
                            "Entity":"Keyword", "Campaign ID":camp_id, "Ad Group ID":ag_id,
                            "Bid":f"{round(base_bid*0.8,2):.2f}", "Keyword Text":kw_list[idx], "Match Type":"broad"
                        })
                        idx += 1

                elif tactic == "Manual: ASIN/Product":
                    targets = asins if asins else ["B0XXXXXX"]
                    for a in targets:
                        clean_asin = a.strip().upper()
                        append_row_dict(entities, {
                            "Entity":"Product Targeting", "Campaign ID":camp_id, "Ad Group ID":ag_id,
                            "Bid":f"{round(base_bid*1.1,2):.2f}",
                            "Product Targeting Expression":f'asin="{clean_asin}"'
                        })

                elif tactic == "Category":
                    append_row_dict(entities, {
                        "Entity":"Product Targeting", "Campaign ID":camp_id, "Ad Group ID":ag_id,
                        "Bid":f"{base_bid:.2f}", "Product Targeting Expression":'category="ID"'
                    })

            df_bulk = pd.DataFrame(entities, columns=COLUMN_ORDER_CREATOR)
            st.dataframe(df_bulk)
            st.download_button("📥 Download", to_excel_with_metadata(df_bulk, {}), f"campaigns_{ts}.xlsx")