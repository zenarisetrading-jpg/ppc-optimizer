"""
Firebase FIRESTORE Integration for PPC Optimizer
V3.1 - With verbose debugging

SETUP:
1. Download service account JSON from Firebase Console
2. Copy to project folder (e.g., firebase_credentials.json)
3. Add to .streamlit/secrets.toml:
   [firebase]
   credentials_file = "firebase_credentials.json"
"""

import json
import hashlib
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
import pandas as pd
import numpy as np
import streamlit as st

# Debug helper
def debug_log(msg: str):
    """Log debug messages that show in Streamlit."""
    print(f"[Firebase Debug] {msg}")
    if 'firebase_debug' not in st.session_state:
        st.session_state['firebase_debug'] = []
    st.session_state['firebase_debug'].append(f"{datetime.now().strftime('%H:%M:%S')} - {msg}")

# Try to import firebase_admin
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    FIREBASE_AVAILABLE = True
    debug_log("✅ firebase_admin imported successfully")
except ImportError as e:
    FIREBASE_AVAILABLE = False
    firebase_admin = None
    credentials = None
    firestore = None
    debug_log(f"❌ firebase_admin import failed: {e}")


class FirebaseDB:
    """Firebase Firestore wrapper for PPC Optimizer."""
    
    # Use session_state for persistence across Streamlit reruns
    @classmethod
    def _get_state(cls):
        """Get or initialize state from session_state."""
        if 'firebase_state' not in st.session_state:
            st.session_state['firebase_state'] = {
                'initialized': False,
                'db': None,
                'app': None,
                'last_error': None,
                'project_id': None
            }
        return st.session_state['firebase_state']
    
    @classmethod
    def initialize(cls, streamlit_secrets) -> bool:
        """Initialize Firebase Firestore from JSON file."""
        state = cls._get_state()
        
        debug_log(f"initialize() called, FIREBASE_AVAILABLE={FIREBASE_AVAILABLE}")
        
        if not FIREBASE_AVAILABLE:
            state['last_error'] = "firebase-admin not installed. Run: pip install firebase-admin"
            debug_log(f"❌ {state['last_error']}")
            return False
        
        # Check if already initialized
        if state['initialized'] and state['db'] is not None:
            debug_log("Already initialized, returning True")
            return True
        
        try:
            # Step 1: Get firebase config from secrets
            debug_log("Step 1: Reading secrets...")
            firebase_config = None
            
            if hasattr(streamlit_secrets, 'firebase'):
                firebase_config = streamlit_secrets.firebase
                debug_log("Found secrets via attribute access")
            elif 'firebase' in streamlit_secrets:
                firebase_config = streamlit_secrets['firebase']
                debug_log("Found secrets via dict access")
            else:
                available_keys = list(streamlit_secrets.keys()) if hasattr(streamlit_secrets, 'keys') else dir(streamlit_secrets)
                state['last_error'] = f"No [firebase] section in secrets. Available: {available_keys}"
                debug_log(f"❌ {state['last_error']}")
                return False
            
            debug_log(f"Firebase config keys: {list(firebase_config.keys()) if hasattr(firebase_config, 'keys') else 'N/A'}")
            
            # Step 2: Get credentials file path
            debug_log("Step 2: Getting credentials file...")
            credentials_file = None
            
            if hasattr(firebase_config, 'credentials_file'):
                credentials_file = firebase_config.credentials_file
            elif isinstance(firebase_config, dict) and 'credentials_file' in firebase_config:
                credentials_file = firebase_config['credentials_file']
            else:
                try:
                    credentials_file = firebase_config.get('credentials_file')
                except:
                    pass
            
            debug_log(f"Credentials file: {credentials_file}")
            
            if not credentials_file:
                state['last_error'] = "No credentials_file in firebase config"
                debug_log(f"❌ {state['last_error']}")
                return False
            
            # Step 3: Check file exists
            debug_log("Step 3: Checking file exists...")
            if not os.path.exists(credentials_file):
                possible_paths = [
                    credentials_file,
                    os.path.join(os.getcwd(), credentials_file),
                    os.path.join(os.path.dirname(__file__), credentials_file),
                    f"./{credentials_file}",
                ]
                
                found_path = None
                for path in possible_paths:
                    debug_log(f"  Trying: {path} - exists: {os.path.exists(path)}")
                    if os.path.exists(path):
                        found_path = path
                        break
                
                if not found_path:
                    state['last_error'] = f"Credentials file not found: {credentials_file}. CWD: {os.getcwd()}"
                    debug_log(f"❌ {state['last_error']}")
                    return False
                
                credentials_file = found_path
            
            debug_log(f"Using credentials file: {credentials_file}")
            
            # Step 4: Load credentials
            debug_log("Step 4: Loading credentials...")
            try:
                cred = credentials.Certificate(credentials_file)
                debug_log("✅ Credentials loaded")
                
                with open(credentials_file, 'r') as f:
                    data = json.load(f)
                state['project_id'] = data.get('project_id', 'unknown')
                debug_log(f"Project ID: {state['project_id']}")
            except Exception as e:
                state['last_error'] = f"Invalid credentials file: {str(e)}"
                debug_log(f"❌ {state['last_error']}")
                return False
            
            # Step 5: Initialize Firebase app
            debug_log("Step 5: Initializing Firebase app...")
            try:
                try:
                    state['app'] = firebase_admin.get_app()
                    debug_log("Firebase app already exists, reusing")
                except ValueError:
                    state['app'] = firebase_admin.initialize_app(cred)
                    debug_log("✅ Firebase app initialized")
            except Exception as e:
                state['last_error'] = f"Failed to initialize Firebase app: {str(e)}"
                debug_log(f"❌ {state['last_error']}")
                return False
            
            # Step 6: Get Firestore client
            debug_log("Step 6: Getting Firestore client...")
            try:
                state['db'] = firestore.client()
                debug_log("✅ Firestore client created")
            except Exception as e:
                state['last_error'] = f"Failed to get Firestore client: {str(e)}"
                debug_log(f"❌ {state['last_error']}")
                return False
            
            # Step 7: Test connection
            debug_log("Step 7: Testing connection...")
            try:
                collections = list(state['db'].collections())
                debug_log(f"✅ Connection test passed. Collections: {[c.id for c in collections]}")
            except Exception as e:
                debug_log(f"⚠️ Connection test warning: {str(e)}")
            
            state['initialized'] = True
            state['last_error'] = None
            debug_log(f"✅ Firestore fully initialized for project: {state['project_id']}")
            return True
            
        except Exception as e:
            state['last_error'] = f"Unexpected error: {str(e)}"
            debug_log(f"❌ {state['last_error']}")
            import traceback
            debug_log(f"Traceback: {traceback.format_exc()}")
            return False
    
    @classmethod
    def get_db(cls):
        """Get Firestore client."""
        return cls._get_state()['db']
    
    @classmethod
    def get_last_error(cls) -> str:
        return cls._get_state()['last_error'] or "Unknown error"
    
    @classmethod
    def is_available(cls) -> bool:
        state = cls._get_state()
        return FIREBASE_AVAILABLE and state['initialized'] and state['db'] is not None
    
    @classmethod
    def get_status(cls) -> Dict:
        """Get detailed connection status."""
        state = cls._get_state()
        return {
            'available': cls.is_available(),
            'initialized': state['initialized'],
            'firebase_installed': FIREBASE_AVAILABLE,
            'project_id': state['project_id'],
            'error': state['last_error'],
            'db_exists': state['db'] is not None
        }
    
    @classmethod
    def get_debug_logs(cls) -> List[str]:
        """Get debug logs from session state."""
        return st.session_state.get('firebase_debug', [])
    
    @staticmethod
    def _hash_term(term: str) -> str:
        """Create safe document ID from search term."""
        return hashlib.md5(term.lower().strip().encode()).hexdigest()[:16]
    
    @staticmethod
    def _get_account_id(df: pd.DataFrame) -> str:
        """Auto-detect account ID from campaign naming convention."""
        if 'Campaign Name' not in df.columns:
            return 'default'
        campaigns = df['Campaign Name'].dropna().unique()
        if len(campaigns) == 0:
            return 'default'
        
        first = str(campaigns[0])
        if '_' in first:
            prefix = first.split('_')[0]
            clean = ''.join(c for c in prefix if c.isalnum() or c == '-')
            return clean.lower()[:20] if clean else 'default'
        return 'default'
    
    @staticmethod
    def _get_week_key(date: datetime = None) -> str:
        """Get ISO week key (YYYY-WXX format)."""
        if date is None:
            date = datetime.now()
        year, week, _ = date.isocalendar()
        return f"{year}-W{week:02d}"


# ==========================================
# SAVE FUNCTIONS
# ==========================================

def save_upload_snapshot(
    df: pd.DataFrame, 
    date_info: dict, 
    stats: dict, 
    account_id: str = None
) -> Tuple[bool, str]:
    """Save upload snapshot to Firestore.
    
    FIXED: Uses the END DATE from the report's date range to determine
    the week key, not the upload date. This ensures different weekly
    reports go to different documents.
    """
    if not FirebaseDB.is_available():
        return False, f"Firestore not available: {FirebaseDB.get_last_error()}"
    
    try:
        db = FirebaseDB.get_db()
        
        if not account_id:
            account_id = FirebaseDB._get_account_id(df)
        
        # FIXED: Use the END date from the data's date range, not current date
        # This ensures each week's data goes to a unique document
        end_date_str = date_info.get('end', '')
        if end_date_str:
            try:
                # Try to parse the end date from the report
                from dateutil import parser as date_parser
                end_date = date_parser.parse(str(end_date_str))
                week_key = FirebaseDB._get_week_key(end_date)
                debug_log(f"Using data end date for week_key: {end_date_str} -> {week_key}")
            except Exception as e:
                # Fallback to current date
                week_key = FirebaseDB._get_week_key()
                debug_log(f"Could not parse end date '{end_date_str}', using current: {week_key}")
        else:
            # Fallback: Use upload timestamp with counter to avoid overwrites
            week_key = FirebaseDB._get_week_key()
            
            # Check if this week already exists, if so append a counter
            existing = db.collection('uploads').document(account_id).collection('weeks').document(week_key).get()
            if existing.exists:
                # Find next available slot
                counter = 2
                while True:
                    new_key = f"{week_key}-{counter}"
                    check = db.collection('uploads').document(account_id).collection('weeks').document(new_key).get()
                    if not check.exists:
                        week_key = new_key
                        debug_log(f"Week exists, using new key: {week_key}")
                        break
                    counter += 1
                    if counter > 10:  # Safety limit
                        break
        
        timestamp = datetime.now()
        
        # Aggregate keywords (top 500 by orders)
        keyword_data = {}
        keyword_count = 0
        
        if 'Customer Search Term' in df.columns:
            agg_cols = {}
            for col in ['Clicks', 'Spend', 'Orders_Attributed', 'Sales_Attributed', 'Impressions']:
                if col in df.columns:
                    agg_cols[col] = 'sum'
            
            if agg_cols:
                keyword_agg = df.groupby('Customer Search Term').agg(agg_cols).reset_index()
                keyword_agg = keyword_agg.sort_values(
                    'Orders_Attributed' if 'Orders_Attributed' in keyword_agg.columns else 'Clicks',
                    ascending=False
                ).head(500)
                
                for _, row in keyword_agg.iterrows():
                    term = str(row['Customer Search Term'])
                    clicks = int(row.get('Clicks', 0))
                    orders = int(row.get('Orders_Attributed', 0))
                    
                    keyword_data[FirebaseDB._hash_term(term)] = {
                        't': term[:100],
                        'c': clicks,
                        's': round(float(row.get('Spend', 0)), 2),
                        'o': orders,
                        'r': round(float(row.get('Sales_Attributed', 0)), 2),
                        'cvr': round((orders / clicks * 100) if clicks > 0 else 0, 2)
                    }
                    keyword_count += 1
        
        # Prepare document data
        upload_data = {
            'timestamp': timestamp,
            'week': week_key,
            'account_id': account_id,
            'date_range': {
                'start': str(date_info.get('start', '')),
                'end': str(date_info.get('end', '')),
                'weeks': float(date_info.get('weeks', 1)),
                'label': date_info.get('label', '')
            },
            'summary': {
                'total_spend': round(float(stats.get('total_spend', 0)), 2),
                'total_sales': round(float(stats.get('total_sales', 0)), 2),
                'total_orders': int(stats.get('total_orders', 0)),
                'harvest_count': int(stats.get('harvest_count', 0)),
                'negative_count': int(stats.get('negative_count', 0)),
                'keyword_count': keyword_count
            },
            'keywords': keyword_data
        }
        
        # Save to Firestore
        doc_ref = db.collection('uploads').document(account_id).collection('weeks').document(week_key)
        doc_ref.set(upload_data)
        
        debug_log(f"Saved {keyword_count} keywords for {account_id}/{week_key}")
        return True, f"✅ Saved {keyword_count} keywords for week {week_key}"
        
    except Exception as e:
        error_msg = f"Firestore save error: {str(e)}"
        debug_log(f"❌ {error_msg}")
        return False, error_msg


# ==========================================
# RETRIEVAL FUNCTIONS
# ==========================================

def get_previous_upload(account_id: str = 'default') -> Optional[dict]:
    """Get most recent previous upload (not current week)."""
    if not FirebaseDB.is_available():
        return None
    
    try:
        db = FirebaseDB.get_db()
        current_week = FirebaseDB._get_week_key()
        
        weeks_ref = db.collection('uploads').document(account_id).collection('weeks')
        docs = weeks_ref.order_by('week', direction=firestore.Query.DESCENDING).limit(5).stream()
        
        for doc in docs:
            data = doc.to_dict()
            if data.get('week') != current_week:
                return data
        
        return None
        
    except Exception as e:
        debug_log(f"get_previous_upload error: {e}")
        return None


def get_upload_by_week(account_id: str, week_key: str) -> Optional[dict]:
    """Get upload for a specific week."""
    if not FirebaseDB.is_available():
        return None
    
    try:
        db = FirebaseDB.get_db()
        doc_ref = db.collection('uploads').document(account_id).collection('weeks').document(week_key)
        doc = doc_ref.get()
        return doc.to_dict() if doc.exists else None
    except Exception as e:
        debug_log(f"get_upload_by_week error: {e}")
        return None


def get_historical_uploads(account_id: str = 'default', num_weeks: int = 8) -> List[dict]:
    """Get historical uploads for trend analysis."""
    if not FirebaseDB.is_available():
        return []
    
    try:
        db = FirebaseDB.get_db()
        weeks_ref = db.collection('uploads').document(account_id).collection('weeks')
        docs = weeks_ref.order_by('week', direction=firestore.Query.DESCENDING).limit(num_weeks).stream()
        
        result = []
        for doc in docs:
            data = doc.to_dict()
            result.append({
                'week': data.get('week', doc.id),
                'timestamp': data.get('timestamp'),
                'summary': data.get('summary', {}),
                'keywords': data.get('keywords', {}),
                'date_range': data.get('date_range', {})
            })
        
        return result
        
    except Exception as e:
        debug_log(f"get_historical_uploads error: {e}")
        return []


# ==========================================
# VELOCITY COMPARISON
# ==========================================

def get_velocity_comparison(
    current_df: pd.DataFrame, 
    account_id: str = None,
    min_clicks: int = 10
) -> pd.DataFrame:
    """Compare current upload with previous week for velocity tracking."""
    if not account_id:
        account_id = FirebaseDB._get_account_id(current_df)
    
    previous = get_previous_upload(account_id)
    
    if not previous or 'keywords' not in previous:
        return pd.DataFrame(columns=[
            'Search Term', 'Orders_Current', 'Orders_Previous', 'Orders_Change_%',
            'Orders_Delta', 'CVR_Current_%', 'CVR_Previous_%', 'CVR_Change',
            'Clicks_Current', 'Clicks_Previous', 'Spend', 'Sales', 'Trend'
        ])
    
    prev_keywords = previous.get('keywords', {})
    velocity_data = []
    
    if 'Customer Search Term' not in current_df.columns:
        return pd.DataFrame()
    
    agg_cols = {}
    for col in ['Clicks', 'Spend', 'Orders_Attributed', 'Sales_Attributed']:
        if col in current_df.columns:
            agg_cols[col] = 'sum'
    
    if not agg_cols:
        return pd.DataFrame()
    
    current_agg = current_df.groupby('Customer Search Term').agg(agg_cols).reset_index()
    
    for _, row in current_agg.iterrows():
        term = str(row['Customer Search Term'])
        term_hash = FirebaseDB._hash_term(term)
        
        curr_orders = int(row.get('Orders_Attributed', 0))
        curr_clicks = int(row.get('Clicks', 0))
        curr_spend = float(row.get('Spend', 0))
        curr_sales = float(row.get('Sales_Attributed', 0))
        
        prev = prev_keywords.get(term_hash, {})
        prev_orders = prev.get('o', prev.get('orders', 0))
        prev_clicks = prev.get('c', prev.get('clicks', 0))
        prev_cvr_stored = prev.get('cvr', None)
        
        total_clicks = curr_clicks + prev_clicks
        if total_clicks < min_clicks:
            continue
        
        curr_cvr = (curr_orders / curr_clicks * 100) if curr_clicks > 0 else 0
        prev_cvr = prev_cvr_stored if prev_cvr_stored is not None else ((prev_orders / prev_clicks * 100) if prev_clicks > 0 else 0)
        cvr_change = curr_cvr - prev_cvr
        
        orders_delta = curr_orders - prev_orders
        
        if prev_orders > 0:
            orders_pct = ((curr_orders - prev_orders) / prev_orders) * 100
        else:
            orders_pct = 999 if curr_orders > 0 else 0
        
        if prev_clicks > 0:
            clicks_pct = ((curr_clicks - prev_clicks) / prev_clicks) * 100
        else:
            clicks_pct = 999 if curr_clicks > 0 else 0
        
        if orders_pct >= 50:
            trend = "📈 Rising Strong"
        elif orders_pct >= 20:
            trend = "↗️ Rising"
        elif orders_pct <= -50:
            trend = "📉 Falling Strong"
        elif orders_pct <= -20:
            trend = "↘️ Falling"
        elif clicks_pct >= 50:
            trend = "⬆️ Traffic Up"
        elif clicks_pct <= -50:
            trend = "⬇️ Traffic Down"
        else:
            trend = "→ Stable"
        
        velocity_data.append({
            'Search Term': term,
            'Orders_Current': curr_orders,
            'Orders_Previous': prev_orders,
            'Orders_Change_%': round(orders_pct, 1),
            'Orders_Delta': orders_delta,
            'Clicks_Current': curr_clicks,
            'Clicks_Previous': prev_clicks,
            'Clicks_Change_%': round(clicks_pct, 1),
            'CVR_Current_%': round(curr_cvr, 2),
            'CVR_Previous_%': round(prev_cvr, 2),
            'CVR_Change': round(cvr_change, 2),
            'Spend': round(curr_spend, 2),
            'Sales': round(curr_sales, 2),
            'Trend': trend,
            'Total_Clicks': total_clicks
        })
    
    df = pd.DataFrame(velocity_data)
    
    if df.empty:
        return df
    
    return df.sort_values('Orders_Delta', ascending=False, key=abs)


def get_weekly_trends(
    current_df: pd.DataFrame, 
    account_id: str = None, 
    num_weeks: int = 4
) -> pd.DataFrame:
    """Get weekly trends for top keywords over past N weeks."""
    if not account_id:
        account_id = FirebaseDB._get_account_id(current_df)
    
    history = get_historical_uploads(account_id, num_weeks + 1)
    
    if len(history) < 2:
        return pd.DataFrame()
    
    if 'Customer Search Term' not in current_df.columns:
        return pd.DataFrame()
    
    agg_cols = {}
    for col in ['Clicks', 'Orders_Attributed', 'Sales_Attributed']:
        if col in current_df.columns:
            agg_cols[col] = 'sum'
    
    if not agg_cols:
        return pd.DataFrame()
    
    current_agg = current_df.groupby('Customer Search Term').agg(agg_cols).reset_index()
    sort_col = 'Orders_Attributed' if 'Orders_Attributed' in current_agg.columns else 'Clicks'
    current_agg = current_agg.sort_values(sort_col, ascending=False).head(30)
    
    trend_data = []
    
    for _, row in current_agg.iterrows():
        term = str(row['Customer Search Term'])
        term_hash = FirebaseDB._hash_term(term)
        
        curr_orders = int(row.get('Orders_Attributed', 0))
        curr_clicks = int(row.get('Clicks', 0))
        curr_cvr = (curr_orders / curr_clicks * 100) if curr_clicks > 0 else 0
        
        row_data = {
            'Search Term': term,
            'Current': curr_orders,
            'Current_CVR': round(curr_cvr, 2)
        }
        
        weekly_orders = []
        
        for i, upload in enumerate(history[:num_weeks]):
            kw_data = upload.get('keywords', {}).get(term_hash, {})
            orders = kw_data.get('o', kw_data.get('orders', 0))
            cvr = kw_data.get('cvr', 0)
            
            row_data[f'W{i+1}_Orders'] = orders
            row_data[f'W{i+1}_CVR'] = round(cvr, 2)
            weekly_orders.append(orders)
        
        if len(weekly_orders) >= 2:
            first_half = sum(weekly_orders[:len(weekly_orders)//2]) or 1
            second_half = sum(weekly_orders[len(weekly_orders)//2:]) + curr_orders
            trend_pct = ((second_half - first_half) / first_half) * 100
            row_data['Trend_%'] = round(trend_pct, 1)
        else:
            row_data['Trend_%'] = 0
        
        trend_data.append(row_data)
    
    df = pd.DataFrame(trend_data)
    return df.sort_values('Trend_%', ascending=False) if not df.empty else df


# ==========================================
# ACCOUNT MANAGEMENT
# ==========================================

def list_accounts() -> List[str]:
    """List all account IDs in the database."""
    if not FirebaseDB.is_available():
        return []
    
    try:
        db = FirebaseDB.get_db()
        docs = db.collection('uploads').stream()
        return sorted([doc.id for doc in docs])
    except Exception as e:
        debug_log(f"list_accounts error: {e}")
        return []


def get_account_info(account_id: str) -> Dict:
    """Get detailed info about an account."""
    if not FirebaseDB.is_available():
        return {}
    
    try:
        db = FirebaseDB.get_db()
        weeks_ref = db.collection('uploads').document(account_id).collection('weeks')
        docs = list(weeks_ref.order_by('week').stream())
        
        if not docs:
            return {}
        
        weeks = [doc.id for doc in docs]
        
        return {
            'account_id': account_id,
            'upload_count': len(weeks),
            'first_upload': weeks[0] if weeks else None,
            'last_upload': weeks[-1] if weeks else None,
            'weeks': weeks
        }
    except Exception as e:
        debug_log(f"get_account_info error: {e}")
        return {}


def get_upload_count(account_id: str = None) -> int:
    """Get number of uploads for an account or total."""
    if not FirebaseDB.is_available():
        return 0
    
    try:
        db = FirebaseDB.get_db()
        
        if account_id:
            weeks_ref = db.collection('uploads').document(account_id).collection('weeks')
            docs = list(weeks_ref.stream())
            return len(docs)
        else:
            total = 0
            accounts = list_accounts()
            for acc in accounts:
                weeks_ref = db.collection('uploads').document(acc).collection('weeks')
                docs = list(weeks_ref.stream())
                total += len(docs)
            return total
    except Exception as e:
        debug_log(f"get_upload_count error: {e}")
        return 0


def reset_account(account_id: str) -> Tuple[bool, str]:
    """Reset/clear all data for a specific account."""
    if not FirebaseDB.is_available():
        return False, "Firestore not available"
    
    if not account_id:
        return False, "Account ID required"
    
    try:
        db = FirebaseDB.get_db()
        
        weeks_ref = db.collection('uploads').document(account_id).collection('weeks')
        docs = weeks_ref.stream()
        
        deleted = 0
        for doc in docs:
            doc.reference.delete()
            deleted += 1
        
        db.collection('uploads').document(account_id).delete()
        
        return True, f"✅ Cleared {deleted} weeks for account: {account_id}"
    except Exception as e:
        return False, f"Reset error: {str(e)}"


def reset_database() -> Tuple[bool, str]:
    """Reset/clear ALL data."""
    if not FirebaseDB.is_available():
        return False, "Firestore not available"
    
    try:
        accounts = list_accounts()
        total_deleted = 0
        
        for account_id in accounts:
            success, msg = reset_account(account_id)
            if success:
                total_deleted += 1
        
        return True, f"✅ Cleared all data ({total_deleted} accounts)"
    except Exception as e:
        return False, f"Reset error: {str(e)}"


def get_top_movers_chart_data(
    velocity_df: pd.DataFrame, 
    top_n: int = 15,
    metric: str = 'orders'
) -> Dict:
    """Prepare data for bar chart showing top movers."""
    if velocity_df.empty:
        return {'rising': pd.DataFrame(), 'falling': pd.DataFrame()}
    
    df = velocity_df[velocity_df['Orders_Change_%'].abs() < 500].copy()
    
    if df.empty:
        return {'rising': pd.DataFrame(), 'falling': pd.DataFrame()}
    
    if 'Orders_Delta' in df.columns:
        rising = df[df['Orders_Delta'] > 0].nlargest(top_n, 'Orders_Delta')
        falling = df[df['Orders_Delta'] < 0].nsmallest(top_n, 'Orders_Delta')
    else:
        rising = df[df['Orders_Change_%'] > 0].nlargest(top_n, 'Orders_Change_%')
        falling = df[df['Orders_Change_%'] < 0].nsmallest(top_n, 'Orders_Change_%')
    
    return {
        'rising': rising,
        'falling': falling
    }
