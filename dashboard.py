"""
Business Finance Dashboard 2026
================================
Interactive Streamlit dashboard for tracking business finances.
Reads live data from Google Sheets (2026_Business_Finance).

Run:  streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime
import re
import os
import json
import io
import tempfile
import streamlit.components.v1 as components
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ─── Configuration ───────────────────────────────────────────────────────────
SHEET_ID = '1aLRvXRf9ni6i7u6WzcEk9psoTwa9XQNJQZ3jS-nXUj4'
DRIVE_ROOT_FOLDER = '1s3EXNwPn47Rg2Ca2lPtBMBtHLEQHhtx7'
_TOKEN_FILE = Path(__file__).parent / 'token.json'
CURRENT_YEAR = 2026  # Change this (+ SHEET_ID) when starting a new year

MONTHS = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
]

# ─── Theme System ────────────────────────────────────────────────────────────
THEMES = {
    'light': {
        'bg': '#FAFAFA',
        'surface': '#FFFFFF',
        'surface2': '#F5F5F5',
        'surface3': '#EBEBEB',
        'border': 'rgba(0,0,0,0.08)',
        'border_hover': 'rgba(0,0,0,0.15)',
        'text': '#1A1A2E',
        'text_secondary': '#6B7280',
        'muted': '#9CA3AF',
        'card_shadow': '0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04)',
        'card_shadow_hover': '0 4px 12px rgba(0,0,0,0.08)',
        'chart_grid': 'rgba(0,0,0,0.06)',
        'chart_zeroline': 'rgba(0,0,0,0.10)',
        'row_hover': 'rgba(0,0,0,0.02)',
        'row_border': 'rgba(0,0,0,0.04)',
    },
    'dark': {
        'bg': '#0D1117',
        'surface': '#161B22',
        'surface2': '#1C2333',
        'surface3': '#242D3D',
        'border': 'rgba(255,255,255,0.06)',
        'border_hover': 'rgba(255,255,255,0.12)',
        'text': '#F0F0F0',
        'text_secondary': '#8B949E',
        'muted': '#6B7280',
        'card_shadow': '0 1px 3px rgba(0,0,0,0.3)',
        'card_shadow_hover': '0 4px 12px rgba(0,0,0,0.4)',
        'chart_grid': 'rgba(255,255,255,0.04)',
        'chart_zeroline': 'rgba(255,255,255,0.06)',
        'row_hover': 'rgba(255,255,255,0.025)',
        'row_border': 'rgba(255,255,255,0.03)',
    },
}

def _t():
    """Return current theme dict based on session state."""
    return THEMES[st.session_state.get('theme', 'light')]

# ─── Accent Colors (shared across themes) ───────────────────────────────────
C_ORANGE = '#E85D26'
C_ORANGE_LIGHT = '#FF7A45'
C_GREEN = '#4ADE80'
C_GREEN_DIM = 'rgba(74,222,128,0.15)'
C_RED = '#F87171'
C_RED_DIM = 'rgba(248,113,113,0.15)'
C_BLUE = '#60A5FA'

CHART_COLORS = [
    '#E85D26', '#FF7A45', '#FFB088', '#60A5FA', '#3B82F6',
    '#818CF8', '#A78BFA', '#C084FC', '#4ADE80', '#34D399', '#6EE7B7',
]

# Badge color mapping: category → (text_color, bg_color)
BADGE_STYLES = {
    'AI Software': (C_ORANGE_LIGHT, 'rgba(232,93,38,0.15)'),
    'AI Studio': (C_ORANGE_LIGHT, 'rgba(232,93,38,0.15)'),
    'Accounting': (C_BLUE, 'rgba(96,165,250,0.15)'),
    'Insurance': (C_GREEN, C_GREEN_DIM),
    'Office': (C_ORANGE_LIGHT, 'rgba(232,93,38,0.15)'),
    'Miles': (C_BLUE, 'rgba(96,165,250,0.15)'),
    'Education': (C_GREEN, C_GREEN_DIM),
    'Restaurants': (C_RED, C_RED_DIM),
    'Travel Cost': (C_GREEN, C_GREEN_DIM),
    'Gewerbe': (C_BLUE, 'rgba(96,165,250,0.15)'),
    'Gear': (C_BLUE, 'rgba(96,165,250,0.15)'),
    'Gear Rental': (C_GREEN, C_GREEN_DIM),
    'Animation': (C_BLUE, 'rgba(96,165,250,0.15)'),
    'Photography': (C_GREEN, C_GREEN_DIM),
    'Video Production': (C_GREEN, C_GREEN_DIM),
}

FONT = "'Inter', -apple-system, 'Helvetica Neue', Helvetica, Arial, sans-serif"

# Category → filename code mapping for uploaded expense PDFs
CATEGORIES = [
    'Insurance', 'Accounting', 'Gear', 'AI Software', 'Restaurants',
    'Office', 'Miles', 'Education', 'Gear Rental', 'Travel Cost', 'Gewerbe',
]
CATEGORY_FILE_MAP = {
    'Insurance': 'Insurance', 'Accounting': 'Accounting', 'Gear': 'Gear',
    'AI Software': 'AI_Software', 'Restaurants': 'Restaurants', 'Office': 'Office',
    'Miles': 'Miles', 'Education': 'Education', 'Gear Rental': 'Gear_Rental',
    'Travel Cost': 'Travel_Cost', 'Gewerbe': 'Gewerbe',
}
YEAR_FOLDER = str(CURRENT_YEAR)
INVOICES_FOLDER = f"INVOICES {CURRENT_YEAR}"
OFFERS_FOLDER = f"OFFERS {CURRENT_YEAR}"
# Jan/Feb 2026 used 03_Regular Cost + 04_Irregular Cost subfolders.
# From March 2026 onward, all expenses go into a single "Costs" subfolder.
_LEGACY_COST_SUBFOLDERS = ['03_Regular Cost', '04_Irregular Cost']
_NEW_COST_SUBFOLDER = 'Costs'

# Tax rate for self-employed income estimate (adjust to your bracket)
TAX_RATE_INCOME = 0.30   # ~30% combined income tax + solidarity surcharge
VAT_RATE = 0.19          # 19% German Umsatzsteuer

# ─── Invoice / Offer Generator — Business Info ──────────────────────────────
BIZ_INFO = {
    'name': 'Josef Sindelka',
    'street': 'Planckstraße 13',
    'city': '22765 Hamburg',
    'phone': '+420 777 603 301',
    'email': 'hello@josefsindelka.com',
    'website': 'www.josefsindelka.com',
    'ust_id': 'DE348386373',
    'steuernummer': '42/231/04652',
    'bank': 'N26',
    'iban': 'DE97 1001 1001 2627 4924 55',
    'bic': 'NTSBDEB1XXX',
}

INCOME_CATEGORIES = ['Photography', 'Animation', 'Video Production', 'AI Studio']

SEED_CLIENTS = [
    {'id': 'K10002', 'name': 'NSR Bartu Academy', 'address': 'Schulhausstrasse 35, 8706 Meilen, Swasiland', 'notes': 'Video post-production. Tax-exempt: §4 Nr. 1a UStG (export to third country). VAT 0%.', 'country': 'Switzerland/Swaziland'},
    {'id': 'K10005', 'name': 'KundenbüroHH GmbH & Co.', 'address': 'Planckstr. 13, 22765 Hamburg', 'notes': 'Agency intermediary for Wempe projects.', 'country': 'Germany'},
    {'id': 'K10008', 'name': 'Gerresheim serviert GmbH & Co. KG', 'address': 'Australiastraße 52B, 20457 Hamburg', 'notes': 'Event photography and animation/onboarding videos.', 'country': 'Germany'},
    {'id': 'K10009', 'name': 'Forward Thinking Tech GmbH c/o Kanzlei ASG', 'address': 'Am Sandtorkai 76, 20457 Hamburg', 'notes': 'Portrait and group photography.', 'country': 'Germany'},
    {'id': 'K10012', 'name': 'Nüssli (Schweiz) AG', 'address': 'Hauptstrasse 36, 8536 Hüttwilen, Schweiz', 'notes': 'Swiss event construction. Video reels.', 'country': 'Switzerland'},
    {'id': 'K10013', 'name': 'ATELIER BRÜCKNER GmbH', 'address': 'Krefelder Straße 32, 70376 Stuttgart', 'notes': 'Exhibition design. Expo 2025 Osaka.', 'country': 'Germany'},
    {'id': 'K10017', 'name': 'Tom Heinemann / Time of Motion', 'address': 'Planckstraße 13, 22765 Hamburg', 'notes': 'Intermediary for BAT, Wempe, EDEKA, Berlitz, Montblanc projects.', 'country': 'Germany'},
    {'id': 'K10020', 'name': 'Pinck & Herbold Treuhand GmbH & Co. KG', 'address': 'Langenstücken 34, 22393 Hamburg', 'notes': 'Real estate flat photography.', 'country': 'Germany'},
    {'id': 'K10021', 'name': 'notonly', 'address': 'Greflingerstrasse 7, 22299 Hamburg', 'notes': 'Event visuals — video loops for events.', 'country': 'Germany'},
    {'id': 'K10022', 'name': 'Claybird Inc.', 'address': '2595 Canyon Blvd, Suite 340, Boulder, CO 80302 USA', 'notes': 'US client. AI Studio photo generation.', 'country': 'USA'},
]


# ─── Activity Log ────────────────────────────────────────────────────────────

def _log_activity(action, details=''):
    """Append an entry to the in-session activity log and persist to Google Sheets."""
    entry = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'action': action,
        'details': details,
    }
    if 'activity_log' not in st.session_state:
        st.session_state['activity_log'] = []
    st.session_state['activity_log'].insert(0, entry)
    # Keep only last 200 entries in session
    st.session_state['activity_log'] = st.session_state['activity_log'][:200]
    # Persist to Google Sheets (best-effort, non-blocking)
    try:
        sh = _gsheet()
        try:
            ws_log = sh.worksheet('Log')
        except Exception:
            ws_log = sh.add_worksheet(title='Log', rows=500, cols=3)
            ws_log.update('A1:C1', [['Timestamp', 'Action', 'Details']])
        ws_log.append_row(
            [entry['timestamp'], entry['action'], entry['details']],
            value_input_option='RAW',
        )
    except Exception:
        pass  # Log persistence is best-effort


def _load_activity_log():
    """Load recent activity log entries from Google Sheets."""
    if 'activity_log_loaded' in st.session_state:
        return st.session_state.get('activity_log', [])
    try:
        ws_log = _gsheet().worksheet('Log')
        rows = ws_log.get_all_values()
        entries = []
        for row in reversed(rows[1:]):  # skip header, newest first
            if len(row) >= 3:
                entries.append({
                    'timestamp': row[0],
                    'action': row[1],
                    'details': row[2],
                })
        st.session_state['activity_log'] = entries[:200]
        st.session_state['activity_log_loaded'] = True
    except Exception:
        st.session_state['activity_log'] = st.session_state.get('activity_log', [])
        st.session_state['activity_log_loaded'] = True
    return st.session_state['activity_log']


# ─── Google API Helpers ──────────────────────────────────────────────────────

def _get_google_creds():
    """Load Google OAuth credentials from Streamlit Secrets or local token.json."""
    _default_scopes = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets',
    ]

    # ── Streamlit Cloud: read credentials from st.secrets ──
    if 'google_credentials' in st.secrets:
        sec = st.secrets['google_credentials']
        # Manual token exchange to guarantee a fresh access token
        import urllib.request, urllib.parse
        token_data = urllib.parse.urlencode({
            'client_id': str(sec['client_id']),
            'client_secret': str(sec['client_secret']),
            'refresh_token': str(sec['refresh_token']),
            'grant_type': 'refresh_token',
        }).encode()
        req = urllib.request.Request(str(sec['token_uri']), data=token_data)
        with urllib.request.urlopen(req, timeout=10) as resp:
            token_resp = json.loads(resp.read())
        creds = Credentials(
            token=token_resp['access_token'],
            refresh_token=str(sec['refresh_token']),
            token_uri=str(sec['token_uri']),
            client_id=str(sec['client_id']),
            client_secret=str(sec['client_secret']),
            scopes=_default_scopes,
        )
        return creds

    # ── Local dev: read from token.json ──
    with open(_TOKEN_FILE) as f:
        td = json.load(f)
    creds = Credentials(
        token=td['token'], refresh_token=td['refresh_token'],
        token_uri=td['token_uri'], client_id=td['client_id'],
        client_secret=td['client_secret'],
        scopes=td.get('scopes', _default_scopes),
    )
    if creds.expired or not creds.valid:
        creds.refresh(Request())
        td['token'] = creds.token
        with open(_TOKEN_FILE, 'w') as f:
            json.dump(td, f)
    return creds


@st.cache_resource(ttl=2400)
def _get_gspread_client():
    creds = _get_google_creds()
    if 'google_credentials' in st.secrets:
        # Cloud: build client with explicit auth header to avoid
        # AuthorizedSession not attaching the token on Python 3.13
        import requests as _req
        from gspread import Client
        session = _req.Session()
        session.headers['Authorization'] = f'Bearer {creds.token}'
        return Client(auth=creds, session=session)
    return gspread.authorize(creds)


@st.cache_resource(ttl=2400)
def _get_drive_service():
    return build('drive', 'v3', credentials=_get_google_creds())


@st.cache_resource(ttl=300)
def _gsheet():
    """Return the gspread Spreadsheet object (cached 5 min)."""
    return _get_gspread_client().open_by_key(SHEET_ID)


@st.cache_data(ttl=300)
def _drive_find_folder(parent_id, name):
    """Find a folder by name under parent. Returns folder ID or None."""
    drive = _get_drive_service()
    q = f"'{parent_id}' in parents and name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res = drive.files().list(q=q, fields='files(id)').execute()
    files = res.get('files', [])
    return files[0]['id'] if files else None


def _drive_get_or_create_folder(parent_id, name):
    """Find or create a folder by name under parent."""
    fid = _drive_find_folder.__wrapped__(parent_id, name)  # bypass cache
    if fid:
        return fid
    drive = _get_drive_service()
    meta = {'name': name, 'parents': [parent_id], 'mimeType': 'application/vnd.google-apps.folder'}
    f = drive.files().create(body=meta, fields='id').execute()
    return f['id']


def _drive_list_files(folder_id, name_contains=None):
    """List files in a Drive folder. Returns list of {id, name, mimeType}."""
    drive = _get_drive_service()
    q = f"'{folder_id}' in parents and trashed=false"
    if name_contains:
        q += f" and name contains '{name_contains}'"
    res = drive.files().list(q=q, fields='files(id,name,mimeType)', pageSize=500).execute()
    return res.get('files', [])


def _drive_upload_bytes(folder_id, filename, data_bytes, mimetype='application/pdf'):
    """Upload bytes to a Drive folder. Returns file dict {id, name}."""
    drive = _get_drive_service()
    media = MediaIoBaseUpload(io.BytesIO(data_bytes), mimetype=mimetype, resumable=True)
    meta = {'name': filename, 'parents': [folder_id]}
    return drive.files().create(body=meta, media_body=media, fields='id,name').execute()


def _drive_delete_file(file_id):
    """Delete a file from Google Drive."""
    _get_drive_service().files().delete(fileId=file_id).execute()


def _drive_rename_file(file_id, new_name, new_parent_id=None):
    """Rename/move a file on Google Drive."""
    drive = _get_drive_service()
    body = {'name': new_name}
    kwargs = {'fileId': file_id, 'body': body, 'fields': 'id,name,parents'}
    if new_parent_id:
        current = drive.files().get(fileId=file_id, fields='parents').execute()
        old_parents = ','.join(current.get('parents', []))
        kwargs['addParents'] = new_parent_id
        kwargs['removeParents'] = old_parents
    return drive.files().update(**kwargs).execute()


def _drive_download_bytes(file_id):
    """Download file content from Drive as bytes."""
    drive = _get_drive_service()
    return drive.files().get_media(fileId=file_id).execute()


def _get_year_folder():
    """Get the Drive folder ID for the current year (e.g. '2026'). Cached."""
    return _drive_find_folder(DRIVE_ROOT_FOLDER, YEAR_FOLDER)


def _get_cost_subfolders(month_folder_id, month_name):
    """Return list of cost subfolder IDs to scan for a given month.
    Jan/Feb use legacy 03_Regular Cost + 04_Irregular Cost.
    March onward uses the single 'Costs' subfolder.
    """
    # Check if 'Costs' subfolder exists (new structure)
    costs_id = _drive_find_folder(month_folder_id, _NEW_COST_SUBFOLDER)
    if costs_id:
        return [(_NEW_COST_SUBFOLDER, costs_id)]
    # Fall back to legacy subfolders (Jan/Feb)
    result = []
    for name in _LEGACY_COST_SUBFOLDERS:
        fid = _drive_find_folder(month_folder_id, name)
        if fid:
            result.append((name, fid))
    return result


# ─── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="JS() Finance 2026",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─── Custom CSS (theme-aware) ────────────────────────────────────────────────
def _inject_css():
    """Inject theme-aware CSS. Called once per render cycle."""
    t = _t()
    st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* ── Base ── */
    .stApp {{
        background-color: {t['bg']};
        font-family: {FONT};
        min-height: 100vh;
    }}
    .stApp > header {{
        background-color: {t['bg']};
    }}

    .stMainBlockContainer {{
        max-width: 1400px;
        padding: 1.5rem 2.5rem 3rem 2.5rem;
    }}

    /* ── Date Header ── */
    .js-date {{
        font-family: {FONT};
        font-size: 0.7rem;
        color: {t['text_secondary']};
        letter-spacing: 0.08em;
        font-weight: 400;
    }}

    /* ── Tabs (pill style) ── */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 4px;
        background: {t['surface']};
        border: 1px solid {t['border']};
        border-radius: 12px;
        padding: 4px;
        border-bottom: none;
    }}
    .stTabs [data-baseweb="tab"] {{
        background: transparent;
        border: none;
        border-radius: 8px;
        padding: 0.45rem 1.3rem;
        color: {t['text_secondary']};
        font-family: {FONT};
        font-size: 0.7rem;
        font-weight: 500;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        transition: all 0.25s ease;
        white-space: nowrap;
        height: auto;
    }}
    .stTabs [data-baseweb="tab"]:hover {{
        color: {t['text']};
        background: {t['surface2']};
    }}
    .stTabs [aria-selected="true"] {{
        background: linear-gradient(135deg, {C_ORANGE} 0%, {C_ORANGE_LIGHT} 100%) !important;
        border-color: transparent !important;
        color: #fff !important;
        font-weight: 600;
        box-shadow: 0 2px 8px rgba(232,93,38,0.25), inset 0 1px 0 rgba(255,255,255,0.15);
    }}
    .stTabs [data-baseweb="tab-highlight"] {{
        display: none;
    }}
    .stTabs [data-baseweb="tab-border"] {{
        display: none;
    }}

    /* ── Cards ── */
    .card {{
        background: {t['surface']};
        border: 1px solid {t['border']};
        border-radius: 16px;
        padding: 1.25rem 1.5rem;
        box-shadow: {t['card_shadow']};
        transition: box-shadow 0.2s ease, border-color 0.2s ease;
        margin-bottom: 0.5rem;
    }}
    .card:hover {{
        box-shadow: {t['card_shadow_hover']};
        border-color: {t['border_hover']};
    }}
    .card-label {{
        font-size: 0.55rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: {t['text_secondary']};
        margin-bottom: 0.75rem;
        font-family: {FONT};
        font-weight: 500;
        line-height: 1.4;
    }}
    .card-value {{
        font-size: clamp(0.95rem, 2.5vw, 1.75rem);
        font-weight: 600;
        font-family: {FONT};
        letter-spacing: -0.03em;
        color: {t['text']};
        white-space: nowrap;
    }}
    .card-sub {{
        font-size: 0.7rem;
        color: {t['muted']};
        margin-top: 0.5rem;
        font-family: {FONT};
        font-weight: 400;
    }}
    .positive {{ color: {C_GREEN}; }}
    .negative {{ color: {C_RED}; }}
    .accent {{ color: {C_ORANGE}; }}
    .blue {{ color: {C_BLUE}; }}

    /* ── Chart Containers ── */
    .chart-card {{
        background: {t['surface']};
        border: 1px solid {t['border']};
        border-radius: 16px;
        padding: 1.75rem;
        margin-bottom: 1.5rem;
        box-shadow: {t['card_shadow']};
    }}
    .chart-title {{
        font-size: 0.6rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: {t['text_secondary']};
        margin-bottom: 1.25rem;
        font-family: {FONT};
        font-weight: 500;
    }}

    /* ── Data Tables ── */
    .data-table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 0.82rem;
        font-family: {FONT};
    }}
    .data-table th {{
        text-align: left;
        padding: 0.85rem 1rem;
        font-size: 0.55rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: {t['text_secondary']};
        border-bottom: 1px solid {t['border']};
        font-weight: 500;
    }}
    .data-table td {{
        padding: 0.85rem 1rem;
        border-bottom: 1px solid {t['row_border']};
        color: {t['text']};
    }}
    .data-table tr:hover td {{
        background: {t['row_hover']};
    }}
    .data-table .num {{
        font-family: {FONT};
        text-align: right;
        font-size: 0.8rem;
        font-weight: 500;
        letter-spacing: -0.02em;
        font-variant-numeric: tabular-nums;
    }}
    .data-table .total-row td {{
        background: {t['surface2']};
        font-weight: 600;
        border-top: 1px solid {t['border']};
    }}

    /* ── Badges ── */
    .badge {{
        display: inline-block;
        padding: 0.2rem 0.55rem;
        border-radius: 6px;
        font-size: 0.6rem;
        font-weight: 500;
        letter-spacing: 0.03em;
        font-family: {FONT};
    }}
    .badge-paid {{ color: {C_GREEN}; background: rgba(34,197,94,0.12); }}
    .badge-sent {{ color: {C_ORANGE}; background: rgba(232,93,38,0.12); }}
    .badge-draft {{ color: {t['muted']}; background: {t['surface3']}; }}

    /* ── Filter Pills (horizontal radio in dashboard) ── */
    [data-testid="stRadio"] > div[role="radiogroup"] {{
        gap: 0.4rem;
        flex-wrap: wrap;
    }}
    [data-testid="stRadio"] > div[role="radiogroup"] > label {{
        background: {t['surface']} !important;
        border: 1px solid {t['border']} !important;
        border-radius: 20px !important;
        padding: 0.35rem 1rem !important;
        font-size: 0.78rem !important;
        font-weight: 500;
        font-family: {FONT};
        color: {t['text_secondary']} !important;
        transition: all 0.2s ease;
        cursor: pointer;
    }}
    [data-testid="stRadio"] > div[role="radiogroup"] > label > div:last-child {{
        color: {t['text_secondary']} !important;
    }}
    [data-testid="stRadio"] > div[role="radiogroup"] > label:hover {{
        border-color: {t['border_hover']} !important;
        color: {t['text']} !important;
    }}
    [data-testid="stRadio"] > div[role="radiogroup"] > label:hover > div:last-child {{
        color: {t['text']} !important;
    }}
    [data-testid="stRadio"] > div[role="radiogroup"] > label[data-checked="true"],
    [data-testid="stRadio"] > div[role="radiogroup"] > label:has(input:checked) {{
        background: {t['text']} !important;
        color: {t['surface']} !important;
        border-color: {t['text']} !important;
        font-weight: 600;
    }}
    [data-testid="stRadio"] > div[role="radiogroup"] > label[data-checked="true"] > div:last-child,
    [data-testid="stRadio"] > div[role="radiogroup"] > label:has(input:checked) > div:last-child {{
        color: {t['surface']} !important;
    }}
    [data-testid="stRadio"] > div[role="radiogroup"] > label > div:first-child {{
        display: none !important;  /* hide default radio circle */
    }}

    /* ── Dashboard row action buttons ── */
    [data-testid="stHorizontalBlock"] .stButton > button {{
        min-height: 0;
        padding: 0.15rem 0.5rem;
        font-size: 0.75rem;
    }}

    /* ── Container borders (form cards) ── */
    [data-testid="stVerticalBlockBorderWrapper"] {{
        border-radius: 16px !important;
        border-color: {t['border']} !important;
    }}

    /* ── Progress Bar ── */
    .progress-bar-bg {{
        width: 100%;
        height: 10px;
        background: {t['surface3']};
        border: 1px solid {t['border']};
        border-radius: 999px;
        overflow: hidden;
        position: relative;
    }}
    .progress-bar-fill {{
        height: 100%;
        border-radius: 999px;
        background: {C_ORANGE};
        font-size: 0;
        transition: width 1.5s ease;
    }}
    .goal-milestones {{
        display: flex;
        justify-content: space-between;
        margin-top: 0.6rem;
        font-size: 0.58rem;
        color: {t['muted']};
        font-family: {FONT};
        letter-spacing: 0.03em;
        font-weight: 400;
    }}

    /* ── Month Grid ── */
    .month-grid {{
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 0.75rem;
    }}
    .month-grid-2 {{
        display: grid;
        grid-template-columns: repeat(2, 1fr);
        gap: 0.75rem;
    }}
    .month-card {{
        background: {t['surface']};
        border: 1px solid {t['border']};
        border-radius: 12px;
        padding: 0.75rem 0.5rem;
        text-align: center;
        box-shadow: {t['card_shadow']};
        transition: border-color 0.25s ease;
    }}
    .month-card:hover {{
        border-color: {t['border_hover']};
    }}
    .month-card .m-label {{
        font-size: 0.55rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: {t['text_secondary']};
        margin-bottom: 0.4rem;
        font-family: {FONT};
        font-weight: 500;
    }}
    .month-card .m-value {{
        font-family: {FONT};
        font-size: 0.85rem;
        font-weight: 600;
        letter-spacing: -0.02em;
        white-space: nowrap;
    }}

    /* ── Summary Row ── */
    .summary-row {{
        display: flex;
        justify-content: space-between;
        padding: 0.85rem 0;
        border-bottom: 1px solid {t['row_border']};
        font-size: 0.82rem;
        font-family: {FONT};
    }}
    .summary-row:last-child {{ border-bottom: none; }}
    .summary-row .s-label {{ color: {t['text_secondary']}; font-weight: 400; }}
    .summary-row .s-value {{ font-weight: 600; color: {t['text']}; }}

    /* ── Footer ── */
    .js-footer {{
        text-align: center;
        font-size: 0.55rem;
        color: {t['muted']};
        letter-spacing: 0.12em;
        text-transform: uppercase;
        padding: 3rem 0 1.5rem 0;
        border-top: 1px solid {t['border']};
        margin-top: 3rem;
        font-family: {FONT};
        font-weight: 400;
    }}

    /* ── Section Headers ── */
    .section-hdr {{
        font-size: 0.6rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: {t['text_secondary']};
        margin: 2rem 0 1.25rem 0;
        font-family: {FONT};
        font-weight: 500;
    }}

    /* ── Widget Labels & Text Visibility ── */
    .stSelectbox label,
    .stTextInput label,
    .stNumberInput label,
    .stDateInput label,
    .stTextArea label,
    .stRadio label,
    .stCheckbox label,
    .stFileUploader label,
    .stMultiSelect label,
    .stSlider label,
    .stColorPicker label {{
        color: {t['text']} !important;
        font-family: {FONT};
        font-weight: 500;
        font-size: 0.82rem;
    }}
    .stSelectbox label p,
    .stTextInput label p,
    .stNumberInput label p,
    .stDateInput label p,
    .stTextArea label p,
    .stFileUploader label p {{
        color: {t['text']} !important;
    }}

    /* Streamlit markdown text */
    .stMarkdown, .stMarkdown p {{
        color: {t['text']};
        font-family: {FONT};
    }}
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4 {{
        color: {t['text']} !important;
        font-family: {FONT};
    }}

    /* Streamlit headings / subheaders */
    [data-testid="stHeading"] {{
        color: {t['text']} !important;
    }}
    [data-testid="stHeading"] h1,
    [data-testid="stHeading"] h2,
    [data-testid="stHeading"] h3 {{
        color: {t['text']} !important;
        font-family: {FONT};
    }}

    /* Streamlit caption / help text */
    .stCaption, .stCaption p {{
        color: {t['text_secondary']} !important;
    }}

    /* Tab content text */
    .stTabs [data-baseweb="tab-panel"] {{
        color: {t['text']};
    }}

    /* Override Streamlit defaults */
    #MainMenu {{visibility: hidden;}}
    header[data-testid="stHeader"] {{background: {t['bg']}; }}
    .stDeployButton, .stAppDeployButton {{display: none;}}

    div[data-testid="stVerticalBlock"] > div {{
        gap: 0.5rem;
    }}

    .stDataFrame {{
        border-radius: 16px;
        overflow: hidden;
    }}

    /* ── Buttons ── */
    .stButton > button {{
        border-radius: 10px;
        border: 1px solid {t['border']};
        background: {t['surface']};
        color: {t['text_secondary']};
        font-family: {FONT};
        font-weight: 500;
        font-size: 0.7rem;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        padding: 0.45rem 1.2rem;
        transition: all 0.25s ease;
        white-space: nowrap;
    }}
    .stButton > button:hover {{
        background: {t['surface2']};
        color: {t['text']};
        border-color: {t['border_hover']};
    }}
    .stButton > button[kind="primary"] {{
        background: linear-gradient(135deg, {C_ORANGE} 0%, {C_ORANGE_LIGHT} 100%);
        border: 1px solid rgba(255,255,255,0.18);
        color: #fff;
        font-weight: 600;
        box-shadow: 0 2px 8px rgba(232,93,38,0.25), inset 0 1px 0 rgba(255,255,255,0.2);
        backdrop-filter: blur(4px);
    }}
    .stButton > button[kind="primary"]:hover {{
        background: linear-gradient(135deg, {C_ORANGE_LIGHT} 0%, {C_ORANGE} 100%);
        border-color: rgba(255,255,255,0.25);
        box-shadow: 0 4px 14px rgba(232,93,38,0.35), inset 0 1px 0 rgba(255,255,255,0.25);
    }}

    /* ── Transaction rows ── */
    .tx-row {{
        display: flex;
        align-items: center;
        padding: 0.7rem 1rem;
        border-bottom: 1px solid {t['row_border']};
        font-size: 0.82rem;
        color: {t['text']};
        font-family: {FONT};
    }}
    .tx-row:hover {{
        background: {t['row_hover']};
    }}
    .tx-header {{
        display: flex;
        align-items: center;
        padding: 0.85rem 1rem;
        border-bottom: 1px solid {t['border']};
        font-size: 0.55rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: {t['text_secondary']};
        font-weight: 500;
        font-family: {FONT};
    }}
    .tx-actions .stButton > button {{
        padding: 0.25rem 0.6rem;
        font-size: 0.62rem;
        min-height: 0;
        line-height: 1;
        border-radius: 8px;
    }}
    .tx-del .stButton > button {{
        border-color: rgba(248,113,113,0.2);
        color: {C_RED};
    }}
    .tx-del .stButton > button:hover {{
        background: rgba(248,113,113,0.08);
        color: {C_RED};
        border-color: rgba(248,113,113,0.35);
    }}

    /* ── Theme toggle button ── */
    .theme-toggle .stButton > button {{
        border-radius: 999px;
        padding: 0.3rem 0.6rem;
        font-size: 1.1rem;
        min-height: 0;
        line-height: 1;
        background: {t['surface2']};
        border: 1px solid {t['border']};
        text-transform: none;
        letter-spacing: 0;
    }}
    .theme-toggle .stButton > button:hover {{
        background: {t['surface3']};
    }}

    /* ── Dialog ── */
    div[data-testid="stModal"] > div {{
        background: {t['surface']};
        border: 1px solid {t['border']};
        border-radius: 16px;
    }}

    /* ── Inputs ── */
    .stSelectbox > div > div,
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stDateInput > div > div > input {{
        background: {t['surface2']};
        border-color: {t['border']};
        border-radius: 10px;
        color: {t['text']};
        font-family: {FONT};
    }}

    /* ── Text area ── */
    .stTextArea textarea {{
        background: {t['surface2']};
        border-color: {t['border']};
        border-radius: 10px;
        color: {t['text']};
        font-family: {FONT};
    }}

    /* ── Expander ── */
    .streamlit-expanderHeader {{
        background: {t['surface']};
        border-radius: 12px;
        border: 1px solid {t['border']};
        color: {t['text']};
    }}

    /* ── Plotly ── */
    .js-plotly-plot .plotly .main-svg {{
        background: transparent !important;
    }}

    /* ── Mobile Responsive (iPhone 16/17 Pro = 393px) ── */
    @media (max-width: 480px) {{
        .stMainBlockContainer {{
            padding: 0.5rem 0.75rem;
        }}
        .card {{
            padding: 14px 16px;
            border-radius: 12px;
        }}
        .card-value {{
            font-size: 1.3rem;
        }}
        .stTabs [data-baseweb="tab-list"] {{
            overflow-x: auto;
            flex-wrap: nowrap;
        }}
        .stTabs [data-baseweb="tab"] {{
            font-size: 0.65rem;
            padding: 6px 12px;
            white-space: nowrap;
        }}
        .chart-card {{
            padding: 14px;
            border-radius: 12px;
        }}
        .data-table {{
            font-size: 0.75rem;
        }}
        .data-table th, .data-table td {{
            padding: 0.6rem 0.5rem;
        }}
        .month-grid {{
            grid-template-columns: repeat(2, 1fr);
        }}
        [data-testid="column"] {{
            min-width: 100% !important;
        }}
    }}
</style>
""", unsafe_allow_html=True)

_inject_css()


# ─── Data Loading ────────────────────────────────────────────────────────────

def _invalidate_data_caches():
    """Clear only the data-related caches after a mutation (save/delete/edit).
    Preserves long-lived caches like _drive_find_folder and get_exchange_rate."""
    load_data.clear()
    _auto_scan_changes.clear()
    _load_offers.clear()
    _load_document_meta.clear()

@st.cache_data(ttl=30)
def load_data():
    """Load all data from Google Sheets with 30s cache."""
    try:
        sh = _gsheet()
    except Exception as e:
        st.error(f"Cannot connect to Google Sheet: {type(e).__name__}: {e}")
        st.stop()

    data = {}

    # 1. EXPENSES — clean tabular structure
    ws_exp = sh.worksheet('Expenses')
    exp_records = ws_exp.get_all_records()
    expenses = pd.DataFrame(exp_records)
    expenses['Netto (€)'] = pd.to_numeric(
        expenses.get('Netto (€)', pd.Series(dtype=float)).apply(_clean_currency), errors='coerce'
    ).fillna(0)
    expenses['Brutto (€)'] = pd.to_numeric(
        expenses.get('Brutto (€)', pd.Series(dtype=float)).apply(_clean_currency), errors='coerce'
    ).fillna(0)
    if 'Date of Payment' in expenses.columns:
        expenses['Date of Payment'] = pd.to_datetime(expenses['Date of Payment'], dayfirst=True, errors='coerce')
    data['expenses'] = expenses

    # 2. INCOME — complex structure (paid + unpaid sections)
    ws_inc = sh.worksheet('Income')
    inc_vals = ws_inc.get_all_values()
    if inc_vals:
        inc_headers = inc_vals[0]
        inc_df = pd.DataFrame(inc_vals[1:], columns=inc_headers)
        # Parse Date column
        if 'Date' in inc_df.columns:
            inc_df['Date'] = pd.to_datetime(inc_df['Date'], dayfirst=True, errors='coerce')
        data['income_paid'] = _parse_income_section(inc_df, 0)
        data['income_unpaid'] = _parse_income_section(inc_df, 1)
        # Clean currency-formatted strings in Netto/Brutto columns
        for key in ('income_paid', 'income_unpaid'):
            df_inc = data[key]
            if len(df_inc):
                for col in ('Netto (€)', 'Brutto (€)'):
                    if col in df_inc.columns:
                        df_inc[col] = pd.to_numeric(
                            df_inc[col].apply(_clean_currency), errors='coerce'
                        ).fillna(0)
    else:
        data['income_paid'] = pd.DataFrame()
        data['income_unpaid'] = pd.DataFrame()

    # 3. OVERVIEW — Compute from raw data
    exp_by_month = expenses.groupby('Month')['Netto (€)'].sum()
    paid = data['income_paid']
    inc_by_month = pd.Series(dtype=float)
    if len(paid) and 'Month' in paid.columns:
        inc_by_month = paid['Netto (€)'].groupby(paid['Month']).sum()
    overview_rows = []
    for m in MONTHS:
        inc = inc_by_month.get(m, 0.0)
        exp = exp_by_month.get(m, 0.0)
        overview_rows.append({'Month': m, 'Income': inc, 'Expenses': exp, 'Profit_Loss': inc - exp})
    data['overview'] = pd.DataFrame(overview_rows)

    # 4. GOAL TRACKER
    try:
        ws_goal = sh.worksheet('Goal Tracker')
        goal_vals = ws_goal.get_all_values()
        data['goal_raw'] = pd.DataFrame(goal_vals) if goal_vals else None
    except Exception:
        data['goal_raw'] = None

    # 5. 2025 reference data
    try:
        ws_2025 = sh.worksheet('2025')
        vals_2025 = ws_2025.get_all_values()
        data['hist_2025'] = pd.DataFrame(vals_2025) if vals_2025 else None
    except Exception:
        data['hist_2025'] = None

    return data


def _clean_currency(val):
    """Strip currency symbols and thousand separators from a string value."""
    if isinstance(val, str):
        return val.replace('€', '').replace(',', '').strip()
    return val


def _parse_income_section(df, group_index):
    """Extract paid (group 0) or unpaid (group 1) invoices from the Income sheet."""
    df = df.copy()
    netto_col = df.get('Netto (€)', pd.Series(dtype=float))
    df['_netto'] = pd.to_numeric(netto_col.apply(_clean_currency), errors='coerce')

    groups, current = [], []
    for i, valid in df['_netto'].notna().items():
        if valid:
            current.append(i)
        elif current:
            groups.append(current)
            current = []
    if current:
        groups.append(current)

    if group_index >= len(groups):
        return pd.DataFrame()

    result = df.loc[groups[group_index]].copy()

    for col in result.columns:
        if col == '_netto':
            continue
        mask = result[col].astype(str).str.contains('Total|TOTAL', case=False, na=False)
        result = result[~mask]

    if 'Client' in result.columns:
        result = result[result['Client'].notna() & (result['Client'].astype(str).str.strip() != '')]

    return result.drop(columns=['_netto'], errors='ignore')


def _parse_goal(goal_df):
    """Extract goal, acquired, to-go from the Goal Tracker sheet."""
    info = {'goal': 120_000, 'acquired': 0, 'to_go': 120_000, 'pct': 0}
    if goal_df is None:
        return info

    for i in range(len(goal_df)):
        cell = str(goal_df.iloc[i, 0]).strip().upper() if pd.notna(goal_df.iloc[i, 0]) else ''
        for j in range(1, goal_df.shape[1]):
            val = pd.to_numeric(_clean_currency(goal_df.iloc[i, j]), errors='coerce')
            if pd.notna(val) and val > 0:
                if cell == 'GOAL':
                    info['goal'] = val
                elif 'ACQUIRED' in cell:
                    info['acquired'] = val
                elif 'TO GO' in cell:
                    info['to_go'] = val
                break

    if info['goal'] > 0 and info['acquired'] > 0:
        info['pct'] = (info['acquired'] / info['goal']) * 100
    return info


def _parse_2025_monthly(hist_df):
    """Extract monthly income totals from the 2025 sheet."""
    if hist_df is None:
        return {}

    lookup = {m.lower(): m for m in MONTHS}
    result = {}

    for i in range(len(hist_df)):
        cell = str(hist_df.iloc[i, 0]).strip().lower() if pd.notna(hist_df.iloc[i, 0]) else ''
        if cell in lookup:
            for j in range(1, hist_df.shape[1]):
                val = pd.to_numeric(_clean_currency(hist_df.iloc[i, j]), errors='coerce')
                if pd.notna(val) and val > 0:
                    result[lookup[cell]] = val
                    break
    return result


# ─── UI Helpers ──────────────────────────────────────────────────────────────

def chart_layout(fig, height=400, **kw):
    """Apply theme-aware styling to a Plotly figure."""
    t = _t()
    fig.update_layout(
        height=height,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=t['muted'], size=12, family=FONT),
        margin=dict(l=40, r=20, t=40, b=40),
        legend=dict(
            bgcolor='rgba(0,0,0,0)', bordercolor='rgba(0,0,0,0)',
            font=dict(color=t['muted'], size=11, family=FONT),
        ),
        xaxis=dict(gridcolor=t['chart_grid'], zerolinecolor=t['chart_zeroline']),
        yaxis=dict(gridcolor=t['chart_grid'], zerolinecolor=t['chart_zeroline']),
        **kw,
    )
    return fig


def metric_card(label, value, sub=None, color_class=''):
    """Render a styled metric card."""
    if isinstance(value, float):
        val_str = f"\u20ac{value:,.2f}"
    elif isinstance(value, int):
        val_str = f"\u20ac{value:,}"
    else:
        val_str = str(value)

    sub_html = f'<div class="card-sub">{sub}</div>' if sub else ''
    st.markdown(f"""
    <div class="card">
        <div class="card-label">{label}</div>
        <div class="card-value {color_class}">{val_str}</div>
        {sub_html}
    </div>
    """, unsafe_allow_html=True)


def gauge_card(label, value, max_val, fmt_value=None, sub=None, color=C_ORANGE):
    """Render a circular gauge KPI card (SVG-based)."""
    t = _t()
    pct = min(value / max_val, 1.0) if max_val > 0 else 0
    circumference = 2 * 3.14159 * 54  # radius=54
    offset = circumference * (1 - pct)
    display = fmt_value or (f"\u20ac{value:,.0f}" if isinstance(value, (int, float)) else str(value))

    st.markdown(f"""
    <div class="card" style="text-align:center;">
        <svg width="130" height="130" viewBox="0 0 120 120" style="margin:0 auto;display:block;">
            <circle cx="60" cy="60" r="54" fill="none" stroke="{t['surface3']}" stroke-width="10"/>
            <circle cx="60" cy="60" r="54" fill="none" stroke="{color}" stroke-width="10"
                stroke-dasharray="{circumference:.1f}" stroke-dashoffset="{offset:.1f}"
                stroke-linecap="round" transform="rotate(-90 60 60)"
                style="transition: stroke-dashoffset 0.6s ease;"/>
            <text x="60" y="56" text-anchor="middle" fill="{t['text']}"
                font-size="18" font-weight="700" font-family="Inter">{display}</text>
            <text x="60" y="74" text-anchor="middle" fill="{t['muted']}"
                font-size="11" font-family="Inter">{int(pct*100)}%</text>
        </svg>
        <div class="card-label" style="margin-top:8px;">{label}</div>
        {'<div class="card-sub">'+sub+'</div>' if sub else ''}
    </div>""", unsafe_allow_html=True)


def fmt_eur(v):
    """Format a number as Euro currency."""
    if v < 0:
        return f"\u2212\u20ac{abs(v):,.2f}"
    return f"\u20ac{v:,.2f}"


def badge_html(text, category=None):
    """Generate an inline badge span for a category."""
    t = _t()
    colors = BADGE_STYLES.get(category or text, (t['muted'], t['surface3']))
    return (f'<span class="badge" style="color:{colors[0]};background:{colors[1]}">'
            f'{text}</span>')


def html_table(headers, rows, num_cols=None):
    """Generate an HTML table matching the design's data-table class."""
    num_cols = set(num_cols or [])
    html = '<table class="data-table"><thead><tr>'
    for i, h in enumerate(headers):
        cls = ' class="num"' if i in num_cols else ''
        html += f'<th{cls}>{h}</th>'
    html += '</tr></thead><tbody>'
    for row in rows:
        is_total = row.get('_total', False)
        cls = ' class="total-row"' if is_total else ''
        html += f'<tr{cls}>'
        for i, (_, v) in enumerate([(k, v) for k, v in row.items() if k != '_total']):
            td_cls = ' class="num"' if i in num_cols else ''
            if is_total:
                html += f'<td{td_cls}><strong>{v}</strong></td>'
            else:
                html += f'<td{td_cls}>{v}</td>'
        html += '</tr>'
    html += '</tbody></table>'
    return html


def chart_card_html(title, content_html):
    """Render a chart-card with title and HTML content in a single markdown block."""
    st.markdown(f"""
    <div class="chart-card">
        <div class="chart-title">{title}</div>
        {content_html}
    </div>
    """, unsafe_allow_html=True)


def section_title(title):
    """Render a section title for chart areas (no wrapping card)."""
    st.markdown(f'<div class="chart-title">{title}</div>', unsafe_allow_html=True)


# ─── TAB 1 — Overview ───────────────────────────────────────────────────────

def tab_overview(data):
    overview = data['overview']
    active = overview[(overview['Income'] > 0) | (overview['Expenses'] > 0)]

    if active.empty:
        st.info("No data available yet.")
        return

    total_income = active['Income'].sum()
    total_expenses = active['Expenses'].sum()
    net_pl = total_income - total_expenses
    active_months = active['Month'].tolist()
    month_range = f"{active_months[0][:3]} \u2013 {active_months[-1][:3]} 2026" if len(active_months) > 1 else f"{active_months[0]} 2026"

    # --- KPI Cards ---
    k1, k2, k3 = st.columns(3)
    with k1:
        metric_card('Total Income', total_income, sub=month_range, color_class='accent')
    with k2:
        metric_card('Total Expenses', total_expenses, sub=month_range)
    with k3:
        pl_class = 'negative' if net_pl < 0 else 'positive'
        st.markdown(f"""
        <div class="card">
            <div class="card-label">Profit / Loss</div>
            <div class="card-value {pl_class}">{fmt_eur(net_pl)}</div>
            <div class="card-sub">Net result YTD</div>
        </div>
        """, unsafe_allow_html=True)

    # --- Monthly Breakdown Table ---
    rows = []
    for _, r in active.iterrows():
        pl_val = r['Profit_Loss']
        pl_cls = 'positive' if pl_val >= 0 else 'negative'
        rows.append({
            'Month': r['Month'],
            'Income': f'<span class="num">{fmt_eur(r["Income"])}</span>',
            'Expenses': f'<span class="num">{fmt_eur(r["Expenses"])}</span>',
            'Profit / Loss': f'<span class="num {pl_cls}">{fmt_eur(pl_val)}</span>',
        })
    # Totals row
    pl_cls = 'positive' if net_pl >= 0 else 'negative'
    rows.append({
        'Month': 'Totals',
        'Income': f'<span class="num">{fmt_eur(total_income)}</span>',
        'Expenses': f'<span class="num">{fmt_eur(total_expenses)}</span>',
        'Profit / Loss': f'<span class="num {pl_cls}">{fmt_eur(net_pl)}</span>',
        '_total': True,
    })
    chart_card_html('Monthly Breakdown',
                    html_table(['Month', 'Income', 'Expenses', 'Profit / Loss'], rows, num_cols={1, 2, 3}))



# ─── TAB 2 — Expenses ───────────────────────────────────────────────────────

def tab_expenses(data):
    expenses = data['expenses']
    if expenses.empty:
        st.info("No expense data available.")
        return

    df = expenses

    # --- KPI Cards ---
    total_netto = df['Netto (€)'].sum()
    months_avail = sorted(df['Month'].dropna().unique().tolist(),
                          key=lambda m: MONTHS.index(m) if m in MONTHS else 99)
    month_totals = df.groupby('Month')['Netto (€)'].sum()

    cols = st.columns(min(len(months_avail) + 1, 4))
    for i, m in enumerate(months_avail[:3]):
        with cols[i]:
            metric_card(f'{m} Expenses', month_totals.get(m, 0))
    with cols[min(len(months_avail), 3)]:
        st.markdown(f"""
        <div class="card">
            <div class="card-label">Total Expenses</div>
            <div class="card-value negative">{fmt_eur(total_netto)}</div>
        </div>
        """, unsafe_allow_html=True)

    # --- Expenses by Category (pie chart) ---
    section_title('Expenses by Category')
    cat = df.groupby('Category')['Netto (€)'].sum().sort_values(ascending=False).reset_index()
    fig = go.Figure(data=[go.Pie(
        labels=cat['Category'], values=cat['Netto (€)'],
        hole=0.55,
        marker=dict(colors=CHART_COLORS[:len(cat)], line=dict(width=0)),
        textinfo='label+percent',
        textfont=dict(size=13, color=_t()["text"], family=FONT),
        hovertemplate='<b>%{label}</b><br>\u20ac%{value:,.2f}<br>%{percent}<extra></extra>',
    )])
    chart_layout(fig, height=650)
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # --- All Expense Categories Table ---
    cat_month = df.pivot_table(index='Category', columns='Month', values='Netto (€)',
                                aggfunc='sum', fill_value=0)
    # Sort months
    month_order = [m for m in MONTHS if m in cat_month.columns]
    cat_month = cat_month[month_order]
    cat_month['Total'] = cat_month.sum(axis=1)
    cat_month = cat_month.sort_values('Total', ascending=False)

    headers = ['Category'] + month_order + ['Total']
    rows = []
    for cat_name, r in cat_month.iterrows():
        row = {'Category': badge_html(cat_name, cat_name)}
        for m in month_order:
            row[m] = fmt_eur(r[m])
        row['Total'] = fmt_eur(r['Total'])
        rows.append(row)
    # Total row
    total_row = {'Category': '<strong>TOTAL</strong>', '_total': True}
    for m in month_order:
        total_row[m] = fmt_eur(cat_month[m].sum())
    total_row['Total'] = fmt_eur(cat_month['Total'].sum())
    rows.append(total_row)

    chart_card_html('All Expense Categories',
                    html_table(headers, rows, num_cols=set(range(1, len(headers)))))

    # --- Spending by Vendor Chart ---
    section_title('Spending by Vendor')
    vendor = df.groupby('Recipient')['Netto (€)'].sum().sort_values(ascending=True).tail(15).reset_index()
    fig_vendor = go.Figure(go.Bar(
        y=vendor['Recipient'], x=vendor['Netto (€)'], orientation='h',
        marker_color=C_ORANGE,
        marker=dict(cornerradius=4),
        text=[fmt_eur(v) for v in vendor['Netto (€)']],
        textposition='outside', textfont=dict(color=_t()["text"], size=10, family=FONT),
    ))
    chart_layout(fig_vendor, height=max(300, len(vendor) * 32))
    st.plotly_chart(fig_vendor, use_container_width=True)

    # --- Transaction History Table with Edit/Delete ---
    section_title('Transaction History')

    # Search & Filter controls (#1)
    fc1, fc2, fc3 = st.columns([2.5, 1.5, 1.5])
    with fc1:
        search_q = st.text_input("Search", placeholder="Search recipient, notes...",
                                  key='exp_search', label_visibility='collapsed')
    with fc2:
        all_cats = ['All Categories'] + sorted(df['Category'].dropna().unique().tolist())
        filter_cat = st.selectbox("Category", all_cats, key='exp_filter_cat', label_visibility='collapsed')
    with fc3:
        filter_months = ['All Months'] + [m for m in MONTHS if m in df['Month'].values]
        filter_month = st.selectbox("Month", filter_months, key='exp_filter_month', label_visibility='collapsed')

    filtered_df = df.copy()
    if search_q.strip():
        q_lower = search_q.strip().lower()
        mask = filtered_df.apply(
            lambda r: q_lower in str(r.get('Recipient', '')).lower()
                      or q_lower in str(r.get('Notes', '')).lower()
                      or q_lower in str(r.get('Category', '')).lower(),
            axis=1)
        filtered_df = filtered_df[mask]
    if filter_cat != 'All Categories':
        filtered_df = filtered_df[filtered_df['Category'] == filter_cat]
    if filter_month != 'All Months':
        filtered_df = filtered_df[filtered_df['Month'] == filter_month]

    st.markdown("""<div class="tx-header">
        <span style="flex:0.4">&#35;</span>
        <span style="flex:1.2">DATE</span>
        <span style="flex:2.2">RECIPIENT</span>
        <span style="flex:1.8">CATEGORY</span>
        <span style="flex:1.4;text-align:right">AMOUNT</span>
        <span style="flex:1.6;text-align:center">ACTIONS</span>
    </div>""", unsafe_allow_html=True)

    sorted_exp = filtered_df.sort_values('Date of Payment', ascending=False).reset_index(drop=True)
    total_count = len(sorted_exp)

    # Pagination
    PAGE_SIZE = 15
    total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
    page = st.session_state.get('exp_page', 0)
    page = min(page, total_pages - 1)
    start_idx = page * PAGE_SIZE
    page_df = sorted_exp.iloc[start_idx:start_idx + PAGE_SIZE]

    for idx, (_, r) in enumerate(page_df.iterrows(), start_idx + 1):
        display_num = total_count - idx + 1
        date_str = ''
        if pd.notna(r.get('Date of Payment')):
            try:
                dt = pd.to_datetime(r['Date of Payment'])
                date_str = dt.strftime('%d.%m.%Y')
            except Exception:
                date_str = str(r['Date of Payment'])
        cat_name = str(r.get('Category', ''))
        inv_id = int(r.get('Invoice-ID', idx))
        amount_str = fmt_eur(r['Netto (€)'])

        cols = st.columns([0.4, 1.2, 2.2, 1.8, 1.4, 0.8, 0.8])
        cell_style = f'font-size:0.82rem;color:{_t()["text"]};padding:0.3rem 0;font-family:{FONT};'
        with cols[0]:
            st.markdown(f'<div style="{cell_style}color:{_t()["text_secondary"]}">{display_num}</div>', unsafe_allow_html=True)
        with cols[1]:
            st.markdown(f'<div style="{cell_style}">{date_str}</div>', unsafe_allow_html=True)
        with cols[2]:
            st.markdown(f'<div style="{cell_style}">{r.get("Recipient", "")}</div>', unsafe_allow_html=True)
        with cols[3]:
            st.markdown(f'<div style="{cell_style}">{badge_html(cat_name, cat_name)}</div>', unsafe_allow_html=True)
        with cols[4]:
            st.markdown(f'<div style="{cell_style}text-align:right;font-weight:600">{amount_str}</div>', unsafe_allow_html=True)
        with cols[5]:
            st.markdown('<div class="tx-actions">', unsafe_allow_html=True)
            if st.button("Edit", key=f"edit_{inv_id}"):
                st.session_state['_edit_nonce'] = st.session_state.get('_edit_nonce', 0) + 1
                edit_expense_dialog(r.to_dict())
            st.markdown('</div>', unsafe_allow_html=True)
        with cols[6]:
            st.markdown('<div class="tx-actions tx-del">', unsafe_allow_html=True)
            if st.button("Delete", key=f"del_{inv_id}"):
                delete_expense_dialog(r.to_dict())
            st.markdown('</div>', unsafe_allow_html=True)

    # Pagination controls
    if total_pages > 1:
        pc1, pc2, pc3 = st.columns([1, 2, 1])
        with pc1:
            if page > 0 and st.button("← Prev", key='exp_prev'):
                st.session_state['exp_page'] = page - 1
                st.rerun()
        with pc2:
            st.markdown(
                f'<div style="text-align:center;color:{_t()["text_secondary"]};font-size:0.8rem;padding-top:0.4rem">'
                f'Page {page + 1} of {total_pages} · {total_count} expenses</div>',
                unsafe_allow_html=True)
        with pc3:
            if page < total_pages - 1 and st.button("Next →", key='exp_next'):
                st.session_state['exp_page'] = page + 1
                st.rerun()


# ─── TAB 3 — Income ─────────────────────────────────────────────────────────

def tab_income(data):
    overview = data['overview']
    paid = data.get('income_paid', pd.DataFrame())
    unpaid = data.get('income_unpaid', pd.DataFrame())

    total_paid = pd.to_numeric(paid.get('Netto (€)', pd.Series(dtype=float)), errors='coerce').sum() if len(paid) else 0
    total_unpaid = pd.to_numeric(unpaid.get('Netto (€)', pd.Series(dtype=float)), errors='coerce').sum() if len(unpaid) else 0
    num_paid = len(paid)
    num_unpaid = len(unpaid)

    # Category breakdown for paid
    cat_totals = {}
    if len(paid) and 'Category' in paid.columns:
        p = paid.copy()
        p['_n'] = pd.to_numeric(p['Netto (€)'], errors='coerce')
        cat_totals = p.groupby('Category')['_n'].sum().to_dict()

    # --- KPI Cards ---
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        metric_card('Total Paid Income', total_paid,
                     sub=f'{num_paid} invoices', color_class='positive')
    with k2:
        metric_card('Unpaid Outstanding', total_unpaid,
                     sub=f'{num_unpaid} invoices', color_class='negative')

    # Show top 2 categories in remaining cards
    sorted_cats = sorted(cat_totals.items(), key=lambda x: -x[1])
    for i, (cat, val) in enumerate(sorted_cats[:2]):
        with [k3, k4][i]:
            pct = (val / total_paid * 100) if total_paid > 0 else 0
            color = 'accent' if i == 0 else 'blue'
            st.markdown(f"""
            <div class="card">
                <div class="card-label">{cat}</div>
                <div class="card-value {color}">{fmt_eur(val)}</div>
                <div class="card-sub">{pct:.1f}% of paid income</div>
            </div>
            """, unsafe_allow_html=True)

    # --- Charts ---
    c1, c2 = st.columns(2)

    with c1:
        section_title('Income by Category')
        if cat_totals:
            fig = go.Figure(data=[go.Pie(
                labels=list(cat_totals.keys()), values=list(cat_totals.values()),
                hole=0.55,
                marker=dict(colors=[C_ORANGE, C_BLUE, C_GREEN][:len(cat_totals)], line=dict(width=0)),
                textinfo='label+percent+value',
                texttemplate='%{label}<br>\u20ac%{value:,.0f}<br>(%{percent})',
                textfont=dict(size=12, color=_t()["text"], family=FONT),
            )])
            chart_layout(fig, height=320)
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        section_title('Income by Client')
        if len(paid) and 'Client' in paid.columns:
            p = paid.copy()
            p['_n'] = pd.to_numeric(p['Netto (€)'], errors='coerce')
            cr = p.groupby('Client')['_n'].sum().sort_values(ascending=True).reset_index()
            colors_list = [C_ORANGE, C_ORANGE_LIGHT, '#FFB088', '#FFD4BB']
            fig2 = go.Figure(go.Bar(
                y=cr['Client'], x=cr['_n'], orientation='h',
                marker_color=colors_list[:len(cr)] if len(cr) <= 4 else C_ORANGE,
                marker=dict(cornerradius=6),
                text=[fmt_eur(v) for v in cr['_n']],
                textposition='outside', textfont=dict(color=_t()["text"], size=11, family=FONT),
            ))
            chart_layout(fig2, height=max(250, len(cr) * 55))
            fig2.update_layout(showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)

    # --- Paid Invoices Table ---
    if len(paid):
        section_title('Paid Invoices')

        st.markdown("""<div class="tx-header">
            <span style="flex:0.3">&#35;</span>
            <span style="flex:0.7">INVOICE</span>
            <span style="flex:0.7">DATE</span>
            <span style="flex:1.2">CLIENT</span>
            <span style="flex:1.0">PROJECT</span>
            <span style="flex:0.9">CATEGORY</span>
            <span style="flex:0.6;text-align:right">NETTO</span>
            <span style="flex:0.6;text-align:right">BRUTTO</span>
            <span style="flex:0.6;text-align:center">ACTION</span>
        </div>""", unsafe_allow_html=True)

        _cs = f'font-size:0.82rem;color:{_t()["text"]};padding:0.3rem 0;font-family:{FONT};'
        for idx, (_, r) in enumerate(paid.iterrows(), 1):
            date_str = ''
            if 'Date' in r.index and pd.notna(r['Date']):
                try:
                    dt = pd.to_datetime(r['Date'])
                    date_str = dt.strftime('%d.%m.%Y')
                except Exception:
                    date_str = str(r['Date'])

            inv_num = str(r.get('Invoice Number', ''))
            client = str(r.get('Client', ''))
            project = str(r.get('Project', ''))
            cat_name = str(r.get('Category', ''))
            netto = pd.to_numeric(r.get('Netto (€)', 0), errors='coerce')
            brutto = pd.to_numeric(r.get('Brutto (€)', 0), errors='coerce')

            cols = st.columns([0.3, 0.7, 0.7, 1.2, 1.0, 0.9, 0.6, 0.6, 0.6])
            with cols[0]:
                st.markdown(f'<div style="{_cs}color:{_t()["text_secondary"]}">{idx}</div>', unsafe_allow_html=True)
            with cols[1]:
                st.markdown(f'<div style="{_cs}">{inv_num}</div>', unsafe_allow_html=True)
            with cols[2]:
                st.markdown(f'<div style="{_cs}">{date_str}</div>', unsafe_allow_html=True)
            with cols[3]:
                st.markdown(f'<div style="{_cs}">{client}</div>', unsafe_allow_html=True)
            with cols[4]:
                st.markdown(f'<div style="{_cs}">{project}</div>', unsafe_allow_html=True)
            with cols[5]:
                st.markdown(f'<div style="{_cs}">{badge_html(cat_name, cat_name)}</div>', unsafe_allow_html=True)
            with cols[6]:
                st.markdown(f'<div style="{_cs}text-align:right;font-weight:600">{fmt_eur(netto) if not pd.isna(netto) else ""}</div>', unsafe_allow_html=True)
            with cols[7]:
                st.markdown(f'<div style="{_cs}text-align:right;font-weight:600">{fmt_eur(brutto) if not pd.isna(brutto) else ""}</div>', unsafe_allow_html=True)
            with cols[8]:
                st.markdown('<div class="tx-actions tx-del">', unsafe_allow_html=True)
                if st.button("Delete", key=f"del_paid_{inv_num}"):
                    delete_income_invoice_dialog(r.to_dict(), 'paid')
                st.markdown('</div>', unsafe_allow_html=True)

    # --- Unpaid Invoices Table ---
    if len(unpaid):
        section_title('Unpaid Invoices')

        st.markdown("""<div class="tx-header">
            <span style="flex:0.7">INVOICE</span>
            <span style="flex:0.7">DATE</span>
            <span style="flex:1.2">CLIENT</span>
            <span style="flex:1.0">PROJECT</span>
            <span style="flex:0.9">CATEGORY</span>
            <span style="flex:0.6;text-align:right">NETTO</span>
            <span style="flex:1.2;text-align:center">ACTIONS</span>
        </div>""", unsafe_allow_html=True)

        _cs2 = f'font-size:0.82rem;color:{_t()["text"]};padding:0.3rem 0;font-family:{FONT};'
        for _, r in unpaid.iterrows():
            date_str = ''
            if 'Date' in r.index and pd.notna(r['Date']):
                try:
                    dt = pd.to_datetime(r['Date'])
                    date_str = dt.strftime('%d.%m.%Y')
                except Exception:
                    date_str = str(r['Date'])

            inv_num = str(r.get('Invoice Number', ''))
            client = str(r.get('Client', ''))
            project = str(r.get('Project', ''))
            cat_name = str(r.get('Category', ''))
            netto = pd.to_numeric(r.get('Netto (€)', 0), errors='coerce')

            cols = st.columns([0.7, 0.7, 1.2, 1.0, 0.9, 0.6, 0.6, 0.6])
            with cols[0]:
                st.markdown(f'<div style="{_cs2}">{inv_num}</div>', unsafe_allow_html=True)
            with cols[1]:
                st.markdown(f'<div style="{_cs2}">{date_str}</div>', unsafe_allow_html=True)
            with cols[2]:
                st.markdown(f'<div style="{_cs2}">{client}</div>', unsafe_allow_html=True)
            with cols[3]:
                st.markdown(f'<div style="{_cs2}">{project}</div>', unsafe_allow_html=True)
            with cols[4]:
                st.markdown(f'<div style="{_cs2}">{badge_html(cat_name, cat_name)}</div>', unsafe_allow_html=True)
            with cols[5]:
                st.markdown(f'<div style="{_cs2}text-align:right;font-weight:600">{fmt_eur(netto) if not pd.isna(netto) else ""}</div>', unsafe_allow_html=True)
            with cols[6]:
                st.markdown('<div class="tx-actions">', unsafe_allow_html=True)
                if st.button("Mark Paid", key=f"paid_{inv_num}"):
                    mark_invoice_paid_dialog(r.to_dict())
                st.markdown('</div>', unsafe_allow_html=True)
            with cols[7]:
                st.markdown('<div class="tx-actions tx-del">', unsafe_allow_html=True)
                if st.button("Delete", key=f"del_unpaid_{inv_num}"):
                    delete_income_invoice_dialog(r.to_dict(), 'unpaid')
                st.markdown('</div>', unsafe_allow_html=True)

    elif total_unpaid == 0:
        st.markdown(f"""
        <div class="card" style="border-left: 3px solid {C_GREEN};">
            <div class="card-value positive" style="font-size:1.2rem">All invoices are paid!</div>
        </div>
        """, unsafe_allow_html=True)


# ─── TAB 4 — Goal Tracker ───────────────────────────────────────────────────

def tab_goal(data):
    goal_info = _parse_goal(data.get('goal_raw'))
    overview = data['overview']
    m2025 = _parse_2025_monthly(data.get('hist_2025'))

    goal = goal_info['goal']

    # Compute acquired dynamically: 2025 (Sep-Dec) + 2026 paid income
    goal_months_2025 = ['September', 'October', 'November', 'December']
    subtotal_2025_goal = sum(m2025.get(m, 0) for m in goal_months_2025)
    subtotal_2026_goal = overview['Income'].sum()
    acquired = subtotal_2025_goal + subtotal_2026_goal
    remaining = max(0, goal - acquired)
    pct = (acquired / goal * 100) if goal > 0 else 0

    active_months_2026 = len(overview[overview['Income'] > 0])
    months_left = max(1, 12 - active_months_2026)
    monthly_target = remaining / months_left

    # --- KPI Cards ---
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        gauge_card('Annual Goal', acquired, goal,
                   fmt_value=f'\u20ac{acquired:,.0f}',
                   sub=f'of \u20ac{goal:,.0f} goal', color=C_ORANGE)
    with k2:
        metric_card('Acquired', acquired, sub=f'{pct:.1f}% achieved', color_class='positive')
    with k3:
        metric_card('Remaining', remaining, sub=f'{100-pct:.1f}% to go')
    with k4:
        metric_card('Monthly Target', monthly_target,
                    sub=f'Avg over remaining {months_left} months', color_class='accent')

    # --- Progress Bar ---
    pct_clamped = min(pct, 100)
    progress_html = f"""
    <div style="margin:1rem 0">
        <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width:{pct_clamped:.1f}%">{pct:.1f}%</div>
        </div>
        <div class="goal-milestones">
            <span>\u20ac0</span>
            <span>\u20ac{goal*0.25:,.0f}</span>
            <span>\u20ac{goal*0.5:,.0f}</span>
            <span>\u20ac{goal*0.75:,.0f}</span>
            <span>\u20ac{goal:,.0f}</span>
        </div>
    </div>
    """
    chart_card_html('Progress to Goal', progress_html)

    # --- 2025 vs 2026 Month Grids ---
    if m2025:
        c1, c2 = st.columns(2)

        with c1:
            grid_months = ['September', 'October', 'November', 'December']
            content_html = '<div class="month-grid">'
            for m in grid_months:
                val = m2025.get(m, 0)
                content_html += f"""
                <div class="month-card">
                    <div class="m-label">{m[:3]}</div>
                    <div class="m-value accent">\u20ac{val:,.0f}</div>
                </div>"""
            content_html += '</div>'
            subtotal_2025 = sum(m2025.get(m, 0) for m in grid_months)
            content_html += f"""
            <div class="summary-row" style="margin-top:1rem">
                <span class="s-label">2025 Subtotal</span>
                <span class="s-value accent">\u20ac{subtotal_2025:,.0f}</span>
            </div>"""
            chart_card_html('2025 Income (Sep \u2013 Dec)', content_html)

        with c2:
            ov_dict = dict(zip(overview['Month'], overview['Income']))
            active_2026 = [(m, ov_dict.get(m, 0)) for m in MONTHS if ov_dict.get(m, 0) > 0]
            n_cols = max(len(active_2026), 2)
            grid_class = 'month-grid' if n_cols >= 4 else 'month-grid-2'

            content_html = f'<div class="{grid_class}">'
            for m, val in active_2026:
                content_html += f"""
                <div class="month-card">
                    <div class="m-label">{m[:3]}</div>
                    <div class="m-value positive">\u20ac{val:,.2f}</div>
                </div>"""
            content_html += '</div>'
            subtotal_2026 = sum(v for _, v in active_2026)
            content_html += f"""
            <div class="summary-row" style="margin-top:1rem">
                <span class="s-label">2026 Subtotal</span>
                <span class="s-value positive">\u20ac{subtotal_2026:,.2f}</span>
            </div>"""
            chart_card_html('2026 Income (Jan \u2013 Present)', content_html)

    # --- Cumulative Progress Chart ---
    if m2025:
        section_title('Cumulative Progress')

        ov_dict = dict(zip(overview['Month'], overview['Income']))
        labels = ['Sep 25', 'Oct 25', 'Nov 25', 'Dec 25']
        labels += [f'{m[:3]} 26' for m in MONTHS]

        cum_vals = []
        running = 0
        for m in ['September', 'October', 'November', 'December']:
            running += m2025.get(m, 0)
            cum_vals.append(running)
        for m in MONTHS:
            inc = ov_dict.get(m, 0)
            if inc > 0:
                running += inc
                cum_vals.append(running)
            else:
                cum_vals.append(None)

        # Goal line
        goal_step = goal / 16
        goal_line = [goal_step * (i + 1) for i in range(16)]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=labels, y=cum_vals, mode='lines+markers', name='Cumulative Income',
            line=dict(color=C_ORANGE, width=3),
            marker=dict(size=6, color=C_ORANGE),
            fill='tozeroy', fillcolor='rgba(232,93,38,0.1)',
            connectgaps=False,
        ))
        fig.add_trace(go.Scatter(
            x=labels, y=goal_line, mode='lines', name='Goal Line',
            line=dict(color=_t()["surface3"], width=2, dash='dash'),
        ))
        fig.update_layout(
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        )
        chart_layout(fig, height=350)
        fig.update_yaxes(range=[0, goal * 1.1])
        st.plotly_chart(fig, use_container_width=True)

    # --- Cash Flow Projection (#6) ---
    section_title('Cash Flow Projection')

    ov_dict = dict(zip(overview['Month'], overview['Income']))
    exp_dict = dict(zip(overview['Month'], overview['Expenses']))
    active_income_months = [m for m in MONTHS if ov_dict.get(m, 0) > 0]

    if active_income_months:
        avg_monthly_income = sum(ov_dict[m] for m in active_income_months) / len(active_income_months)
        active_expense_months = [m for m in MONTHS if exp_dict.get(m, 0) > 0]
        avg_monthly_expenses = (sum(exp_dict[m] for m in active_expense_months) / len(active_expense_months)) if active_expense_months else 0
        avg_monthly_net = avg_monthly_income - avg_monthly_expenses

        # Project forward
        current_month_idx = len(active_income_months)  # how many months of data we have
        projected_income_eoy = sum(ov_dict.get(m, 0) for m in MONTHS) + avg_monthly_income * (12 - current_month_idx)
        projected_expenses_eoy = sum(exp_dict.get(m, 0) for m in MONTHS) + avg_monthly_expenses * (12 - current_month_idx)
        projected_net_eoy = projected_income_eoy - projected_expenses_eoy

        # Goal projection
        if goal > 0:
            months_to_goal = max(0, (remaining / avg_monthly_income)) if avg_monthly_income > 0 else float('inf')
            current_month_num = MONTHS.index(active_income_months[-1]) + 1
            goal_month_num = current_month_num + int(months_to_goal)
            if goal_month_num <= 16:  # within the Sep25-Dec26 window
                goal_eta = f"~{MONTHS[min(goal_month_num - 1, 11)][:3]} {CURRENT_YEAR}" if goal_month_num <= 12 else "On track"
            else:
                goal_eta = "After Dec 2026"
        else:
            goal_eta = "No goal set"

        k1, k2, k3, k4 = st.columns(4)
        with k1:
            metric_card('Avg Monthly Income', avg_monthly_income,
                        sub=f'Based on {len(active_income_months)} months', color_class='accent')
        with k2:
            metric_card('Avg Monthly Expenses', avg_monthly_expenses,
                        sub=f'Based on {len(active_expense_months)} months')
        with k3:
            net_cls = 'positive' if avg_monthly_net >= 0 else 'negative'
            metric_card('Avg Monthly Net', avg_monthly_net,
                        sub='Income minus expenses', color_class=net_cls)
        with k4:
            st.markdown(f"""
            <div class="card">
                <div class="card-label">Projected Year-End Net</div>
                <div class="card-value {'positive' if projected_net_eoy >= 0 else 'negative'}">{fmt_eur(projected_net_eoy)}</div>
                <div class="card-sub">Goal ETA: {goal_eta}</div>
            </div>
            """, unsafe_allow_html=True)

        # Projection chart
        actual_income = [ov_dict.get(m, 0) for m in MONTHS]
        projected = []
        for i, m in enumerate(MONTHS):
            if ov_dict.get(m, 0) > 0:
                projected.append(None)
            else:
                projected.append(avg_monthly_income)

        fig_proj = go.Figure()
        fig_proj.add_trace(go.Bar(
            name='Actual Income', x=[m[:3] for m in MONTHS],
            y=actual_income,
            marker_color=C_ORANGE,
            marker=dict(cornerradius=4),
        ))
        fig_proj.add_trace(go.Bar(
            name='Projected Income', x=[m[:3] for m in MONTHS],
            y=projected,
            marker_color='rgba(232,93,38,0.25)',
            marker=dict(cornerradius=4, line=dict(color=C_ORANGE, width=1)),
        ))
        fig_proj.update_layout(
            barmode='stack', bargap=0.25,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        )
        chart_layout(fig_proj, height=320)
        st.plotly_chart(fig_proj, use_container_width=True)
    else:
        st.info("No income data yet — projections will appear once you have at least one month of data.")


# ─── TAB 5 — Taxes ──────────────────────────────────────────────────────────

def tab_taxes(data):
    overview = data['overview']
    total_income = overview['Income'].sum()
    total_expenses = overview['Expenses'].sum()

    if total_income == 0 and total_expenses == 0:
        st.info("No financial data yet — tax estimates will appear once you have income or expenses.")
        return

    # --- KPI Cards ---
    taxable_income = total_income - total_expenses
    est_income_tax = max(0, taxable_income * TAX_RATE_INCOME)
    est_vat = total_income * VAT_RATE
    vat_deductible = total_expenses * VAT_RATE
    net_vat = max(0, est_vat - vat_deductible)
    total_tax_burden = est_income_tax + net_vat
    after_tax = total_income - total_expenses - total_tax_burden

    tk1, tk2, tk3, tk4 = st.columns(4)
    with tk1:
        st.markdown(f"""
        <div class="card">
            <div class="card-label">Taxable Profit</div>
            <div class="card-value {'positive' if taxable_income >= 0 else 'negative'}">{fmt_eur(taxable_income)}</div>
            <div class="card-sub">Income minus deductible expenses</div>
        </div>
        """, unsafe_allow_html=True)
    with tk2:
        st.markdown(f"""
        <div class="card">
            <div class="card-label">Est. Income Tax (~{TAX_RATE_INCOME:.0%})</div>
            <div class="card-value negative">{fmt_eur(est_income_tax)}</div>
            <div class="card-sub">Set aside for Einkommensteuer</div>
        </div>
        """, unsafe_allow_html=True)
    with tk3:
        st.markdown(f"""
        <div class="card">
            <div class="card-label">Net VAT / Umsatzsteuer</div>
            <div class="card-value negative">{fmt_eur(net_vat)}</div>
            <div class="card-sub">{fmt_eur(est_vat)} output \u2212 {fmt_eur(vat_deductible)} input</div>
        </div>
        """, unsafe_allow_html=True)
    with tk4:
        st.markdown(f"""
        <div class="card" style="border-left: 3px solid {C_ORANGE};">
            <div class="card-label">After-Tax Estimate</div>
            <div class="card-value {'positive' if after_tax >= 0 else 'negative'}">{fmt_eur(after_tax)}</div>
            <div class="card-sub">Total tax burden: {fmt_eur(total_tax_burden)}</div>
        </div>
        """, unsafe_allow_html=True)

    # --- Quarterly VAT Summary ---
    section_title('Quarterly VAT Summary')

    quarters = [
        ('Q1 (Jan-Mar)', ['January', 'February', 'March']),
        ('Q2 (Apr-Jun)', ['April', 'May', 'June']),
        ('Q3 (Jul-Sep)', ['July', 'August', 'September']),
        ('Q4 (Oct-Dec)', ['October', 'November', 'December']),
    ]
    q_rows = []
    for q_name, q_months in quarters:
        q_income = sum(overview.loc[overview['Month'] == m, 'Income'].sum() for m in q_months)
        q_expenses = sum(overview.loc[overview['Month'] == m, 'Expenses'].sum() for m in q_months)
        if q_income > 0 or q_expenses > 0:
            q_vat_out = q_income * VAT_RATE
            q_vat_in = q_expenses * VAT_RATE
            q_net_vat = max(0, q_vat_out - q_vat_in)
            q_rows.append({
                'Quarter': q_name,
                'Income': fmt_eur(q_income),
                'Expenses': fmt_eur(q_expenses),
                'VAT Output': fmt_eur(q_vat_out),
                'VAT Input': fmt_eur(q_vat_in),
                'VAT Due': fmt_eur(q_net_vat),
            })
    if q_rows:
        chart_card_html('Quarterly VAT Breakdown',
                        html_table(['Quarter', 'Income', 'Expenses', 'VAT Output', 'VAT Input', 'VAT Due'],
                                   q_rows, num_cols={1, 2, 3, 4, 5}))

    # --- Monthly Tax Breakdown ---
    section_title('Monthly Tax Breakdown')

    active = overview[(overview['Income'] > 0) | (overview['Expenses'] > 0)]
    m_rows = []
    for _, r in active.iterrows():
        m_inc = r['Income']
        m_exp = r['Expenses']
        m_profit = m_inc - m_exp
        m_tax = max(0, m_profit * TAX_RATE_INCOME)
        m_vat_out = m_inc * VAT_RATE
        m_vat_in = m_exp * VAT_RATE
        m_vat_net = max(0, m_vat_out - m_vat_in)
        m_rows.append({
            'Month': r['Month'],
            'Income': fmt_eur(m_inc),
            'Expenses': fmt_eur(m_exp),
            'Profit': f'<span class="num {"positive" if m_profit >= 0 else "negative"}">{fmt_eur(m_profit)}</span>',
            'Income Tax': fmt_eur(m_tax),
            'Net VAT': fmt_eur(m_vat_net),
        })
    # Totals
    m_rows.append({
        'Month': 'Totals',
        'Income': fmt_eur(total_income),
        'Expenses': fmt_eur(total_expenses),
        'Profit': f'<span class="num {"positive" if taxable_income >= 0 else "negative"}">{fmt_eur(taxable_income)}</span>',
        'Income Tax': fmt_eur(est_income_tax),
        'Net VAT': fmt_eur(net_vat),
        '_total': True,
    })
    chart_card_html('Monthly Tax Detail',
                    html_table(['Month', 'Income', 'Expenses', 'Profit', 'Income Tax', 'Net VAT'],
                               m_rows, num_cols={1, 2, 3, 4, 5}))

    # --- Year-End Projection ---
    active_months = len(active)
    if active_months > 0:
        section_title('Year-End Tax Projection')

        avg_income = total_income / active_months
        avg_expenses = total_expenses / active_months
        months_remaining = 12 - active_months

        proj_income = total_income + avg_income * months_remaining
        proj_expenses = total_expenses + avg_expenses * months_remaining
        proj_profit = proj_income - proj_expenses
        proj_income_tax = max(0, proj_profit * TAX_RATE_INCOME)
        proj_vat = max(0, proj_income * VAT_RATE - proj_expenses * VAT_RATE)
        proj_total_tax = proj_income_tax + proj_vat

        pk1, pk2, pk3, pk4 = st.columns(4)
        with pk1:
            metric_card('Projected Income', proj_income,
                        sub=f'Based on {active_months} months avg', color_class='accent')
        with pk2:
            metric_card('Projected Expenses', proj_expenses,
                        sub=f'{months_remaining} months remaining')
        with pk3:
            st.markdown(f"""
            <div class="card">
                <div class="card-label">Projected Income Tax</div>
                <div class="card-value negative">{fmt_eur(proj_income_tax)}</div>
                <div class="card-sub">~{TAX_RATE_INCOME:.0%} of projected profit</div>
            </div>
            """, unsafe_allow_html=True)
        with pk4:
            st.markdown(f"""
            <div class="card" style="border-left: 3px solid {C_ORANGE};">
                <div class="card-label">Total Tax Burden (Projected)</div>
                <div class="card-value negative">{fmt_eur(proj_total_tax)}</div>
                <div class="card-sub">Income tax + VAT for full year</div>
            </div>
            """, unsafe_allow_html=True)

    # --- Tax rates info ---
    st.markdown(f"""
    <div style="margin-top:1.5rem;padding:0.8rem 1rem;background:{_t()["surface2"]};border-radius:8px;
                border:1px solid {_t()["border"]};font-size:0.78rem;color:{_t()["muted"]}">
        <strong style="color:{_t()["text"]}">Tax Rates Used:</strong>
        Income Tax: {TAX_RATE_INCOME:.0%} (combined Einkommensteuer + Soli) &middot;
        VAT: {VAT_RATE:.0%} (Umsatzsteuer) &middot;
        These are estimates only &mdash; consult your Steuerberater for exact figures.
        Rates can be adjusted in the dashboard configuration (TAX_RATE_INCOME, VAT_RATE).
    </div>
    """, unsafe_allow_html=True)


# ─── TAB 6 — INVOICES / OFFERS ──────────────────────────────────────────────

# ---------- Client Database (Google Sheets) ----------

def _get_clients_worksheet():
    """Get or create the 'Clients' worksheet."""
    ss = _gsheet()
    try:
        return ss.worksheet('Clients')
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title='Clients', rows=100, cols=6)
        ws.update('A1:F1', [['ID', 'Name', 'Address', 'Notes', 'Country', 'Added']])
        return ws


def _get_counters_worksheet():
    """Get or create the 'Counters' worksheet for offer/invoice/client numbering."""
    ss = _gsheet()
    try:
        return ss.worksheet('Counters')
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title='Counters', rows=5, cols=2)
        ws.update('A1:B4', [
            ['Counter', 'Value'],
            ['offer', '27'],
            ['invoice', '53'],
            ['client', '23'],
        ])
        return ws


@st.cache_data(ttl=300)
def _load_clients_db():
    """Load client database from Google Sheets. Returns list of client dicts."""
    try:
        ws = _get_clients_worksheet()
        rows = ws.get_all_records()
        if rows:
            return [{'id': r.get('ID', ''), 'name': r.get('Name', ''),
                      'address': r.get('Address', ''), 'notes': r.get('Notes', ''),
                      'country': r.get('Country', '')} for r in rows if r.get('Name')]
    except Exception:
        pass
    return []


def _save_client_to_sheet(client):
    """Append a new client row to the Clients worksheet."""
    try:
        ws = _get_clients_worksheet()
        ws.append_row([
            client['id'], client['name'], client.get('address', ''),
            client.get('notes', ''), client.get('country', ''),
            datetime.now().strftime('%Y-%m-%d'),
        ], value_input_option='USER_ENTERED')
        _load_clients_db.clear()
    except Exception as e:
        st.error(f"Could not save client: {e}")


def _seed_clients_if_empty():
    """Seed the Clients worksheet with known clients if it's empty."""
    clients = _load_clients_db()
    if len(clients) > 0:
        return clients
    try:
        ws = _get_clients_worksheet()
        rows = []
        for c in SEED_CLIENTS:
            rows.append([c['id'], c['name'], c['address'], c['notes'],
                         c.get('country', ''), '2026-01-01'])
        ws.append_rows(rows, value_input_option='USER_ENTERED')
        _load_clients_db.clear()
        return SEED_CLIENTS
    except Exception:
        return SEED_CLIENTS


def _get_counter(name):
    """Read a named counter value from the Counters sheet."""
    try:
        ws = _get_counters_worksheet()
        records = ws.get_all_records()
        for r in records:
            if r.get('Counter') == name:
                return int(r.get('Value', 0))
    except Exception:
        pass
    defaults = {'offer': 27, 'invoice': 53, 'client': 23}
    return defaults.get(name, 0)


def _increment_counter(name):
    """Increment a named counter and return the new value."""
    try:
        ws = _get_counters_worksheet()
        records = ws.get_all_records()
        for i, r in enumerate(records):
            if r.get('Counter') == name:
                new_val = int(r.get('Value', 0)) + 1
                ws.update_cell(i + 2, 2, new_val)
                return new_val
    except Exception:
        pass
    defaults = {'offer': 27, 'invoice': 53, 'client': 23}
    return defaults.get(name, 0) + 1


def _next_offer_number():
    n = _increment_counter('offer')
    return f"AG{CURRENT_YEAR}{str(n).zfill(3)}"


def _next_invoice_number():
    n = _increment_counter('invoice')
    return f"RE{CURRENT_YEAR}{str(n).zfill(3)}"


def _next_client_id():
    n = _increment_counter('client')
    return f"K{str(n).zfill(5)}"


def _get_or_create_client(name, address='', notes='', country=''):
    """Find existing client by name or create a new one."""
    clients = _load_clients_db()
    if not clients:
        clients = _seed_clients_if_empty()
    name_lower = name.strip().lower()
    for c in clients:
        if c['name'].lower() == name_lower:
            return c
        # partial match
        if name_lower in c['name'].lower() or c['name'].lower() in name_lower:
            return c
    # Create new
    new_client = {
        'id': _next_client_id(),
        'name': name.strip(),
        'address': address.strip(),
        'notes': notes.strip(),
        'country': country.strip(),
    }
    _save_client_to_sheet(new_client)
    return new_client


# ---------- Offers Sheet CRUD ----------

_OFFERS_COLS = ['Offer Number', 'Date', 'Client', 'Project', 'Category',
                'Netto', 'Brutto', 'Validity', 'Valid Until', 'Status',
                'Items JSON', 'Created']


def _get_offers_worksheet():
    """Get or create the 'Offers' worksheet."""
    ss = _gsheet()
    try:
        return ss.worksheet('Offers')
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title='Offers', rows=200, cols=len(_OFFERS_COLS))
        ws.update(f'A1:{chr(64+len(_OFFERS_COLS))}1', [_OFFERS_COLS])
        return ws


@st.cache_data(ttl=300)
def _load_offers():
    """Load all offers from the Offers worksheet. Returns list of dicts."""
    try:
        ws = _get_offers_worksheet()
        rows = ws.get_all_records()
        return rows if rows else []
    except Exception:
        return []


def _save_offer_to_sheet(offer_data):
    """Append a new offer row to the Offers worksheet."""
    try:
        ws = _get_offers_worksheet()
        ws.append_row([
            offer_data.get('offer_number', ''),
            offer_data.get('date', ''),
            offer_data.get('client', ''),
            offer_data.get('project', ''),
            offer_data.get('category', ''),
            offer_data.get('netto', 0),
            offer_data.get('brutto', 0),
            offer_data.get('validity', 30),
            offer_data.get('valid_until', ''),
            offer_data.get('status', 'SENT'),
            offer_data.get('items_json', '[]'),
            datetime.now().strftime('%Y-%m-%d %H:%M'),
        ], value_input_option='USER_ENTERED')
        _load_offers.clear()
    except Exception as e:
        st.error(f"Could not save offer: {e}")


def _update_offer_status(offer_number, new_status):
    """Update the status of an offer in the Offers worksheet."""
    try:
        ws = _get_offers_worksheet()
        col_a = ws.col_values(1)  # Offer Number column
        for i, val in enumerate(col_a[1:], start=2):
            if str(val).strip() == str(offer_number).strip():
                ws.update_cell(i, 10, new_status)  # Column J = Status
                _load_offers.clear()
                return True
    except Exception as e:
        st.error(f"Could not update offer status: {e}")
    return False


def _delete_offer_from_sheet(offer_number):
    """Delete an offer row from the sheet and its PDF from Drive."""
    try:
        ws = _get_offers_worksheet()
        col_a = ws.col_values(1)
        for i, val in enumerate(col_a[1:], start=2):
            if str(val).strip() == str(offer_number).strip():
                ws.delete_rows(i)
                _load_offers.clear()
                break
    except Exception:
        pass
    # Delete PDF from Drive
    try:
        folder_id = _get_offers_folder_id()
        if folder_id:
            search_key = str(offer_number).replace('AG', '')
            files = _drive_list_files(folder_id)
            for f in files:
                if search_key in f['name'] and f['name'].endswith('.pdf'):
                    _drive_delete_file(f['id'])
                    break
    except Exception:
        pass


# ---------- DocumentMeta Sheet (for Drafts + Edit) ----------

_DOCMETA_COLS = ['Doc Number', 'Doc Type', 'Client', 'Client Address', 'Project',
                 'Category', 'Description', 'Date', 'Location', 'Event Date',
                 'Service Date', 'Invoice Type', 'Deposit Label', 'Project Total',
                 'Validity', 'Netto', 'Brutto', 'Items JSON', 'Status', 'Created']


def _get_docmeta_worksheet():
    """Get or create the 'DocumentMeta' worksheet."""
    ss = _gsheet()
    try:
        return ss.worksheet('DocumentMeta')
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title='DocumentMeta', rows=200, cols=len(_DOCMETA_COLS))
        ws.update(f'A1:{chr(64+len(_DOCMETA_COLS))}1', [_DOCMETA_COLS])
        return ws


@st.cache_data(ttl=300)
def _load_document_meta():
    """Load all document metadata. Returns list of dicts."""
    try:
        ws = _get_docmeta_worksheet()
        rows = ws.get_all_records()
        return rows if rows else []
    except Exception:
        return []


def _save_document_meta(doc_number, doc_type, meta):
    """Save or update document metadata (upsert by doc number)."""
    try:
        ws = _get_docmeta_worksheet()
        col_a = ws.col_values(1)  # Doc Number
        row_idx = None
        for i, val in enumerate(col_a[1:], start=2):
            if str(val).strip() == str(doc_number).strip():
                row_idx = i
                break
        row_data = [
            doc_number, doc_type,
            meta.get('client', ''), meta.get('client_address', ''),
            meta.get('project', ''), meta.get('category', ''),
            meta.get('description', ''), meta.get('date', ''),
            meta.get('location', ''), meta.get('event_date', ''),
            meta.get('service_date', ''), meta.get('invoice_type', ''),
            meta.get('deposit_label', ''), meta.get('project_total', ''),
            meta.get('validity', ''), meta.get('netto', 0),
            meta.get('brutto', 0), meta.get('items_json', '[]'),
            meta.get('status', 'DRAFT'),
            datetime.now().strftime('%Y-%m-%d %H:%M'),
        ]
        if row_idx:
            ws.update(f'A{row_idx}:T{row_idx}', [row_data],
                      value_input_option='USER_ENTERED')
        else:
            ws.append_row(row_data, value_input_option='USER_ENTERED')
        _load_document_meta.clear()
    except Exception as e:
        st.error(f"Could not save document meta: {e}")


def _get_document_meta(doc_number):
    """Get metadata for a single document by its number."""
    metas = _load_document_meta()
    for m in metas:
        if str(m.get('Doc Number', '')).strip() == str(doc_number).strip():
            return m
    return None


def _delete_document_meta(doc_number):
    """Delete a document metadata row."""
    try:
        ws = _get_docmeta_worksheet()
        col_a = ws.col_values(1)
        for i, val in enumerate(col_a[1:], start=2):
            if str(val).strip() == str(doc_number).strip():
                ws.delete_rows(i)
                _load_document_meta.clear()
                return True
    except Exception:
        pass
    return False


# ---------- PDF Generator (fpdf2, Swiss-style) ----------

def _fmt_eur_de(num):
    """Format number as German-style: 1.234,56"""
    s = f"{num:,.2f}"  # 1,234.56
    # Swap . and ,
    s = s.replace(',', 'X').replace('.', ',').replace('X', '.')
    return s


def _generate_document_pdf(doc_type, doc_data):
    """Generate a Swiss-style A4 PDF invoice or offer.
    doc_type: 'invoice' or 'offer'
    doc_data: dict with keys: number, date, client_name, client_address, client_id,
              title, description, location, event_date, service_date,
              items (list of dicts: pos, description, detail, qty, unit, unit_price, total),
              subtotal, vat, total, invoice_type, deposit_label, project_total, validity
    Returns: bytes (PDF content)
    """
    from fpdf import FPDF

    BRAND_R, BRAND_G, BRAND_B = 11, 71, 20  # #0B4714

    class InvoicePDF(FPDF):
        pass

    pdf = InvoicePDF(orientation='P', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=False)
    pdf.add_page()
    W, H = 210, 297
    ML, MR, MT = 25, 20, 15
    RIGHT = W - MR
    content_w = W - ML - MR

    # ── Green accent line at top ──
    pdf.set_draw_color(BRAND_R, BRAND_G, BRAND_B)
    pdf.set_line_width(0.8)
    pdf.line(ML, MT, RIGHT, MT)

    # ── Header: Name top-right in brand green ──
    pdf.set_text_color(BRAND_R, BRAND_G, BRAND_B)
    pdf.set_font('Helvetica', 'B', 22)
    pdf.set_xy(ML, MT + 5)
    pdf.cell(content_w, 8, BIZ_INFO['name'], align='R')

    pdf.set_text_color(60, 60, 60)
    pdf.set_font('Helvetica', '', 8.5)
    hy = MT + 18
    pdf.set_xy(ML, hy)
    pdf.cell(content_w, 3.5, BIZ_INFO['email'], align='R')
    hy += 3.5
    pdf.set_xy(ML, hy)
    pdf.cell(content_w, 3.5, BIZ_INFO['website'], align='R')

    # ── Sender line (small gray) ──
    cy = MT + 42
    pdf.set_font('Helvetica', '', 7)
    pdf.set_text_color(150, 150, 150)
    sender_line = f"{BIZ_INFO['name']}  ·  {BIZ_INFO['street']}  ·  {BIZ_INFO['city']}"
    pdf.set_xy(ML, cy)
    pdf.cell(content_w, 3, sender_line)

    # ── Client address ──
    cy += 5
    pdf.set_text_color(29, 29, 29)
    pdf.set_font('Helvetica', 'B', 10)
    pdf.set_xy(ML, cy)
    pdf.cell(100, 5, doc_data.get('client_name', ''))
    cy += 5
    pdf.set_font('Helvetica', '', 9.5)
    client_addr = doc_data.get('client_address', '')
    if client_addr:
        for line in client_addr.split('\n'):
            pdf.set_xy(ML, cy)
            pdf.cell(100, 4.5, line.strip())
            cy += 4.5

    # ── Document metadata right column ──
    meta_label_x = RIGHT - 52
    my = MT + 48
    pdf.set_font('Helvetica', '', 8.5)

    is_offer = doc_type == 'offer'
    if is_offer:
        meta = [
            ('Estimate No.', doc_data.get('number', '')),
            ('Client No.', doc_data.get('client_id', '')),
            ('Date', doc_data.get('date_formatted', '')),
            ('Valid until', doc_data.get('valid_until', '')),
        ]
    else:
        meta = [
            ('Invoice No.', doc_data.get('number', '')),
            ('Client No.', doc_data.get('client_id', '')),
            ('Date', doc_data.get('date_formatted', '')),
        ]
        if doc_data.get('service_date'):
            meta.append(('Service Date', doc_data['service_date']))

    for label, val in meta:
        pdf.set_font('Helvetica', '', 8.5)
        pdf.set_text_color(100, 100, 100)
        pdf.set_xy(meta_label_x, my)
        pdf.cell(52, 5, label)
        pdf.set_font('Helvetica', 'B', 8.5)
        pdf.set_text_color(29, 29, 29)
        pdf.set_xy(meta_label_x, my)
        pdf.cell(52, 5, str(val or ''), align='R')
        my += 5.5

    # ── Document heading ──
    ty = max(cy + 10, my + 8)
    pdf.set_text_color(BRAND_R, BRAND_G, BRAND_B)
    pdf.set_font('Helvetica', 'B', 16)
    if is_offer:
        heading = 'ESTIMATE'
    elif doc_data.get('invoice_type') == 'deposit' and doc_data.get('deposit_label'):
        heading = doc_data['deposit_label'].upper()
    else:
        heading = 'INVOICE'
    pdf.set_xy(ML, ty)
    pdf.cell(content_w, 8, heading)

    # ── Project title ──
    ty += 8
    pdf.set_text_color(29, 29, 29)
    title = doc_data.get('title', '')
    if title:
        pdf.set_font('Helvetica', 'B', 11)
        pdf.set_xy(ML, ty)
        pdf.cell(content_w, 6, title)
        ty += 6

    # ── Location / date line ──
    loc = doc_data.get('location', '')
    evt_date = doc_data.get('event_date', '')
    if loc or evt_date:
        pdf.set_font('Helvetica', '', 8.5)
        pdf.set_text_color(100, 100, 100)
        parts = [loc, f'Date: {evt_date}' if evt_date else '']
        parts = [p for p in parts if p]
        pdf.set_xy(ML, ty)
        pdf.cell(content_w, 5, '  ·  '.join(parts))
        ty += 6

    # ── Description ──
    description = doc_data.get('description', '')
    if description:
        ty += 1
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(50, 50, 50)
        desc_lines = pdf.multi_cell(content_w, 4.2, description, dry_run=True, output="LINES")
        for dl in desc_lines:
            pdf.set_xy(ML, ty)
            pdf.cell(content_w, 4.2, dl)
            ty += 4.2
        ty += 4

    # ── Line items table ──
    ty += 2
    col_pos = ML
    col_desc = ML + 12
    col_qty = ML + 82
    col_unit = ML + 100
    col_price = ML + 122
    col_total = RIGHT

    # Table header background
    pdf.set_fill_color(242, 242, 242)
    pdf.rect(ML, ty - 4.5, content_w, 7, 'F')
    pdf.set_text_color(BRAND_R, BRAND_G, BRAND_B)
    pdf.set_font('Helvetica', 'B', 7.5)
    pdf.set_xy(col_pos + 1, ty - 1)
    pdf.cell(10, 3, 'Pos.')
    pdf.set_xy(col_desc, ty - 1)
    pdf.cell(40, 3, 'Description')
    pdf.set_xy(col_qty, ty - 1)
    pdf.cell(15, 3, 'Qty')
    pdf.set_xy(col_unit, ty - 1)
    pdf.cell(15, 3, 'Unit')
    pdf.set_xy(col_price, ty - 1)
    pdf.cell(20, 3, 'Unit Price')
    th_right = RIGHT - 1
    # Total € header right-aligned
    pdf.set_xy(col_price + 20, ty - 1)
    tw = th_right - (col_price + 20)
    pdf.cell(tw, 3, 'Total \u20ac', align='R')
    ty += 5

    # Separator
    pdf.set_draw_color(210, 210, 210)
    pdf.set_line_width(0.3)
    pdf.line(ML, ty - 1.5, RIGHT, ty - 1.5)
    ty += 2

    # Table rows
    pdf.set_text_color(29, 29, 29)
    for item in doc_data.get('items', []):
        if ty > 240:
            _draw_footer(pdf, W, H, ML, MR, BRAND_R, BRAND_G, BRAND_B)
            pdf.add_page()
            ty = MT + 10

        pos = str(item.get('pos', ''))
        desc = item.get('description', '')
        detail = item.get('detail', '')
        qty = item.get('qty', 0)
        unit = item.get('unit', '')
        unit_price = item.get('unit_price', 0)
        total = item.get('total', qty * unit_price)

        pdf.set_font('Helvetica', '', 9)
        pdf.set_xy(col_pos + 1, ty)
        pdf.cell(10, 5, pos)

        # Description bold
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_xy(col_desc, ty)
        pdf.cell(60, 5, desc)

        pdf.set_font('Helvetica', '', 9)
        pdf.set_xy(col_qty, ty)
        pdf.cell(15, 5, _fmt_eur_de(qty))
        pdf.set_xy(col_unit, ty)
        pdf.cell(15, 5, unit)
        pdf.set_xy(col_price, ty)
        pdf.cell(20, 5, _fmt_eur_de(unit_price))
        # Total right-aligned
        pdf.set_xy(col_price + 20, ty)
        tw2 = th_right - (col_price + 20)
        pdf.cell(tw2, 5, _fmt_eur_de(total), align='R')
        ty += 5

        # Detail line
        if detail:
            pdf.set_font('Helvetica', '', 8)
            pdf.set_text_color(120, 120, 120)
            detail_w = RIGHT - col_desc
            detail_lines = pdf.multi_cell(detail_w, 3.5, detail, dry_run=True, output="LINES")
            for dl in detail_lines:
                pdf.set_xy(col_desc, ty)
                pdf.cell(detail_w, 3.5, dl)
                ty += 3.5
            pdf.set_text_color(29, 29, 29)
        ty += 3

    # ── Totals section ──
    ty += 3
    totals_label_x = RIGHT - 60

    pdf.set_font('Helvetica', '', 9)
    pdf.set_text_color(60, 60, 60)
    pdf.set_xy(totals_label_x, ty)
    pdf.cell(30, 5, 'Subtotal (Netto)')
    pdf.set_xy(totals_label_x + 30, ty)
    pdf.cell(30, 5, _fmt_eur_de(doc_data.get('subtotal', 0)) + ' \u20ac', align='R')
    ty += 5

    pdf.set_xy(totals_label_x, ty)
    pdf.cell(30, 5, 'USt. 19%')
    pdf.set_xy(totals_label_x + 30, ty)
    pdf.cell(30, 5, _fmt_eur_de(doc_data.get('vat', 0)) + ' \u20ac', align='R')
    ty += 3

    # Green separator before total
    pdf.set_draw_color(BRAND_R, BRAND_G, BRAND_B)
    pdf.set_line_width(0.6)
    pdf.line(totals_label_x, ty, RIGHT, ty)
    ty += 6

    pdf.set_text_color(BRAND_R, BRAND_G, BRAND_B)
    pdf.set_font('Helvetica', 'B', 12)
    pdf.set_xy(totals_label_x, ty)
    pdf.cell(30, 6, 'Total (Brutto)')
    pdf.set_xy(totals_label_x + 30, ty)
    pdf.cell(30, 6, _fmt_eur_de(doc_data.get('total', 0)) + ' \u20ac', align='R')

    # Deposit reference
    if not is_offer and doc_data.get('invoice_type') == 'deposit' and doc_data.get('project_total', 0) > 0:
        ty += 7
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(60, 60, 60)
        pdf.set_xy(totals_label_x, ty)
        pdf.cell(60, 5, f"Total project value: {_fmt_eur_de(doc_data['project_total'])} \u20ac")

    # ── Payment terms ──
    if not is_offer:
        ty += 12
        if ty > 255:
            _draw_footer(pdf, W, H, ML, MR, BRAND_R, BRAND_G, BRAND_B)
            pdf.add_page()
            ty = MT + 10
        pdf.set_font('Helvetica', '', 8.5)
        pdf.set_text_color(60, 60, 60)
        pdf.set_xy(ML, ty)
        pdf.cell(content_w, 4, 'Zahlbar sofort, rein netto.')

    # ── Footer ──
    _draw_footer(pdf, W, H, ML, MR, BRAND_R, BRAND_G, BRAND_B)

    return pdf.output()


def _draw_footer(pdf, W, H, ML, MR, BR, BG, BB):
    """Draw the 3-column footer with green headers on the current page."""
    RIGHT = W - MR
    footer_top = H - 28

    # Green line
    pdf.set_draw_color(BR, BG, BB)
    pdf.set_line_width(0.5)
    pdf.line(ML, footer_top, RIGHT, footer_top)

    col1_x = ML
    col2_x = ML + 55
    col3_x = ML + 115
    fy = footer_top + 5

    # Column headers
    pdf.set_font('Helvetica', 'B', 7)
    pdf.set_text_color(BR, BG, BB)
    pdf.set_xy(col1_x, fy)
    pdf.cell(50, 3, 'Josef Sindelka')
    pdf.set_xy(col2_x, fy)
    pdf.cell(50, 3, 'Tax Information')
    pdf.set_xy(col3_x, fy)
    pdf.cell(50, 3, 'Bank Details')

    # Column content
    fy += 4
    pdf.set_font('Helvetica', '', 6.5)
    pdf.set_text_color(100, 100, 100)

    # Col 1: Address
    pdf.set_xy(col1_x, fy)
    pdf.cell(50, 3, BIZ_INFO['street'])
    pdf.set_xy(col1_x, fy + 3)
    pdf.cell(50, 3, BIZ_INFO['city'])

    # Col 2: Tax
    pdf.set_xy(col2_x, fy)
    pdf.cell(50, 3, f"USt-IdNr.: {BIZ_INFO['ust_id']}")
    pdf.set_xy(col2_x, fy + 3)
    pdf.cell(50, 3, f"Steuernr.: {BIZ_INFO['steuernummer']}")

    # Col 3: Bank
    pdf.set_xy(col3_x, fy)
    pdf.cell(50, 3, f"Bank: {BIZ_INFO['bank']}")
    pdf.set_xy(col3_x, fy + 3)
    pdf.cell(50, 3, f"IBAN: {BIZ_INFO['iban']}")
    pdf.set_xy(col3_x, fy + 6)
    pdf.cell(50, 3, f"BIC: {BIZ_INFO['bic']}")


def _get_offers_folder_id():
    """Get or create the OFFERS 2026 folder in Google Drive."""
    year_folder = _get_year_folder()
    if not year_folder:
        return None
    return _drive_get_or_create_folder(year_folder, OFFERS_FOLDER)


# ---------- Tab: Invoices / Offers ----------

def tab_invoices_offers(data):
    """Invoice & Offer Generator tab with Dashboard, New Invoice, New Offer, and Clients sub-tabs."""
    import json as _json

    # Ensure clients are seeded
    clients = _seed_clients_if_empty()
    if not clients:
        clients = _load_clients_db()

    # ── Helper: build unified document list ──────────────────────
    def _build_doc_list():
        """Merge paid invoices, unpaid invoices, offers, and drafts into a
        single list of dicts with keys: number, type, client, project, amount,
        date, status, source.  source = 'income_paid' | 'income_unpaid' |
        'offer' | 'draft'."""
        docs = []

        # -- paid invoices from Income sheet
        paid = data.get('income_paid')
        if paid is not None and not paid.empty:
            for _, r in paid.iterrows():
                inv_num = str(r.get('Invoice Number', '')).strip()
                if not inv_num:
                    continue
                docs.append({
                    'number': f"RE{inv_num}" if not str(inv_num).startswith('RE') else inv_num,
                    'type': 'Invoice',
                    'client': str(r.get('Client', '')),
                    'project': str(r.get('Project', '')),
                    'amount': float(r.get('Brutto (€)', 0) or 0),
                    'netto': float(r.get('Netto (€)', 0) or 0),
                    'date': str(r.get('Date', '')).split(' ')[0].split('T')[0],
                    'status': 'PAID',
                    'category': str(r.get('Category', '')),
                    'source': 'income_paid',
                })

        # -- unpaid invoices from Income sheet
        unpaid = data.get('income_unpaid')
        if unpaid is not None and not unpaid.empty:
            for _, r in unpaid.iterrows():
                inv_num = str(r.get('Invoice Number', '')).strip()
                if not inv_num:
                    continue
                docs.append({
                    'number': f"RE{inv_num}" if not str(inv_num).startswith('RE') else inv_num,
                    'type': 'Invoice',
                    'client': str(r.get('Client', '')),
                    'project': str(r.get('Project', '')),
                    'amount': float(r.get('Brutto (€)', 0) or 0),
                    'netto': float(r.get('Netto (€)', 0) or 0),
                    'date': str(r.get('Date', '')).split(' ')[0].split('T')[0],
                    'status': 'SENT',
                    'category': str(r.get('Category', '')),
                    'source': 'income_unpaid',
                })

        # -- offers
        offers = _load_offers()
        seen_numbers = {d['number'] for d in docs}
        for o in offers:
            onum = str(o.get('Offer Number', '')).strip()
            if not onum:
                continue
            docs.append({
                'number': onum,
                'type': 'Offer',
                'client': str(o.get('Client', '')),
                'project': str(o.get('Project', '')),
                'amount': float(o.get('Brutto', 0) or 0),
                'netto': float(o.get('Netto', 0) or 0),
                'date': str(o.get('Date', '')).split(' ')[0].split('T')[0],
                'status': str(o.get('Status', 'SENT')).upper(),
                'category': str(o.get('Category', '')),
                'source': 'offer',
            })
            seen_numbers.add(onum)

        # -- drafts from DocumentMeta (only add if not already present)
        metas = _load_document_meta()
        for m in metas:
            dnum = str(m.get('Doc Number', '')).strip()
            if not dnum or dnum in seen_numbers:
                continue
            dtype = str(m.get('Doc Type', 'Invoice'))
            docs.append({
                'number': dnum,
                'type': dtype,
                'client': str(m.get('Client', '')),
                'project': str(m.get('Project', '')),
                'amount': float(m.get('Brutto', 0) or 0),
                'netto': float(m.get('Netto', 0) or 0),
                'date': str(m.get('Date', '')).split(' ')[0].split('T')[0],
                'status': str(m.get('Status', 'DRAFT')).upper(),
                'category': str(m.get('Category', '')),
                'source': 'draft',
            })

        return docs

    # ── Status-change dialog ─────────────────────────────────────
    @st.dialog("Change Document Status")
    def _dlg_change_status(doc_number, doc_type, current_status):
        t = _t()
        st.markdown(f"**Document:** {doc_number}")
        st.markdown(f"**Current status:** {current_status}")
        if doc_type == 'Offer':
            options = ['DRAFT', 'SENT', 'ACCEPTED', 'DECLINED', 'EXPIRED']
        else:
            options = ['DRAFT', 'SENT', 'PAID']
        new_status = st.selectbox("New Status", options, key=f'dlg_status_{doc_number}')
        if st.button("Update", type="primary", key=f'dlg_status_ok_{doc_number}'):
            if doc_type == 'Offer':
                _update_offer_status(doc_number, new_status)
                _log_activity('OFFER_STATUS', f"{doc_number} -> {new_status}")
            else:
                inv_num_raw = doc_number.replace('RE', '')
                if current_status == 'DRAFT' and new_status in ('SENT', 'PAID'):
                    # Draft becoming real invoice — add to Income sheet
                    meta = _get_document_meta(doc_number)
                    if meta:
                        inv_data_for_sheet = {
                            'id': '',
                            'invoice_number': int(inv_num_raw) if inv_num_raw.isdigit() else inv_num_raw,
                            'date': datetime.strptime(str(meta.get('Date', '')), '%Y-%m-%d') if meta.get('Date') else datetime.today(),
                            'month': MONTHS[datetime.today().month - 1],
                            'client': meta.get('Client', ''),
                            'project': meta.get('Project', ''),
                            'category': meta.get('Category', INCOME_CATEGORIES[0]),
                            'netto': float(meta.get('Netto', 0) or 0),
                            'brutto': float(meta.get('Brutto', 0) or 0),
                        }
                        target_status = 'paid' if new_status == 'PAID' else 'unpaid'
                        add_invoice_to_excel(inv_data_for_sheet, status=target_status)
                        _invalidate_data_caches()
                elif current_status == 'SENT' and new_status == 'PAID':
                    update_invoice_status_in_excel(inv_num_raw, 'paid')
                    _invalidate_data_caches()
                elif current_status == 'PAID' and new_status == 'SENT':
                    update_invoice_status_in_excel(inv_num_raw, 'unpaid')
                    _invalidate_data_caches()
                _log_activity('INVOICE_STATUS', f"{doc_number} -> {new_status}")
            # Update DocumentMeta status too
            meta_existing = _get_document_meta(doc_number)
            if meta_existing:
                _save_document_meta(doc_number, doc_type, {
                    **{k.lower().replace(' ', '_'): v for k, v in meta_existing.items()},
                    'status': new_status,
                })
            st.success(f"Status updated to **{new_status}**.")
            st.rerun()

    # ── Delete-document dialog ───────────────────────────────────
    @st.dialog("Delete Document")
    def _dlg_delete_doc(doc_number, doc_type, source):
        t = _t()
        st.warning(f"Are you sure you want to delete **{doc_number}**? This cannot be undone.")
        bc1, bc2 = st.columns(2)
        with bc1:
            if st.button("Cancel", use_container_width=True, key=f'dlg_del_cancel_{doc_number}'):
                st.rerun()
        with bc2:
            if st.button("Delete", type="primary", use_container_width=True, key=f'dlg_del_ok_{doc_number}'):
                if doc_type == 'Offer':
                    _delete_offer_from_sheet(doc_number)
                    _log_activity('OFFER_DELETED', doc_number)
                else:
                    inv_num_raw = doc_number.replace('RE', '')
                    if source != 'draft':
                        remove_invoice_from_excel(inv_num_raw)
                        _delete_invoice_pdf(inv_num_raw)
                    _log_activity('INVOICE_DELETED', doc_number)
                # Always clean up DocumentMeta
                _delete_document_meta(doc_number)
                _invalidate_data_caches()
                st.success(f"**{doc_number}** deleted.")
                st.rerun()

    # ── Convert Offer to Invoice dialog ───────────────────────
    @st.dialog("Convert Offer to Invoice")
    def _dlg_convert_offer_to_invoice(offer_number):
        t = _t()
        # Load offer data
        offers = _load_offers()
        offer = None
        for o in offers:
            if str(o.get('Offer Number', '')).strip() == offer_number:
                offer = o
                break

        if not offer:
            st.error(f"Offer {offer_number} not found.")
            return

        st.markdown(f"**Offer:** {offer_number}")
        _conv_client = offer.get('Client', '')
        _conv_project = offer.get('Project', '')
        _conv_brutto = float(offer.get('Brutto', 0) or 0)
        st.markdown(f"**Client:** {_conv_client}")
        st.markdown(f"**Project:** {_conv_project}")
        _conv_amt_str = _fmt_eur_de(_conv_brutto)
        st.markdown(f"**Amount:** {_conv_amt_str} EUR")
        st.info("This will create a new **invoice** (unpaid) from this offer. The original offer PDF will be kept in the offers folder.")

        if st.button("Convert to Invoice", type="primary", key=f'dlg_convert_{offer_number}'):
            try:
                import json as _json_conv

                # 1. Get new invoice number
                inv_number = _next_invoice_number()
                inv_num_raw = inv_number.replace('RE', '')

                # 2. Get offer metadata from DocumentMeta if available
                meta = _get_document_meta(offer_number)

                # 3. Build invoice data for PDF
                netto = float(offer.get('Netto', 0) or 0)
                brutto = _conv_brutto
                vat = brutto - netto

                # Parse items from offer meta or create a single line item
                items = []
                items_json_str = ''
                if meta and meta.get('Items JSON'):
                    items_json_str = meta['Items JSON']
                elif offer.get('Items JSON'):
                    items_json_str = offer['Items JSON']

                if items_json_str:
                    try:
                        items = _json_conv.loads(items_json_str)
                    except Exception:
                        items = []

                if not items:
                    items = [{'pos': 1, 'description': _conv_project or 'Services',
                              'detail': '', 'qty': 1, 'unit': 'pcs',
                              'unit_price': netto, 'total': netto}]

                today = datetime.today()
                date_str = today.strftime('%Y-%m-%d')

                meta_client_addr = meta.get('Client Address', '') if meta else ''
                meta_desc = meta.get('Description', '') if meta else ''
                meta_location = meta.get('Location', '') if meta else ''
                meta_event_date = meta.get('Event Date', '') if meta else ''
                meta_service_date = meta.get('Service Date', '') if meta else date_str

                doc_data = {
                    'number': inv_number,
                    'date': today,
                    'client_name': _conv_client,
                    'client_address': meta_client_addr,
                    'client_id': '',
                    'title': _conv_project,
                    'description': meta_desc,
                    'location': meta_location,
                    'event_date': meta_event_date,
                    'service_date': meta_service_date,
                    'items': items,
                    'subtotal': netto,
                    'vat': vat,
                    'total': brutto,
                    'invoice_type': 'standard',
                    'deposit_label': '',
                    'project_total': 0,
                }

                # 4. Generate PDF
                pdf_bytes = _generate_document_pdf('invoice', doc_data)
                filename = f"notpaid_Rechnung_JosefSindelka_{inv_number}.pdf"

                # 5. Upload to invoices folder in Drive
                folder_id = _get_invoices_folder_id()
                if folder_id:
                    _drive_upload_bytes(folder_id, filename, pdf_bytes)

                # 6. Add to Income sheet as unpaid
                _conv_category = offer.get('Category', INCOME_CATEGORIES[0] if INCOME_CATEGORIES else 'Other')
                inv_data_for_sheet = {
                    'id': '',
                    'invoice_number': int(inv_num_raw) if inv_num_raw.isdigit() else inv_num_raw,
                    'date': today,
                    'month': MONTHS[today.month - 1],
                    'client': _conv_client,
                    'project': _conv_project,
                    'category': _conv_category,
                    'netto': netto,
                    'brutto': brutto,
                }
                add_invoice_to_excel(inv_data_for_sheet, status='unpaid')

                # 7. Save invoice metadata
                _save_document_meta(inv_number, 'Invoice', {
                    'client': _conv_client,
                    'client_address': meta_client_addr,
                    'project': _conv_project,
                    'category': offer.get('Category', ''),
                    'description': meta_desc,
                    'date': date_str,
                    'location': meta_location,
                    'event_date': meta_event_date,
                    'service_date': meta_service_date,
                    'invoice_type': 'standard',
                    'deposit_label': '',
                    'project_total': 0,
                    'validity': '',
                    'netto': netto,
                    'brutto': brutto,
                    'items_json': items_json_str if items_json_str else _json_conv.dumps(items),
                    'status': 'SENT',
                })

                # 8. Update offer status to ACCEPTED (since it's being converted)
                _update_offer_status(offer_number, 'ACCEPTED')

                _invalidate_data_caches()
                _log_activity('OFFER_TO_INVOICE', f"{offer_number} -> {inv_number}")
                st.success(f"Invoice **{inv_number}** created from offer {offer_number}.")
                st.download_button("Download Invoice PDF", data=pdf_bytes,
                                  file_name=filename, mime='application/pdf',
                                  key='conv_dl')
                st.rerun()
            except Exception as e:
                st.error(f"Conversion failed: {e}")

    # ── Sub-tabs ─────────────────────────────────────────────────
    sub0, sub1, sub2, sub3 = st.tabs([
        'DASHBOARD', 'NEW INVOICE', 'NEW OFFER', 'CLIENTS'
    ])

    # ────────────────────────────────────────────────────────────
    # SUB-TAB 0: DASHBOARD
    # ────────────────────────────────────────────────────────────
    with sub0:
        t = _t()
        all_docs = _build_doc_list()

        # KPI calculations
        total_offers = sum(1 for d in all_docs if d['type'] == 'Offer')
        total_invoices = sum(1 for d in all_docs if d['type'] == 'Invoice')
        revenue_paid = sum(d['amount'] for d in all_docs if d['type'] == 'Invoice' and d['status'] == 'PAID')
        pending_amount = sum(d['amount'] for d in all_docs if d['status'] in ('SENT', 'DRAFT'))

        k1, k2, k3, k4 = st.columns(4)
        with k1:
            metric_card("Total Offers", total_offers)
        with k2:
            metric_card("Total Invoices", total_invoices)
        with k3:
            metric_card("Revenue (Paid)", revenue_paid, color_class='green')
        with k4:
            metric_card("Pending", pending_amount, color_class='orange')

        st.markdown("")

        # ── Recent Documents heading + filter ──
        _text_c = t['text']
        st.markdown(f"<h4 style='color:{_text_c};font-family:{FONT};margin-bottom:0.2rem'>Recent Documents</h4>",
                    unsafe_allow_html=True)

        filter_val = st.radio("Filter", ['All', 'Offers', 'Invoices', 'Draft', 'Sent', 'Paid'],
                              horizontal=True, key='dash_filter', label_visibility='collapsed')

        # Apply filter
        if filter_val == 'All':
            filtered = all_docs
        elif filter_val == 'Offers':
            filtered = [d for d in all_docs if d['type'] == 'Offer']
        elif filter_val == 'Invoices':
            filtered = [d for d in all_docs if d['type'] == 'Invoice']
        elif filter_val == 'Draft':
            filtered = [d for d in all_docs if d['status'] == 'DRAFT']
        elif filter_val == 'Sent':
            filtered = [d for d in all_docs if d['status'] == 'SENT']
        elif filter_val == 'Paid':
            filtered = [d for d in all_docs if d['status'] == 'PAID']
        else:
            filtered = all_docs

        # Sort by date descending (most recent first)
        def _sort_key(d):
            try:
                return datetime.strptime(d['date'], '%Y-%m-%d')
            except Exception:
                try:
                    return datetime.strptime(d['date'], '%d.%m.%Y')
                except Exception:
                    return datetime(2000, 1, 1)
        filtered.sort(key=_sort_key, reverse=True)

        if not filtered:
            st.info("No documents match the selected filter.")
        else:
            # Status badge colors
            _status_colors = {
                'PAID': C_GREEN,
                'SENT': C_BLUE,
                'DRAFT': _t()['muted'],
                'ACCEPTED': C_GREEN,
                'DECLINED': C_RED,
                'EXPIRED': C_RED,
            }

            # ── Column header row ──
            _t_muted = t["muted"]
            _t_text = t["text"]
            _t_text2 = t["text_secondary"]
            _t_border = t["border"]
            _hdr_style = f"font-size:0.65rem;font-weight:600;letter-spacing:0.06em;color:{_t_muted};font-family:{FONT}"
            hdr_cols = st.columns([1.2, 0.8, 1.5, 1.5, 1.2, 1, 0.8, 2])
            hdr_labels = ['NUMBER', 'TYPE', 'CLIENT', 'PROJECT', 'AMOUNT', 'DATE', 'STATUS', 'ACTIONS']
            for hc, hl in zip(hdr_cols, hdr_labels):
                with hc:
                    st.markdown(f'<span style="{_hdr_style}">{hl}</span>',
                               unsafe_allow_html=True)

            st.markdown(f'<hr style="margin:0.2rem 0 0.4rem 0;border:none;border-top:1px solid {_t_border}">',
                       unsafe_allow_html=True)

            # ── Document rows ──
            _style_bold = f"font-weight:600;font-size:0.78rem;color:{_t_text}"
            _style_text = f"font-size:0.78rem;color:{_t_text}"
            _style_sec = f"font-size:0.78rem;color:{_t_text2}"
            _style_amt = f"font-size:0.78rem;font-weight:500;color:{_t_text};font-variant-numeric:tabular-nums"

            for idx, doc in enumerate(filtered):
                dn = doc['number']
                s_color = _status_colors.get(doc['status'], t['muted'])
                # Pre-compute RGB for badge background
                _sc_hex = s_color.lstrip("#")
                _sc_r = str(int(_sc_hex[0:2], 16))
                _sc_g = str(int(_sc_hex[2:4], 16))
                _sc_b = str(int(_sc_hex[4:6], 16))
                _sc_rgba = f"{_sc_r},{_sc_g},{_sc_b}"
                _doc_status = doc["status"]
                _badge_style = f"color:{s_color};background:rgba({_sc_rgba},0.15);padding:2px 10px;border-radius:10px;font-size:0.72rem;font-weight:600"

                rc = st.columns([1.2, 0.8, 1.5, 1.5, 1.2, 1, 0.8, 2])
                with rc[0]:
                    st.markdown(f'<span style="{_style_bold}">{dn}</span>', unsafe_allow_html=True)
                with rc[1]:
                    st.markdown(f'<span style="{_style_sec}">{doc["type"]}</span>', unsafe_allow_html=True)
                with rc[2]:
                    st.markdown(f'<span style="{_style_text}">{doc["client"]}</span>', unsafe_allow_html=True)
                with rc[3]:
                    st.markdown(f'<span style="{_style_sec}">{doc["project"]}</span>', unsafe_allow_html=True)
                _amt_str = _fmt_eur_de(doc['amount'])
                with rc[4]:
                    st.markdown(f'<span style="{_style_amt}">{_amt_str} €</span>', unsafe_allow_html=True)
                with rc[5]:
                    st.markdown(f'<span style="{_style_sec}">{doc["date"]}</span>', unsafe_allow_html=True)
                with rc[6]:
                    st.markdown(f'<span style="{_badge_style}">{_doc_status}</span>', unsafe_allow_html=True)

                # Action buttons in last column
                with rc[7]:
                    bc1, bc2, bc3, bc4 = st.columns(4)
                    with bc1:
                        if st.button("📄", key=f"pdf_{dn}", help="Download PDF"):
                            pdf_bytes = None
                            try:
                                if doc['type'] == 'Offer':
                                    fid = _get_offers_folder_id()
                                else:
                                    fid = _get_invoices_folder_id()
                                if fid:
                                    files = _drive_list_files(fid)
                                    search_key = dn.replace('RE', '').replace('AG', '')
                                    for f in files:
                                        if search_key in f['name'] and f['name'].endswith('.pdf'):
                                            pdf_bytes = _drive_download_bytes(f['id'])
                                            st.download_button(
                                                "⬇",
                                                data=pdf_bytes,
                                                file_name=f['name'],
                                                mime='application/pdf',
                                                key=f"dl_{dn}",
                                            )
                                            break
                                    if pdf_bytes is None:
                                        st.caption("Not found")
                            except Exception:
                                st.caption("Err")
                    with bc2:
                        if st.button("✏️", key=f"edit_{dn}", help="Edit"):
                            meta = _get_document_meta(dn)
                            if meta:
                                if doc['type'] == 'Offer':
                                    st.session_state['edit_offer'] = meta
                                else:
                                    st.session_state['edit_invoice'] = meta
                                st.toast(f"Edit form pre-filled for {dn}")
                            else:
                                st.toast("No editable metadata found.")
                    with bc3:
                        if doc['type'] == 'Offer' and doc['status'] != 'DRAFT':
                            btn_label = "🔄"
                            btn_help = "Convert to Invoice"
                        else:
                            btn_label = "📊"
                            btn_help = "Change Status"
                        if st.button(btn_label, key=f"status_{dn}", help=btn_help):
                            if doc['type'] == 'Offer' and doc['status'] != 'DRAFT':
                                _dlg_convert_offer_to_invoice(dn)
                            else:
                                _dlg_change_status(dn, doc['type'], doc['status'])
                    with bc4:
                        if st.button("🗑", key=f"del_{dn}", help="Delete"):
                            _dlg_delete_doc(dn, doc['type'], doc['source'])

    # ────────────────────────────────────────────────────────────
    # SUB-TAB 1: NEW INVOICE
    # ────────────────────────────────────────────────────────────
    with sub1:
        t = _t()
        st.markdown("#### Create Invoice (Rechnung)")
        st.caption("Generate a professional PDF invoice and save to Google Drive.")

        # Check for edit pre-fill
        edit_inv = st.session_state.pop('edit_invoice', None)

        # Container 1: Client + Address
        with st.container(border=True):
            ci1, ci2 = st.columns(2)
            with ci1:
                client_names = ['-- Select Client --'] + [c['name'] for c in clients]
                default_client_idx = 0
                if edit_inv:
                    edit_client_name = str(edit_inv.get('Client', '')).strip()
                    for i, cn in enumerate(client_names):
                        if cn == edit_client_name:
                            default_client_idx = i
                            break
                sel_client_idx = st.selectbox("Client", range(len(client_names)),
                                               format_func=lambda i: client_names[i],
                                               index=default_client_idx,
                                               key='inv_client_sel')
            with ci2:
                if sel_client_idx > 0:
                    sel_client = clients[sel_client_idx - 1]
                    inv_client_name = sel_client['name']
                    inv_client_addr = st.text_area("Client Address",
                                                    value=sel_client.get('address', ''),
                                                    height=68, key='inv_client_addr')
                    inv_client_id = sel_client['id']
                else:
                    inv_client_name = st.text_input("Client Name (new)",
                                                     value=str(edit_inv.get('Client', '')) if edit_inv and default_client_idx == 0 else '',
                                                     key='inv_client_name_new')
                    inv_client_addr = st.text_area("Client Address", height=68,
                                                    value=str(edit_inv.get('Client Address', '')) if edit_inv else '',
                                                    key='inv_client_addr_new')
                    inv_client_id = ''

        # Container 2: Project Title + Invoice Date
        with st.container(border=True):
            ci3, ci4 = st.columns(2)
            with ci3:
                inv_title = st.text_input("Project Title",
                                           value=str(edit_inv.get('Project', '')) if edit_inv else '',
                                           key='inv_title')
            with ci4:
                default_date = datetime.today()
                if edit_inv and edit_inv.get('Date'):
                    try:
                        default_date = datetime.strptime(str(edit_inv['Date']), '%Y-%m-%d')
                    except Exception:
                        pass
                inv_date = st.date_input("Invoice Date", value=default_date, key='inv_date')

        # Container 3: Project Description
        with st.container(border=True):
            inv_description = st.text_area("Project Description",
                                            value=str(edit_inv.get('Description', '')) if edit_inv else '',
                                            height=80, key='inv_desc')

        # Container 4: Category + Service Date
        with st.container(border=True):
            ci5, ci6 = st.columns(2)
            with ci5:
                default_cat_idx = 0
                if edit_inv and edit_inv.get('Category'):
                    try:
                        default_cat_idx = INCOME_CATEGORIES.index(str(edit_inv['Category']))
                    except ValueError:
                        pass
                inv_category = st.selectbox("Category", INCOME_CATEGORIES,
                                             index=default_cat_idx, key='inv_cat')
            with ci6:
                inv_service_date = st.text_input("Service Date", placeholder="e.g. February 2026",
                                                  value=str(edit_inv.get('Service Date', '')) if edit_inv else '',
                                                  key='inv_service_date')

        # Expander: Additional Details
        with st.expander("Additional Details", expanded=bool(edit_inv)):
            ad1, ad2 = st.columns(2)
            with ad1:
                inv_location = st.text_input("Location (optional)",
                                              value=str(edit_inv.get('Location', '')) if edit_inv else '',
                                              key='inv_loc')
            with ad2:
                inv_event_date = st.text_input("Event Date (optional)",
                                                value=str(edit_inv.get('Event Date', '')) if edit_inv else '',
                                                key='inv_event_date')

            inv_type_options = ['Standard', 'Deposit (Abschlagsrechnung)']
            default_inv_type = 0
            if edit_inv and str(edit_inv.get('Invoice Type', '')).lower() == 'deposit':
                default_inv_type = 1
            inv_type = st.selectbox("Invoice Type", inv_type_options,
                                     index=default_inv_type, key='inv_type')
            is_deposit = inv_type.startswith('Deposit')
            deposit_label = ''
            project_total = 0.0
            if is_deposit:
                dc1, dc2 = st.columns(2)
                with dc1:
                    deposit_label = st.text_input("Deposit Label",
                                                   value=str(edit_inv.get('Deposit Label', 'ABSCHLAGSRECHNUNG')) if edit_inv else 'ABSCHLAGSRECHNUNG',
                                                   key='inv_dep_label')
                with dc2:
                    project_total = st.number_input("Total Project Value (EUR)", min_value=0.0,
                                                     step=100.0, format="%.2f",
                                                     value=float(edit_inv.get('Project Total', 0) or 0) if edit_inv else 0.0,
                                                     key='inv_proj_total')

        # Line items
        _text_c = t['text']
        st.markdown(f"<h5 style='color:{_text_c};font-family:{FONT};margin-top:1rem'>Line Items</h5>",
                    unsafe_allow_html=True)
        if 'inv_items' not in st.session_state:
            if edit_inv and edit_inv.get('Items JSON'):
                try:
                    loaded_items = _json.loads(str(edit_inv['Items JSON']))
                    st.session_state['inv_items'] = [
                        {'description': it.get('description', ''), 'detail': it.get('detail', ''),
                         'qty': float(it.get('qty', 1)), 'unit': it.get('unit', 'pcs'),
                         'unit_price': float(it.get('unit_price', 0))}
                        for it in loaded_items
                    ] if loaded_items else [{'description': '', 'detail': '', 'qty': 1.0, 'unit': 'pcs', 'unit_price': 0.0}]
                except Exception:
                    st.session_state['inv_items'] = [{'description': '', 'detail': '', 'qty': 1.0,
                                                       'unit': 'pcs', 'unit_price': 0.0}]
            else:
                st.session_state['inv_items'] = [{'description': '', 'detail': '', 'qty': 1.0,
                                                   'unit': 'pcs', 'unit_price': 0.0}]

        items_to_remove = None
        for idx, item in enumerate(st.session_state['inv_items']):
            ic1, ic2, ic3, ic4, ic5 = st.columns([3, 1, 1, 1.5, 0.5])
            with ic1:
                item['description'] = st.text_input("Description", value=item['description'],
                                                     key=f'inv_item_desc_{idx}')
            with ic2:
                item['qty'] = st.number_input("Qty", value=float(item['qty']), min_value=0.0,
                                               step=0.5, format="%.1f", key=f'inv_item_qty_{idx}')
            with ic3:
                item['unit'] = st.text_input("Unit", value=item['unit'],
                                              key=f'inv_item_unit_{idx}')
            with ic4:
                item['unit_price'] = st.number_input("Price (EUR)", value=float(item['unit_price']),
                                                      min_value=0.0, step=10.0, format="%.2f",
                                                      key=f'inv_item_price_{idx}')
            with ic5:
                if idx > 0:
                    if st.button("x", key=f'inv_item_rm_{idx}'):
                        items_to_remove = idx

            item['detail'] = st.text_input("Detail (optional)", value=item.get('detail', ''),
                                            key=f'inv_item_detail_{idx}')

        if items_to_remove is not None:
            st.session_state['inv_items'].pop(items_to_remove)
            st.rerun()

        if st.button("+ Add Line Item", key='inv_add_line'):
            st.session_state['inv_items'].append({'description': '', 'detail': '', 'qty': 1.0,
                                                   'unit': 'pcs', 'unit_price': 0.0})
            st.rerun()

        # Calculate totals
        line_items = []
        for idx, item in enumerate(st.session_state['inv_items']):
            total_li = item['qty'] * item['unit_price']
            line_items.append({
                'pos': idx + 1,
                'description': item['description'],
                'detail': item.get('detail', ''),
                'qty': item['qty'],
                'unit': item['unit'],
                'unit_price': item['unit_price'],
                'total': total_li,
            })
        subtotal = sum(it['total'] for it in line_items)
        vat = round(subtotal * VAT_RATE, 2)
        total = round(subtotal + vat, 2)

        # Totals display
        st.markdown("---")
        tc1, tc2, tc3 = st.columns(3)
        with tc1:
            st.metric("Subtotal (Netto)", f"{_fmt_eur_de(subtotal)} EUR")
        with tc2:
            st.metric("USt. 19%", f"{_fmt_eur_de(vat)} EUR")
        with tc3:
            st.metric("Total (Brutto)", f"{_fmt_eur_de(total)} EUR")

        st.markdown("---")

        # Two action buttons
        btn1, btn2 = st.columns(2)
        with btn1:
            generate_inv = st.button("Generate PDF", type="primary", use_container_width=True,
                                      key='inv_generate')
        with btn2:
            save_draft_inv = st.button("Save as Draft", use_container_width=True,
                                        key='inv_save_draft')

        # ── Generate Invoice PDF ──
        if generate_inv:
            client_name = inv_client_name.strip() if inv_client_name else ''
            if not client_name:
                st.error("Please select or enter a client name.")
            elif subtotal <= 0:
                st.error("Please add at least one line item with a price.")
            else:
                with st.spinner("Generating invoice..."):
                    client = _get_or_create_client(client_name, inv_client_addr)
                    inv_number = _next_invoice_number()
                    date_formatted = inv_date.strftime('%d.%m.%Y')
                    date_iso = inv_date.strftime('%Y-%m-%d')

                    doc_data = {
                        'number': inv_number,
                        'date': date_iso,
                        'date_formatted': date_formatted,
                        'client_name': client['name'],
                        'client_address': inv_client_addr,
                        'client_id': client['id'],
                        'title': inv_title,
                        'description': inv_description,
                        'location': inv_location,
                        'event_date': inv_event_date,
                        'service_date': inv_service_date,
                        'items': line_items,
                        'subtotal': subtotal,
                        'vat': vat,
                        'total': total,
                        'invoice_type': 'deposit' if is_deposit else 'standard',
                        'deposit_label': deposit_label if is_deposit else '',
                        'project_total': project_total if is_deposit else 0,
                    }

                    pdf_bytes = _generate_document_pdf('invoice', doc_data)
                    filename = f"notpaid_Rechnung_JosefSindelka_{inv_number}.pdf"

                    # Upload to Google Drive
                    try:
                        folder_id = _get_invoices_folder_id()
                        if folder_id:
                            _drive_upload_bytes(folder_id, filename, pdf_bytes)
                            st.success(f"Invoice **{inv_number}** uploaded to Drive: `{INVOICES_FOLDER}/{filename}`")
                        else:
                            st.warning("Could not find INVOICES folder -- PDF generated but not uploaded.")
                    except Exception as e:
                        st.error(f"Drive upload failed: {e}")

                    # Add to Income sheet (unpaid)
                    try:
                        sheet_inv_num = inv_number.replace('RE', '')
                        inv_data_for_sheet = {
                            'id': '',
                            'invoice_number': int(sheet_inv_num) if sheet_inv_num.isdigit() else sheet_inv_num,
                            'date': datetime(inv_date.year, inv_date.month, inv_date.day),
                            'month': MONTHS[inv_date.month - 1],
                            'client': client['name'],
                            'project': inv_title,
                            'category': inv_category,
                            'netto': subtotal,
                            'brutto': total,
                        }
                        add_invoice_to_excel(inv_data_for_sheet, status='unpaid')
                        _invalidate_data_caches()
                        st.success("Added to Income sheet as unpaid invoice.")
                    except Exception as e:
                        st.error(f"Could not add to Income sheet: {e}")

                    # Save metadata
                    _save_document_meta(inv_number, 'Invoice', {
                        'client': client['name'], 'client_address': inv_client_addr,
                        'project': inv_title, 'category': inv_category,
                        'description': inv_description, 'date': date_iso,
                        'location': inv_location, 'event_date': inv_event_date,
                        'service_date': inv_service_date,
                        'invoice_type': 'deposit' if is_deposit else 'standard',
                        'deposit_label': deposit_label if is_deposit else '',
                        'project_total': project_total if is_deposit else 0,
                        'validity': '', 'netto': subtotal, 'brutto': total,
                        'items_json': _json.dumps(line_items), 'status': 'SENT',
                    })

                    _log_activity('INVOICE_CREATED', f"{inv_number} | {client['name']} | {_fmt_eur_de(total)} EUR")

                    st.download_button("Download PDF", data=pdf_bytes, file_name=filename,
                                        mime='application/pdf', key='inv_download')

                    st.session_state['inv_items'] = [{'description': '', 'detail': '', 'qty': 1.0,
                                                       'unit': 'pcs', 'unit_price': 0.0}]

        # ── Save Invoice as Draft ──
        if save_draft_inv:
            client_name = inv_client_name.strip() if inv_client_name else ''
            if not client_name:
                st.error("Please enter a client name to save a draft.")
            else:
                inv_number = _next_invoice_number()
                date_iso = inv_date.strftime('%Y-%m-%d')
                _save_document_meta(inv_number, 'Invoice', {
                    'client': client_name, 'client_address': inv_client_addr,
                    'project': inv_title, 'category': inv_category,
                    'description': inv_description, 'date': date_iso,
                    'location': inv_location, 'event_date': inv_event_date,
                    'service_date': inv_service_date,
                    'invoice_type': 'deposit' if is_deposit else 'standard',
                    'deposit_label': deposit_label if is_deposit else '',
                    'project_total': project_total if is_deposit else 0,
                    'validity': '', 'netto': subtotal, 'brutto': total,
                    'items_json': _json.dumps(line_items), 'status': 'DRAFT',
                })
                _log_activity('INVOICE_DRAFT', f"{inv_number} | {client_name} | {_fmt_eur_de(total)} EUR")
                st.success(f"Draft **{inv_number}** saved. You can find it in the Dashboard.")

    # ────────────────────────────────────────────────────────────
    # SUB-TAB 2: NEW OFFER
    # ────────────────────────────────────────────────────────────
    with sub2:
        t = _t()
        st.markdown("#### Create Offer (Angebot)")
        st.caption("Generate a professional PDF offer and save to Google Drive.")

        # Check for edit pre-fill
        edit_off = st.session_state.pop('edit_offer', None)

        # Container 1: Client + Address
        with st.container(border=True):
            co1, co2 = st.columns(2)
            with co1:
                off_client_names = ['-- Select Client --'] + [c['name'] for c in clients]
                default_off_client = 0
                if edit_off:
                    edit_off_client = str(edit_off.get('Client', '')).strip()
                    for i, cn in enumerate(off_client_names):
                        if cn == edit_off_client:
                            default_off_client = i
                            break
                off_sel_idx = st.selectbox("Client", range(len(off_client_names)),
                                            format_func=lambda i: off_client_names[i],
                                            index=default_off_client,
                                            key='off_client_sel')
            with co2:
                if off_sel_idx > 0:
                    off_sel_client = clients[off_sel_idx - 1]
                    off_client_name = off_sel_client['name']
                    off_client_addr = st.text_area("Client Address",
                                                    value=off_sel_client.get('address', ''),
                                                    height=68, key='off_client_addr')
                    off_client_id = off_sel_client['id']
                else:
                    off_client_name = st.text_input("Client Name (new)",
                                                     value=str(edit_off.get('Client', '')) if edit_off and default_off_client == 0 else '',
                                                     key='off_client_name_new')
                    off_client_addr = st.text_area("Client Address", height=68,
                                                    value=str(edit_off.get('Client Address', '')) if edit_off else '',
                                                    key='off_client_addr_new')
                    off_client_id = ''

        # Container 2: Project Title + Offer Date
        with st.container(border=True):
            co3, co4 = st.columns(2)
            with co3:
                off_title = st.text_input("Project Title",
                                           value=str(edit_off.get('Project', '')) if edit_off else '',
                                           key='off_title')
            with co4:
                default_off_date = datetime.today()
                if edit_off and edit_off.get('Date'):
                    try:
                        default_off_date = datetime.strptime(str(edit_off['Date']), '%Y-%m-%d')
                    except Exception:
                        pass
                off_date = st.date_input("Offer Date", value=default_off_date, key='off_date')

        # Container 3: Description
        with st.container(border=True):
            off_description = st.text_area("Project Description",
                                            value=str(edit_off.get('Description', '')) if edit_off else '',
                                            height=80, key='off_desc')

        # Container 4: Category + Validity
        with st.container(border=True):
            co5, co6 = st.columns(2)
            with co5:
                default_off_cat = 0
                if edit_off and edit_off.get('Category'):
                    try:
                        default_off_cat = INCOME_CATEGORIES.index(str(edit_off['Category']))
                    except ValueError:
                        pass
                off_category = st.selectbox("Category", INCOME_CATEGORIES,
                                             index=default_off_cat, key='off_cat')
            with co6:
                off_validity = st.number_input("Validity (Days)", value=int(edit_off.get('Validity', 30) or 30) if edit_off else 30,
                                                min_value=1, max_value=365, key='off_validity')

        # Expander: Additional Details
        with st.expander("Additional Details", expanded=bool(edit_off)):
            oad1, oad2 = st.columns(2)
            with oad1:
                off_location = st.text_input("Location (optional)",
                                              value=str(edit_off.get('Location', '')) if edit_off else '',
                                              key='off_loc')
            with oad2:
                off_event_date = st.text_input("Event Date (optional)",
                                                value=str(edit_off.get('Event Date', '')) if edit_off else '',
                                                key='off_event_date')

        # Line items
        _text_c = t['text']
        st.markdown(f"<h5 style='color:{_text_c};font-family:{FONT};margin-top:1rem'>Line Items</h5>",
                    unsafe_allow_html=True)
        if 'off_items' not in st.session_state:
            if edit_off and edit_off.get('Items JSON'):
                try:
                    loaded_off_items = _json.loads(str(edit_off['Items JSON']))
                    st.session_state['off_items'] = [
                        {'description': it.get('description', ''), 'detail': it.get('detail', ''),
                         'qty': float(it.get('qty', 1)), 'unit': it.get('unit', 'pcs'),
                         'unit_price': float(it.get('unit_price', 0))}
                        for it in loaded_off_items
                    ] if loaded_off_items else [{'description': '', 'detail': '', 'qty': 1.0, 'unit': 'pcs', 'unit_price': 0.0}]
                except Exception:
                    st.session_state['off_items'] = [{'description': '', 'detail': '', 'qty': 1.0,
                                                       'unit': 'pcs', 'unit_price': 0.0}]
            else:
                st.session_state['off_items'] = [{'description': '', 'detail': '', 'qty': 1.0,
                                                   'unit': 'pcs', 'unit_price': 0.0}]

        off_items_remove = None
        for idx, item in enumerate(st.session_state['off_items']):
            lic1, lic2, lic3, lic4, lic5 = st.columns([3, 1, 1, 1.5, 0.5])
            with lic1:
                item['description'] = st.text_input("Description", value=item['description'],
                                                     key=f'off_item_desc_{idx}')
            with lic2:
                item['qty'] = st.number_input("Qty", value=float(item['qty']), min_value=0.0,
                                               step=0.5, format="%.1f", key=f'off_item_qty_{idx}')
            with lic3:
                item['unit'] = st.text_input("Unit", value=item['unit'],
                                              key=f'off_item_unit_{idx}')
            with lic4:
                item['unit_price'] = st.number_input("Price (EUR)", value=float(item['unit_price']),
                                                      min_value=0.0, step=10.0, format="%.2f",
                                                      key=f'off_item_price_{idx}')
            with lic5:
                if idx > 0:
                    if st.button("x", key=f'off_item_rm_{idx}'):
                        off_items_remove = idx

            item['detail'] = st.text_input("Detail (optional)", value=item.get('detail', ''),
                                            key=f'off_item_detail_{idx}')

        if off_items_remove is not None:
            st.session_state['off_items'].pop(off_items_remove)
            st.rerun()

        if st.button("+ Add Line Item", key='off_add_line'):
            st.session_state['off_items'].append({'description': '', 'detail': '', 'qty': 1.0,
                                                   'unit': 'pcs', 'unit_price': 0.0})
            st.rerun()

        # Calculate totals
        off_line_items = []
        for idx, item in enumerate(st.session_state['off_items']):
            total_i = item['qty'] * item['unit_price']
            off_line_items.append({
                'pos': idx + 1,
                'description': item['description'],
                'detail': item.get('detail', ''),
                'qty': item['qty'],
                'unit': item['unit'],
                'unit_price': item['unit_price'],
                'total': total_i,
            })
        off_subtotal = sum(it['total'] for it in off_line_items)
        off_vat = round(off_subtotal * VAT_RATE, 2)
        off_total = round(off_subtotal + off_vat, 2)

        st.markdown("---")
        otc1, otc2, otc3 = st.columns(3)
        with otc1:
            st.metric("Subtotal (Netto)", f"{_fmt_eur_de(off_subtotal)} EUR")
        with otc2:
            st.metric("USt. 19%", f"{_fmt_eur_de(off_vat)} EUR")
        with otc3:
            st.metric("Total (Brutto)", f"{_fmt_eur_de(off_total)} EUR")

        st.markdown("---")

        # Two action buttons
        obtn1, obtn2 = st.columns(2)
        with obtn1:
            generate_off = st.button("Generate PDF", type="primary", use_container_width=True,
                                      key='off_generate')
        with obtn2:
            save_draft_off = st.button("Save as Draft", use_container_width=True,
                                        key='off_save_draft')

        # ── Generate Offer PDF ──
        if generate_off:
            off_name = off_client_name.strip() if off_client_name else ''
            if not off_name:
                st.error("Please select or enter a client name.")
            elif off_subtotal <= 0:
                st.error("Please add at least one line item with a price.")
            else:
                with st.spinner("Generating offer..."):
                    client = _get_or_create_client(off_name, off_client_addr)
                    off_number = _next_offer_number()

                    from datetime import timedelta
                    valid_until_date = off_date + timedelta(days=off_validity)

                    doc_data = {
                        'number': off_number,
                        'date': off_date.strftime('%Y-%m-%d'),
                        'date_formatted': off_date.strftime('%d.%m.%Y'),
                        'valid_until': valid_until_date.strftime('%d.%m.%Y'),
                        'client_name': client['name'],
                        'client_address': off_client_addr,
                        'client_id': client['id'],
                        'title': off_title,
                        'description': off_description,
                        'location': off_location,
                        'event_date': off_event_date,
                        'items': off_line_items,
                        'subtotal': off_subtotal,
                        'vat': off_vat,
                        'total': off_total,
                        'validity': off_validity,
                    }

                    pdf_bytes = _generate_document_pdf('offer', doc_data)
                    filename = f"Angebot_JosefSindelka_{off_number}.pdf"

                    # Upload to Google Drive
                    try:
                        folder_id = _get_offers_folder_id()
                        if folder_id:
                            _drive_upload_bytes(folder_id, filename, pdf_bytes)
                            st.success(f"Offer **{off_number}** uploaded to Drive: `{OFFERS_FOLDER}/{filename}`")
                        else:
                            st.warning("Could not find/create OFFERS folder -- PDF generated but not uploaded.")
                    except Exception as e:
                        st.error(f"Drive upload failed: {e}")

                    # Save to Offers sheet
                    _save_offer_to_sheet({
                        'offer_number': off_number,
                        'date': off_date.strftime('%Y-%m-%d'),
                        'client': client['name'],
                        'project': off_title,
                        'category': off_category,
                        'netto': off_subtotal,
                        'brutto': off_total,
                        'validity': off_validity,
                        'valid_until': valid_until_date.strftime('%d.%m.%Y'),
                        'status': 'SENT',
                        'items_json': _json.dumps(off_line_items),
                    })

                    # Save metadata
                    _save_document_meta(off_number, 'Offer', {
                        'client': client['name'], 'client_address': off_client_addr,
                        'project': off_title, 'category': off_category,
                        'description': off_description,
                        'date': off_date.strftime('%Y-%m-%d'),
                        'location': off_location, 'event_date': off_event_date,
                        'service_date': '', 'invoice_type': '', 'deposit_label': '',
                        'project_total': 0, 'validity': off_validity,
                        'netto': off_subtotal, 'brutto': off_total,
                        'items_json': _json.dumps(off_line_items), 'status': 'SENT',
                    })

                    _log_activity('OFFER_CREATED', f"{off_number} | {client['name']} | {_fmt_eur_de(off_total)} EUR")

                    st.download_button("Download PDF", data=pdf_bytes, file_name=filename,
                                        mime='application/pdf', key='off_download')

                    st.session_state['off_items'] = [{'description': '', 'detail': '', 'qty': 1.0,
                                                       'unit': 'pcs', 'unit_price': 0.0}]

        # ── Save Offer as Draft ──
        if save_draft_off:
            off_name = off_client_name.strip() if off_client_name else ''
            if not off_name:
                st.error("Please enter a client name to save a draft.")
            else:
                off_number = _next_offer_number()
                from datetime import timedelta
                valid_until_date = off_date + timedelta(days=off_validity)

                # Save to Offers sheet with DRAFT status
                _save_offer_to_sheet({
                    'offer_number': off_number,
                    'date': off_date.strftime('%Y-%m-%d'),
                    'client': off_name,
                    'project': off_title,
                    'category': off_category,
                    'netto': off_subtotal,
                    'brutto': off_total,
                    'validity': off_validity,
                    'valid_until': valid_until_date.strftime('%d.%m.%Y'),
                    'status': 'DRAFT',
                    'items_json': _json.dumps(off_line_items),
                })

                # Save to DocumentMeta
                _save_document_meta(off_number, 'Offer', {
                    'client': off_name, 'client_address': off_client_addr,
                    'project': off_title, 'category': off_category,
                    'description': off_description,
                    'date': off_date.strftime('%Y-%m-%d'),
                    'location': off_location, 'event_date': off_event_date,
                    'service_date': '', 'invoice_type': '', 'deposit_label': '',
                    'project_total': 0, 'validity': off_validity,
                    'netto': off_subtotal, 'brutto': off_total,
                    'items_json': _json.dumps(off_line_items), 'status': 'DRAFT',
                })

                _log_activity('OFFER_DRAFT', f"{off_number} | {off_name} | {_fmt_eur_de(off_total)} EUR")
                st.success(f"Draft **{off_number}** saved. You can find it in the Dashboard.")

    # ────────────────────────────────────────────────────────────
    # SUB-TAB 3: CLIENTS
    # ────────────────────────────────────────────────────────────
    with sub3:
        st.markdown("#### Client Database")
        st.caption("Manage your client list. Clients are stored in Google Sheets.")

        clients_fresh = _load_clients_db()
        if not clients_fresh:
            clients_fresh = _seed_clients_if_empty()

        if clients_fresh:
            client_df = pd.DataFrame(clients_fresh)
            col_order = ['id', 'name', 'address', 'notes', 'country']
            for c in col_order:
                if c not in client_df.columns:
                    client_df[c] = ''
            client_df = client_df[col_order]
            client_df.columns = ['ID', 'Name', 'Address', 'Notes', 'Country']
            st.dataframe(client_df, use_container_width=True, hide_index=True)
        else:
            st.info("No clients yet. They will be auto-created when you generate your first invoice or offer.")

        # Add client form
        with st.expander("Add New Client", expanded=False):
            nc1, nc2 = st.columns(2)
            with nc1:
                new_name = st.text_input("Name", key='new_client_name')
            with nc2:
                new_country = st.text_input("Country", key='new_client_country')
            new_addr = st.text_input("Address", key='new_client_addr')
            new_notes = st.text_input("Notes", key='new_client_notes')

            if st.button("Save Client", key='save_new_client'):
                if not new_name.strip():
                    st.error("Client name is required.")
                else:
                    new_client = {
                        'id': _next_client_id(),
                        'name': new_name.strip(),
                        'address': new_addr.strip(),
                        'notes': new_notes.strip(),
                        'country': new_country.strip(),
                    }
                    _save_client_to_sheet(new_client)
                    _log_activity('CLIENT_ADDED', f"{new_client['id']} | {new_client['name']}")
                    st.success(f"Client **{new_client['name']}** ({new_client['id']}) added.")
                    st.rerun()


# ─── TAB 7 — 2025 ───────────────────────────────────────────────────────────

def tab_2025(data):
    m2025 = _parse_2025_monthly(data.get('hist_2025'))

    if not m2025:
        st.info("No 2025 data available.")
        return

    total_2025 = sum(m2025.values())
    best_month = max(m2025, key=m2025.get) if m2025 else 'N/A'
    best_val = m2025.get(best_month, 0)
    num_months = len(m2025)
    avg_monthly = total_2025 / num_months if num_months > 0 else 0

    # --- KPI Cards ---
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(f"""
        <div class="card">
            <div class="card-label">Total Income (Netto)</div>
            <div class="card-value accent">\u20ac{total_2025:,.2f}</div>
            <div class="card-sub">Full year 2025</div>
        </div>
        """, unsafe_allow_html=True)
    with k2:
        st.markdown(f"""
        <div class="card">
            <div class="card-label">Best Month</div>
            <div class="card-value positive">{best_month}</div>
            <div class="card-sub">\u20ac{best_val:,.2f} netto</div>
        </div>
        """, unsafe_allow_html=True)
    with k3:
        st.markdown(f"""
        <div class="card">
            <div class="card-label">Avg. Monthly Income</div>
            <div class="card-value blue">\u20ac{avg_monthly:,.2f}</div>
            <div class="card-sub">Netto per month</div>
        </div>
        """, unsafe_allow_html=True)
    with k4:
        st.markdown(f"""
        <div class="card">
            <div class="card-label">Active Months</div>
            <div class="card-value">{num_months}</div>
            <div class="card-sub">Months with income</div>
        </div>
        """, unsafe_allow_html=True)

    # --- Monthly Income Table ---
    rows = []
    for m in MONTHS:
        val = m2025.get(m, 0)
        if val > 0:
            rows.append({'Month': m, 'Income (Netto)': fmt_eur(val)})
    rows.append({'Month': 'TOTAL', 'Income (Netto)': fmt_eur(total_2025), '_total': True})

    chart_card_html('Monthly Income (Netto) \u2014 2025',
                    html_table(['Month', 'Income (Netto)'], rows, num_cols={1}))

    # --- Monthly Bar Chart ---
    section_title('Monthly Income (Netto)')

    months_with_data = [m for m in MONTHS if m2025.get(m, 0) > 0]
    vals = [m2025.get(m, 0) for m in months_with_data]

    fig = go.Figure(go.Bar(
        x=[m[:3] for m in months_with_data], y=vals,
        marker_color=C_ORANGE,
        marker=dict(cornerradius=6),
        text=[fmt_eur(v) for v in vals],
        textposition='outside',
        textfont=dict(color=C_ORANGE, size=10, family=FONT),
    ))
    fig.update_layout(
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
    )
    chart_layout(fig, height=380)
    st.plotly_chart(fig, use_container_width=True)

    # --- 2025 vs 2026 comparison ---
    overview = data['overview']
    ov_dict = dict(zip(overview['Month'], overview['Income']))
    active_2026 = {m: v for m, v in ov_dict.items() if v > 0}

    if active_2026:
        section_title('2025 vs 2026 \u2014 Year-over-Year')

        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            name='2025', x=[m[:3] for m in MONTHS],
            y=[m2025.get(m, 0) for m in MONTHS],
            marker_color='rgba(232,93,38,0.4)',
            marker=dict(cornerradius=4),
        ))
        fig2.add_trace(go.Bar(
            name='2026', x=[m[:3] for m in MONTHS],
            y=[ov_dict.get(m, 0) for m in MONTHS],
            marker_color=C_ORANGE,
            marker=dict(cornerradius=4),
        ))
        fig2.update_layout(barmode='group', bargap=0.25, bargroupgap=0.1)
        chart_layout(fig2, height=380)
        st.plotly_chart(fig2, use_container_width=True)


# ─── Upload Expense Helpers ──────────────────────────────────────────────────

_MONTH_NUM = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
    'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
    'januar': 1, 'februar': 2, 'märz': 3, 'mai': 5, 'juni': 6, 'juli': 7,
    'august': 8, 'september': 9, 'oktober': 10, 'november': 11, 'dezember': 12,
}

_CATEGORY_KEYWORDS = {
    'anthropic': 'AI Software', 'openai': 'AI Software', 'chatgpt': 'AI Software',
    'midjourney': 'AI Software', 'claude': 'AI Software', 'cursor': 'AI Software',
    'copilot': 'AI Software', 'perplexity': 'AI Software', 'notion': 'AI Software',
    'weavy': 'AI Software', 'figma': 'AI Software', 'freepik': 'AI Software',
    'canva': 'AI Software', 'adobe': 'AI Software', 'github': 'AI Software',
    'miles mobility': 'Miles', 'miles': 'Miles', 'car rental': 'Miles',
    'sixt': 'Gear Rental', 'grover': 'Gear Rental',
    'lexoffice': 'Accounting', 'lexware': 'Accounting', 'steuerberater': 'Accounting',
    'buchhaltung': 'Accounting', 'haufe': 'Accounting',
    'versicherung': 'Insurance', 'insurance': 'Insurance', 'allianz': 'Insurance',
    'haftpflicht': 'Insurance', 'zurich': 'Insurance', 'markel': 'Insurance',
    'lieferando': 'Restaurants', 'restaurant': 'Restaurants', 'gastronomie': 'Restaurants',
    'uber eats': 'Restaurants', 'wolt': 'Restaurants',
    'amazon': 'Office', 'büro': 'Office',
    'udemy': 'Education', 'coursera': 'Education', 'skillshare': 'Education',
    'education': 'Education', 'kurs': 'Education', 'seminar': 'Education',
    'coaching': 'Education', 'genhq': 'Education',
    'hotel': 'Travel Cost', 'flug': 'Travel Cost', 'flight': 'Travel Cost',
    'booking': 'Travel Cost', 'airbnb': 'Travel Cost',
    'deutsche bahn': 'Travel Cost', 'bahn': 'Travel Cost', 'db reisezentrum': 'Travel Cost',
    'gewerbe': 'Gewerbe', 'finanzamt': 'Gewerbe', 'handelskammer': 'Gewerbe',
    'handwerkskammer': 'Gewerbe', 'ihk': 'Gewerbe',
}


@st.cache_data(ttl=3600)
def get_exchange_rate(from_currency, to_currency='EUR'):
    """Fetch live exchange rate from ECB via Frankfurter API.
    Returns (rate, source_label) or (None, error_msg).
    """
    import urllib.request, json
    try:
        url = f"https://api.frankfurter.dev/v1/latest?base={from_currency}&symbols={to_currency}"
        req = urllib.request.Request(url, headers={'User-Agent': 'FinanceDashboard/1.0'})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            rate = data['rates'][to_currency]
            return rate, f"ECB \u00b7 {data['date']}"
    except Exception:
        return None, "Could not fetch exchange rate"


def extract_pdf_data(uploaded_file):
    """Extract date, amount, vendor, and category from a PDF invoice.
    Returns dict with '_warnings' list for extraction feedback."""
    result = {'date': None, 'netto': 0.0, 'vendor': '', 'currency': 'EUR', 'category': None, '_warnings': []}
    try:
        import pdfplumber
    except ImportError:
        result['_warnings'].append('pdfplumber not installed — cannot extract data from PDF')
        return result

    try:
        uploaded_file.seek(0)
        with pdfplumber.open(uploaded_file) as pdf:
            text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
    except Exception:
        result['_warnings'].append('Could not read PDF — file may be corrupted or password-protected')
        return result

    if not text.strip():
        result['_warnings'].append('PDF appears empty or is a scanned image — no text could be extracted')
        return result

    lines = text.split('\n')
    text_lower = text.lower()

    # ── DATE ────────────────────────────────────────────────────────────
    for line in lines:
        m = re.search(
            r'(?:date\s+(?:of\s+)?issue|invoice\s+date|rechnungsdatum)[:\s]*'
            r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', line, re.IGNORECASE
        )
        if m:
            mn = _MONTH_NUM.get(m.group(1).lower())
            if mn:
                try:
                    result['date'] = datetime(int(m.group(3)), mn, int(m.group(2)))
                    break
                except ValueError:
                    pass
        m2 = re.search(
            r'(?:date\s+(?:of\s+)?issue|invoice\s+date|rechnungsdatum)[:\s]*'
            r'(\d{1,2})\.(\d{1,2})\.(\d{4})', line, re.IGNORECASE
        )
        if m2:
            try:
                result['date'] = datetime(int(m2.group(3)), int(m2.group(2)), int(m2.group(1)))
                break
            except ValueError:
                pass

    if result['date'] is None:
        month_pat = '|'.join(_MONTH_NUM.keys())
        m = re.search(rf'({month_pat})\s+(\d{{1,2}}),?\s+(\d{{4}})', text_lower)
        if m:
            mn = _MONTH_NUM.get(m.group(1))
            if mn:
                try:
                    result['date'] = datetime(int(m.group(3)), mn, int(m.group(2)))
                except ValueError:
                    pass

    if result['date'] is None:
        m = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
        if m:
            try:
                result['date'] = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except ValueError:
                pass

    # ── AMOUNTS (line-by-line) ────────────────────────────────────────
    eur_amounts = []
    usd_amounts = []
    for line in lines:
        if re.search(r'(?:VAT|USt|St\.-Nr|tax.?id|UID)', line, re.IGNORECASE):
            continue
        for m in re.finditer(r'€\s*(\d[\d.,]*\d)', line):
            eur_amounts.append(m.group(1))
        for m in re.finditer(r'(\d[\d.,]*\d)\s*€', line):
            eur_amounts.append(m.group(1))
        for m in re.finditer(r'(\d[\d.,]*\d)\s+EUR\b', line):
            eur_amounts.append(m.group(1))
        for m in re.finditer(r'\bEUR\s+(\d[\d.,]*\d)', line):
            eur_amounts.append(m.group(1))
        for m in re.finditer(r'\$\s*(\d[\d.,]*\d)', line):
            usd_amounts.append(m.group(1))
        for m in re.finditer(r'(\d[\d.,]*\d)\s+USD\b', line):
            usd_amounts.append(m.group(1))

    def _parse_amount(raw, german_format=False):
        if german_format:
            return float(raw.replace('.', '').replace(',', '.'))
        return float(raw.replace(',', ''))

    def _best_amount(raw_list):
        parsed = []
        german = any(',' in a and (a.index(',') > a.rindex('.') if '.' in a else True)
                      for a in raw_list if ',' in a)
        for a in raw_list:
            try:
                parsed.append(_parse_amount(a, german))
            except ValueError:
                pass
        if not parsed:
            return 0.0
        from collections import Counter
        counts = Counter(round(v, 2) for v in parsed)
        most_common_val, most_common_count = counts.most_common(1)[0]
        if most_common_count >= 2:
            return most_common_val
        return max(parsed)

    if eur_amounts:
        result['netto'] = _best_amount(eur_amounts)
        result['currency'] = 'EUR'
    elif usd_amounts:
        result['netto'] = _best_amount(usd_amounts)
        result['currency'] = 'USD'

    # ── NETTO vs BRUTTO ──────────────────────────────────────────────
    result['brutto'] = result['netto']
    for line in lines:
        if re.search(r'(?:VAT|USt|St\.-Nr|tax.?id|UID)', line, re.IGNORECASE):
            continue
        line_lower = line.lower()
        if re.search(r'\b(?:net(?:to)?|zwischensumme|subtotal)\b', line_lower):
            amounts_in_line = re.findall(r'[\d.,]+', line)
            for a in reversed(amounts_in_line):
                try:
                    german = ',' in a and ('.' not in a or a.index(',') > a.rindex('.'))
                    val = _parse_amount(a, german)
                    if val > 0:
                        result['netto'] = val
                        break
                except (ValueError, IndexError):
                    pass
        if re.search(r'\b(?:brutto|gesamt(?:betrag)?|total(?!\s+due))\b', line_lower):
            amounts_in_line = re.findall(r'[\d.,]+', line)
            for a in reversed(amounts_in_line):
                try:
                    german = ',' in a and ('.' not in a or a.index(',') > a.rindex('.'))
                    val = _parse_amount(a, german)
                    if val > 0:
                        result['brutto'] = val
                        break
                except (ValueError, IndexError):
                    pass

    # ── VENDOR ───────────────────────────────────────────────────────
    _SUFFIXES = r'(?:GmbH|AG|Inc\.?|LLC|Ltd\.?|UG|SE|PBC|e\.K\.?|Co\.?|Corp\.?|S\.?A\.?|OpCo)'

    for line in lines:
        m = re.search(r'(.+?)\s+Bill\s+to', line, re.IGNORECASE)
        if m:
            candidate = m.group(1).strip().rstrip(',')
            if len(candidate) > 1:
                result['vendor'] = candidate
                break

    if not result['vendor']:
        vendor_match = re.search(
            r'((?:[A-Z][\w&.-]+[\s,]+){0,3}[A-Z][\w&.-]+[\s,]+)' + _SUFFIXES,
            text
        )
        if vendor_match:
            v = vendor_match.group(1).strip().rstrip(',')
            if not re.match(r'(?:Invoice|Date|Rechnung)', v, re.IGNORECASE):
                result['vendor'] = v

    if not result['vendor']:
        skip = {'invoice', 'rechnung', 'receipt', 'quittung', 'thanks', 'thank you',
                'thanks for', 'your ride', 'bill to'}
        for line in lines[:12]:
            clean = line.strip()
            low = clean.lower()
            if not clean or low in skip or len(clean) < 3:
                continue
            if re.match(r'^[\d\s./-]+$', clean):
                continue
            if re.search(r'@|http|www\.|planck|hamburg|josef|sindelka|invoice\s+number|date\s+of', clean, re.IGNORECASE):
                continue
            if re.search(r'[A-Z]', clean):
                result['vendor'] = clean
                break

    if result['vendor']:
        result['vendor'] = re.sub(r'\s+', ' ', result['vendor']).strip()
        result['vendor'] = re.sub(r',?\s*' + _SUFFIXES + r'$', '', result['vendor']).strip()

    # ── CATEGORY ─────────────────────────────────────────────────────
    for keyword, category in _CATEGORY_KEYWORDS.items():
        if keyword in text_lower:
            result['category'] = category
            break

    # ── EXTRACTION WARNINGS ──────────────────────────────────────────
    if result['netto'] == 0.0:
        result['_warnings'].append('Could not extract amount — please enter manually')
    if result['date'] is None:
        result['_warnings'].append('Could not extract date — please select manually')
    if not result['vendor']:
        result['_warnings'].append('Could not identify vendor — please enter manually')

    return result


def save_expense_pdf(uploaded_file, date, category, recipient):
    """Upload PDF to Google Drive in the correct monthly folder."""
    month_num = date.month
    month_name = MONTHS[month_num - 1]

    year_folder = _get_year_folder()
    if not year_folder:
        year_folder = _drive_get_or_create_folder(DRIVE_ROOT_FOLDER, YEAR_FOLDER)
    month_folder_name = f"{month_num:02d}_{month_name}_{date.year}"
    month_folder = _drive_get_or_create_folder(year_folder, month_folder_name)
    cost_folder = _drive_get_or_create_folder(month_folder, _NEW_COST_SUBFOLDER)

    cat_code = CATEGORY_FILE_MAP.get(category, category.replace(' ', '_'))
    clean_recipient = re.sub(r'[^\w\s-]', '', recipient).strip().replace(' ', '_')
    base_name = f"{date.day:02d}.{date.month:02d}._{cat_code}_{clean_recipient}"
    filename = f"{base_name}.pdf"

    # Check for duplicates
    existing = _drive_list_files(cost_folder, base_name)
    existing_names = {f['name'] for f in existing}
    counter = 1
    while filename in existing_names:
        filename = f"{base_name}_{counter}.pdf"
        counter += 1

    uploaded_file.seek(0)
    result = _drive_upload_bytes(cost_folder, filename, uploaded_file.read())
    return result['name']


def find_expense_pdf(date, category, recipient):
    """Find a PDF on Google Drive for a given expense row.

    Returns dict {id, name, folder_id} if found, None otherwise.
    Checks new 'Costs' subfolder first, then legacy subfolders for Jan/Feb.
    """
    if not isinstance(date, datetime):
        try:
            date = pd.to_datetime(date)
        except Exception:
            return None

    month_num = date.month
    month_name = MONTHS[month_num - 1]
    month_folder_name = f"{month_num:02d}_{month_name}_{date.year}"

    year_folder = _get_year_folder()
    if not year_folder:
        return None
    month_folder = _drive_find_folder(year_folder, month_folder_name)
    if not month_folder:
        return None

    day_prefix = f"{date.day:02d}.{date.month:02d}."
    cat_code = CATEGORY_FILE_MAP.get(category, category.replace(' ', '_'))
    clean_recipient = re.sub(r'[^\w\s-]', '', recipient).strip().replace(' ', '_')

    for subfolder_name, subfolder_id in _get_cost_subfolders(month_folder, month_name):
        files = _drive_list_files(subfolder_id)
        for f in files:
            fname = f['name']
            if not fname.lower().endswith('.pdf'):
                continue
            stem = fname[:-4]
            if not stem.startswith(day_prefix):
                continue
            fname_lower = stem.lower()
            recip_lower = clean_recipient.lower()
            if recip_lower and recip_lower in fname_lower:
                return {'id': f['id'], 'name': f['name'], 'folder_id': subfolder_id}
            alt_recip = recipient.replace(' ', '_').lower()
            if alt_recip and alt_recip in fname_lower:
                return {'id': f['id'], 'name': f['name'], 'folder_id': subfolder_id}

    return None


def delete_expense_from_excel(invoice_id):
    """Delete an expense row from Google Sheet by Invoice-ID."""
    ws = _gsheet().worksheet('Expenses')
    col_a = ws.col_values(1)
    target_row = None
    for i, val in enumerate(col_a[1:], start=2):
        try:
            if val and int(float(val)) == int(invoice_id):
                target_row = i
                break
        except (ValueError, TypeError):
            continue
    if target_row is None:
        return False
    ws.delete_rows(target_row)
    return True


def update_expense_in_excel(invoice_id, updated_data):
    """Update an expense row in Google Sheet by Invoice-ID."""
    ws = _gsheet().worksheet('Expenses')
    col_a = ws.col_values(1)
    target_row = None
    for i, val in enumerate(col_a[1:], start=2):
        try:
            if val and int(float(val)) == int(invoice_id):
                target_row = i
                break
        except (ValueError, TypeError):
            continue
    if target_row is None:
        return False

    date_val = updated_data['date']
    date_str = date_val.strftime('%d.%m.%Y') if isinstance(date_val, datetime) else str(date_val)

    ws.update(f'B{target_row}:J{target_row}', [[
        date_str,
        updated_data['month'],
        updated_data['recipient'],
        updated_data['category'],
        updated_data['currency'],
        updated_data.get('original_amount', updated_data['netto']),
        updated_data['netto'],
        updated_data['brutto'],
        updated_data.get('notes', ''),
    ]], value_input_option='USER_ENTERED')
    return True


def rename_expense_pdf(old_pdf_info, new_date, new_category, new_recipient):
    """Rename/move a PDF on Google Drive if date, category, or recipient changed."""
    if old_pdf_info is None:
        return None

    new_month_num = new_date.month
    new_month_name = MONTHS[new_month_num - 1]

    year_folder = _get_year_folder()
    if not year_folder:
        return None
    month_folder_name = f"{new_month_num:02d}_{new_month_name}_{new_date.year}"
    month_folder = _drive_get_or_create_folder(year_folder, month_folder_name)
    cost_folder = _drive_get_or_create_folder(month_folder, _NEW_COST_SUBFOLDER)

    cat_code = CATEGORY_FILE_MAP.get(new_category, new_category.replace(' ', '_'))
    clean_recipient = re.sub(r'[^\w\s-]', '', new_recipient).strip().replace(' ', '_')
    new_name = f"{new_date.day:02d}.{new_date.month:02d}._{cat_code}_{clean_recipient}.pdf"

    new_parent = cost_folder if cost_folder != old_pdf_info.get('folder_id') else None
    _drive_rename_file(old_pdf_info['id'], new_name, new_parent)
    return new_name


# ─── Invoice Sync Helpers ────────────────────────────────────────────────────

def _get_invoices_folder_id():
    """Get the Google Drive folder ID for the invoices folder."""
    year_folder = _get_year_folder()
    if not year_folder:
        return None
    return _drive_find_folder(year_folder, INVOICES_FOLDER)


def _extract_invoice_id_from_filename(filename):
    """Extract invoice ID from a PDF filename.
    Filenames: [notpaid_]Rechnung[_Region]_Name_RE2026048.pdf or _INV-1.pdf
    Returns (raw_id, normalized_id) where normalized strips 'RE' prefix.
    """
    stem = Path(filename).stem
    if stem.startswith('notpaid_'):
        stem = stem[len('notpaid_'):]
    parts = stem.split('_')
    raw_id = parts[-1] if parts else stem
    normalized = raw_id[2:] if raw_id.startswith('RE') else raw_id
    return raw_id, normalized


@st.cache_data(ttl=30)
def _auto_scan_changes():
    """Cached auto-scan for Drive invoice + expense changes on page load."""
    changes = []
    _scan_errors = []
    try:
        changes.extend(scan_invoice_changes())
    except Exception as e:
        _scan_errors.append(f"Invoice scan: {type(e).__name__}: {e}")
    try:
        changes.extend(scan_expense_changes())
    except Exception as e:
        _scan_errors.append(f"Expense scan: {type(e).__name__}: {e}")
    return changes, _scan_errors


def scan_invoice_changes():
    """Compare INVOICES 2026/ on Google Drive against Income sheet.
    Detects three types of changes:
      - STATUS: paid/unpaid mismatch between filename prefix and sheet section
      - NEW: PDF exists in folder but invoice number not found in sheet
      - MISSING: Invoice in sheet but no matching PDF in folder
    Returns list of dicts with change_type, invoice_number, filename, details.
    """
    changes = []
    inv_folder_id = _get_invoices_folder_id()
    if not inv_folder_id:
        return changes

    # Read income data directly from Google Sheet
    ws_inc = _gsheet().worksheet('Income')
    inc_vals = ws_inc.get_all_values()
    if not inc_vals:
        return changes
    inc_headers = inc_vals[0]
    inc_df = pd.DataFrame(inc_vals[1:], columns=inc_headers)
    if 'Date' in inc_df.columns:
        inc_df['Date'] = pd.to_datetime(inc_df['Date'], dayfirst=True, errors='coerce')
    paid_df = _parse_income_section(inc_df, 0)
    unpaid_df = _parse_income_section(inc_df, 1)

    # Build lookup: invoice_number -> {status, client, netto, project, ...}
    excel_invoices = {}
    for status_label, section_df in [('paid', paid_df), ('unpaid', unpaid_df)]:
        if len(section_df) == 0 or 'Invoice Number' not in section_df.columns:
            continue
        for _, row in section_df.iterrows():
            inv = row.get('Invoice Number')
            if pd.isna(inv):
                continue
            inv_key = str(int(inv)) if isinstance(inv, (int, float)) else str(inv).strip()
            netto_val = pd.to_numeric(_clean_currency(row.get('Netto (€)', 0)), errors='coerce')
            if pd.isna(netto_val):
                netto_val = 0
            excel_invoices[inv_key] = {
                'status': status_label,
                'client': str(row.get('Client', '')),
                'project': str(row.get('Project', '')),
                'netto': netto_val,
                'month': str(row.get('Month', '')),
            }

    # Track which Excel invoices have matching PDFs
    matched_excel_keys = set()

    # Scan PDF files in INVOICES 2026/ on Google Drive
    drive_files = _drive_list_files(inv_folder_id)
    for df in drive_files:
        filename = df['name']
        if not filename.lower().endswith('.pdf'):
            continue
        file_is_unpaid = filename.startswith('notpaid_')
        file_status = 'unpaid' if file_is_unpaid else 'paid'
        raw_id, normalized_id = _extract_invoice_id_from_filename(filename)

        # Try to match against sheet: raw first, then normalized
        excel_entry = excel_invoices.get(raw_id)
        matched_key = raw_id
        if excel_entry is None:
            excel_entry = excel_invoices.get(normalized_id)
            matched_key = normalized_id

        if excel_entry is not None:
            matched_excel_keys.add(matched_key)
            # STATUS change detection
            if excel_entry['status'] != file_status:
                changes.append({
                    'change_type': 'STATUS',
                    'invoice_number': matched_key,
                    'filename': filename,
                    'current_status': excel_entry['status'],
                    'new_status': file_status,
                    'client': excel_entry['client'],
                    'netto': excel_entry['netto'],
                    'project': excel_entry['project'],
                    'drive_file_id': df['id'],
                })
        else:
            # NEW: PDF in folder but not in sheet
            changes.append({
                'change_type': 'NEW',
                'invoice_number': raw_id,
                'filename': filename,
                'current_status': None,
                'new_status': file_status,
                'client': '',
                'netto': 0,
                'project': '',
                'drive_file_id': df['id'],
            })

    # MISSING: invoices in sheet but no matching PDF
    for inv_key, info in excel_invoices.items():
        if inv_key not in matched_excel_keys:
            changes.append({
                'change_type': 'MISSING',
                'invoice_number': inv_key,
                'filename': '',
                'current_status': info['status'],
                'new_status': None,
                'client': info['client'],
                'netto': info['netto'],
                'project': info['project'],
                'drive_file_id': None,
            })

    # Sort: STATUS first, then NEW, then MISSING
    type_order = {'STATUS': 0, 'NEW': 1, 'MISSING': 2}
    changes.sort(key=lambda c: type_order.get(c['change_type'], 99))

    return changes


# Reverse map: filename code → category name (e.g. 'AI_Software' → 'AI Software')
_FILE_TO_CATEGORY = {v.lower(): k for k, v in CATEGORY_FILE_MAP.items()}


def _parse_expense_filename(filename):
    """Parse an expense PDF filename like '02.01._Insurance_Zurich.pdf'.
    Returns dict {date_str, day, month, category, recipient} or None.
    """
    stem = filename[:-4] if filename.lower().endswith('.pdf') else filename
    # Expected: DD.MM._Category_Recipient  or  DD.MM._Category  or  DD.MM_Category_Recipient
    m = re.match(r'^(\d{1,2})\.(\d{1,2})\.?_(.+)$', stem)
    if not m:
        return None
    day, month_num = int(m.group(1)), int(m.group(2))
    rest = m.group(3)
    # Split rest by underscore — first part is category, rest is recipient
    # But category itself can contain underscores (e.g. AI_Software, Gear_Rental, Travel_Cost)
    # Try longest category match first
    rest_lower = rest.lower()
    matched_cat = None
    matched_rest = rest
    for file_code, cat_name in sorted(_FILE_TO_CATEGORY.items(), key=lambda x: -len(x[0])):
        if rest_lower.startswith(file_code):
            matched_cat = cat_name
            matched_rest = rest[len(file_code):]
            if matched_rest.startswith('_'):
                matched_rest = matched_rest[1:]
            break
    if not matched_cat:
        # Fall back: first segment is category
        parts = rest.split('_', 1)
        matched_cat = parts[0].replace('_', ' ')
        matched_rest = parts[1] if len(parts) > 1 else ''
    recipient = matched_rest.replace('_', ' ').strip()
    return {
        'date_str': f"{day:02d}.{month_num:02d}",
        'day': day,
        'month_num': month_num,
        'category': matched_cat,
        'recipient': recipient,
        'filename_stem': stem,
    }


def _keyword_score(file_stem, sheet_word_set):
    """Score how well a Drive filename matches a sheet entry's keywords.
    Exact word overlap is weighted heavily (×10) so that e.g. 'miles'
    matching entry keyword 'miles' beats a substring match like
    'carrental' containing 'rental' from a different entry.
    """
    file_text = file_stem.lower()
    file_words = set(w for w in re.split(r'[\s_.\-+]+', file_text) if len(w) >= 3)
    # Exact word overlap (both sets)
    exact = len(file_words & sheet_word_set)
    # Substring matches (sheet keywords found as substrings in the full text)
    sub = sum(1 for sw in sheet_word_set if sw in file_text)
    # Don't double-count exact matches already in substring count
    return exact * 10 + max(0, sub - exact)


def scan_expense_changes(expenses_df=None):
    """Compare cost folders on Google Drive against Expenses sheet.
    Uses a monthly count-based approach: counts PDF files per month folder
    on Drive vs expense entries for that month in the sheet.

    Detects:
      - SURPLUS (Drive > Sheet): new unregistered expenses
      - DEFICIT (Drive < Sheet): expense PDF deleted, entry should be removed
    Uses keyword matching to identify the specific entries involved.

    If expenses_df is provided, reuses it instead of re-fetching from Sheets.
    """
    changes = []
    year_folder = _get_year_folder()
    if not year_folder:
        return changes

    # Load sheet data (reuse if provided)
    if expenses_df is None:
        ws_exp = _gsheet().worksheet('Expenses')
        exp_records = ws_exp.get_all_records()
        expenses_df = pd.DataFrame(exp_records)

    # Group sheet entries by month with their keywords and row data
    sheet_by_month = {}  # month_name → list of {row_idx, keywords, row_data}
    for idx, row in expenses_df.iterrows():
        month = str(row.get('Month', '')).strip()
        if not month:
            continue
        cat = str(row.get('Category', ''))
        recip = str(row.get('Recipient', ''))
        combined = f"{cat} {recip}".lower()
        words = set(w for w in re.split(r'[\s,.()\-/]+', combined) if len(w) >= 3)
        sheet_by_month.setdefault(month, []).append({
            'idx': idx,
            'keywords': words,
            'row': row,
        })

    # Scan all month folders
    month_folders = _drive_list_files(year_folder)
    for mf in month_folders:
        if mf['mimeType'] != 'application/vnd.google-apps.folder':
            continue
        if not re.match(r'^\d{2}_\w+_\d{4}$', mf['name']):
            continue

        month_name = mf['name'].split('_')[1]

        # Collect all cost PDFs for this month
        drive_files = []
        for cost_subfolder_name, sub_id in _get_cost_subfolders(mf['id'], month_name):
            files = _drive_list_files(sub_id)
            for f in files:
                if f['name'].lower().endswith('.pdf'):
                    drive_files.append({
                        'name': f['name'],
                        'id': f['id'],
                        'subfolder': cost_subfolder_name,
                    })

        drive_count = len(drive_files)
        sheet_entries = sheet_by_month.get(month_name, [])
        sheet_count = len(sheet_entries)

        if drive_count == sheet_count:
            continue  # balanced — no changes

        # ── SURPLUS: Drive has more files → new unregistered expenses ──
        if drive_count > sheet_count:
            new_count = drive_count - sheet_count
            matched_sheet = set()
            unmatched_files = []

            for df in drive_files:
                stem = df['name'][:-4] if df['name'].lower().endswith('.pdf') else df['name']
                best_match = -1
                best_score = 0
                for i, se in enumerate(sheet_entries):
                    if i in matched_sheet:
                        continue
                    score = _keyword_score(stem, se['keywords'])
                    if score > best_score:
                        best_score = score
                        best_match = i
                if best_score >= 2 and best_match >= 0:
                    matched_sheet.add(best_match)
                else:
                    unmatched_files.append(df)

            for df in unmatched_files[:new_count]:
                parsed = _parse_expense_filename(df['name'])
                changes.append({
                    'change_type': 'NEW_EXPENSE',
                    'filename': df['name'],
                    'folder': f"{mf['name']}/{df['subfolder']}",
                    'category': parsed['category'] if parsed else '',
                    'recipient': parsed['recipient'] if parsed else '',
                    'date_str': parsed['date_str'] if parsed else '',
                    'month_folder': mf['name'],
                    'drive_file_id': df['id'],
                })

            if not unmatched_files and new_count > 0:
                changes.append({
                    'change_type': 'NEW_EXPENSE',
                    'filename': f'{new_count} new file(s)',
                    'folder': mf['name'],
                    'category': '', 'recipient': '', 'date_str': '',
                    'month_folder': mf['name'],
                    'drive_file_id': None,
                })

        # ── DEFICIT: Sheet has more entries → PDF was deleted ──
        elif sheet_count > drive_count:
            removed_count = sheet_count - drive_count
            # Build full score matrix of all (file, entry) pairs, then
            # greedily assign highest-scoring pairs first.  This ensures
            # strong matches (e.g. "midjourney"↔Midjourney) take priority
            # and weaker matches fall back to remaining entries.
            all_pairs = []  # (score, file_idx, entry_idx)
            for fi, df in enumerate(drive_files):
                stem = df['name'][:-4] if df['name'].lower().endswith('.pdf') else df['name']
                for ei, se in enumerate(sheet_entries):
                    score = _keyword_score(stem, se['keywords'])
                    if score >= 1:
                        all_pairs.append((score, fi, ei))

            all_pairs.sort(key=lambda x: -x[0])
            matched_files = set()
            matched_entries = set()
            for score, fi, ei in all_pairs:
                if fi not in matched_files and ei not in matched_entries:
                    matched_files.add(fi)
                    matched_entries.add(ei)

            # Some files & entries can't keyword-match (e.g. Airpods↔Amazon)
            # but both exist.  Force-pair remaining unmatched files with
            # remaining unmatched entries so only truly orphaned entries
            # (whose PDF was deleted) stay unmatched.
            remaining_files = sorted(fi for fi in range(len(drive_files))
                                     if fi not in matched_files)
            remaining_entries = sorted(ei for ei in range(len(sheet_entries))
                                       if ei not in matched_entries)
            for p in range(min(len(remaining_files), len(remaining_entries))):
                matched_entries.add(remaining_entries[p])

            unmatched = [se for i, se in enumerate(sheet_entries)
                         if i not in matched_entries]
            for se in unmatched[:removed_count]:
                row = se['row']
                dt = row.get('Date of Payment', '')
                cat = str(row.get('Category', ''))
                recip = str(row.get('Recipient', ''))
                netto = pd.to_numeric(
                    _clean_currency(row.get('Netto (€)', 0)), errors='coerce') or 0
                changes.append({
                    'change_type': 'MISSING_EXPENSE',
                    'category': cat,
                    'recipient': recip,
                    'date_str': str(dt),
                    'netto': netto,
                    'month': month_name,
                    'sheet_id': row.get('Invoice-ID', ''),
                })

    changes.sort(key=lambda c: (
        0 if c['change_type'] == 'NEW_EXPENSE' else 1,
        c.get('date_str', ''),
    ))
    return changes


def update_invoice_status_in_excel(invoice_number, new_status):
    """Move an invoice row between paid and unpaid sections in the Income sheet (Google Sheets)."""
    ws = _gsheet().worksheet('Income')
    all_vals = ws.get_all_values()

    # Find source row (1-indexed)
    source_row = None
    source_data = None
    for i, row in enumerate(all_vals[1:], start=2):
        cell_b = row[1] if len(row) > 1 else ''
        try:
            cell_str = str(int(float(cell_b))) if cell_b else ''
        except (ValueError, TypeError):
            cell_str = str(cell_b).strip()
        if cell_str == str(invoice_number).strip():
            source_row = i
            source_data = row[:9]
            break

    if source_row is None:
        return False

    # Find section boundaries using formulas (get_all_values returns computed values, not formulas)
    paid_total_row = None
    unpaid_label_row = None
    unpaid_total_row = None

    formulas = ws.get('A1:I' + str(len(all_vals)), value_render_option='FORMULA')
    for i, row in enumerate(formulas[1:], start=2):
        cell_a = row[0] if row else ''
        cell_g = row[6] if len(row) > 6 else ''
        cell_h = row[7] if len(row) > 7 else ''
        if cell_a and 'Unpaid' in str(cell_a):
            unpaid_label_row = i
            continue
        is_sum = str(cell_h).startswith('=SUM')
        is_total = 'Total' in str(cell_g) or 'TOTAL' in str(cell_g)
        if is_sum or is_total:
            if unpaid_label_row is None:
                paid_total_row = i
            else:
                unpaid_total_row = i

    if new_status == 'unpaid':
        if unpaid_total_row is None:
            return False
        insert_at = unpaid_total_row
        ws.insert_rows([source_data], row=insert_at, value_input_option='USER_ENTERED')
        del_row = source_row if source_row < insert_at else source_row + 1
        ws.delete_rows(del_row)

    elif new_status == 'paid':
        if paid_total_row is None:
            return False
        insert_at = paid_total_row
        ws.insert_rows([source_data], row=insert_at, value_input_option='USER_ENTERED')
        # source_row shifted down by 1 since we inserted above it
        ws.delete_rows(source_row + 1)

    _rebuild_income_sum_formulas_gsheet(ws)
    return True


def _delete_invoice_pdf(inv_num):
    """Delete invoice PDF from INVOICES 2026/ on Google Drive."""
    inv_key = str(int(float(inv_num))) if str(inv_num).replace('.', '').isdigit() else str(inv_num).strip()
    inv_folder_id = _get_invoices_folder_id()
    if not inv_folder_id:
        return
    files = _drive_list_files(inv_folder_id)
    for f in files:
        if f['name'].lower().endswith('.pdf') and inv_key in f['name']:
            try:
                _drive_delete_file(f['id'])
            except Exception:
                pass
            break


def remove_invoice_from_excel(invoice_number):
    """Remove an invoice row from the Income sheet by Invoice Number (Google Sheets)."""
    ws = _gsheet().worksheet('Income')
    col_b = ws.col_values(2)

    target_row = None
    for i, val in enumerate(col_b[1:], start=2):
        try:
            cell_str = str(int(float(val))) if val else ''
        except (ValueError, TypeError):
            cell_str = str(val).strip()
        if cell_str == str(invoice_number).strip():
            target_row = i
            break

    if target_row is None:
        return False

    ws.delete_rows(target_row)
    _rebuild_income_sum_formulas_gsheet(ws)
    return True


def add_invoice_to_excel(invoice_data, status='unpaid'):
    """Add a new invoice row to the Income sheet (Google Sheets)."""
    ws = _gsheet().worksheet('Income')
    formulas = ws.get('A1:I' + str(ws.row_count), value_render_option='FORMULA')

    # Find section boundaries
    paid_total_row = None
    unpaid_label_row = None
    unpaid_total_row = None

    for i, row in enumerate(formulas[1:], start=2):
        cell_a = row[0] if row else ''
        cell_h = row[7] if len(row) > 7 else ''
        if cell_a and 'Unpaid' in str(cell_a):
            unpaid_label_row = i
            continue
        if cell_h and str(cell_h).startswith('=SUM'):
            if unpaid_label_row is None:
                paid_total_row = i
            else:
                unpaid_total_row = i

    if status == 'paid':
        if paid_total_row is None:
            return False
        insert_at = paid_total_row
    else:
        if unpaid_total_row is None:
            return False
        insert_at = unpaid_total_row

    date_val = invoice_data.get('date', '')
    date_str = date_val.strftime('%d.%m.%Y') if isinstance(date_val, datetime) else str(date_val)

    row_data = [
        invoice_data.get('id', ''),
        invoice_data.get('invoice_number', ''),
        date_str,
        invoice_data.get('month', ''),
        invoice_data.get('client', ''),
        invoice_data.get('project', ''),
        invoice_data.get('category', ''),
        invoice_data.get('netto', 0),
        invoice_data.get('brutto', 0),
    ]
    ws.insert_rows([row_data], row=insert_at, value_input_option='USER_ENTERED')

    _rebuild_income_sum_formulas_gsheet(ws)
    return True


def _parse_german_number(s):
    """Parse a German-format number string (e.g., '2.380,00') to float."""
    s = s.strip()
    if ',' in s:
        return float(s.replace('.', '').replace(',', '.'))
    # No comma: check if '.' is a thousands separator (e.g., '2.000')
    parts = s.split('.')
    if len(parts) == 2 and len(parts[1]) == 3:
        return float(s.replace('.', ''))
    return float(s)


def extract_income_invoice_data(pdf_source):
    """Extract client, amount, date, project, category from an income invoice PDF.

    pdf_source can be a Drive file ID (string) or a local Path object.
    """
    result = {'client': '', 'netto': 0, 'brutto': 0, 'date': None, 'category': '', 'project': ''}
    try:
        import pdfplumber
    except ImportError:
        return result

    try:
        if isinstance(pdf_source, str):
            # Google Drive file ID — download to temp file
            pdf_bytes = _drive_download_bytes(pdf_source)
            tmp = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
            tmp.write(pdf_bytes)
            tmp.close()
            with pdfplumber.open(tmp.name) as pdf:
                text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
            os.unlink(tmp.name)
        else:
            with pdfplumber.open(pdf_source) as pdf:
                text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
    except Exception:
        return result

    if not text.strip():
        return result

    lines = text.split('\n')

    # --- Extract client ---
    # Strategy 1: Line with "Kundennr" — client name is text BEFORE "Kundennr" on same line
    for line in lines:
        m = re.match(r'^(.+?)\s+Kundennr', line)
        if m:
            result['client'] = m.group(1).strip()
            break

    # Strategy 2: "Bill To" / "Rechnungsempfänger" — take next non-empty line
    if not result['client']:
        for i, line in enumerate(lines):
            if re.search(r'bill\s*to|rechnungsempf|recipient', line, re.IGNORECASE):
                for j in range(i + 1, min(i + 4, len(lines))):
                    candidate = lines[j].strip()
                    if candidate and len(candidate) > 2 and not re.match(r'^[\d\s./-]+$', candidate):
                        if not re.search(r'@|http|www\.|invoice|datum|amount|rechnung', candidate, re.IGNORECASE):
                            result['client'] = candidate
                            break
                break

    # --- Extract date (prefer "Datum:" labeled date) ---
    for line in lines:
        m = re.search(r'Datum:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})', line, re.IGNORECASE)
        if m:
            try:
                result['date'] = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
                break
            except ValueError:
                pass
    # Fallback: first DD.MM.YYYY pattern not in an address/phone context
    if result['date'] is None:
        for line in lines:
            if re.search(r'Planck|Hamburg|Tel|IBAN|BIC|Steuer|USt', line, re.IGNORECASE):
                continue
            m = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', line)
            if m:
                try:
                    result['date'] = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
                    break
                except ValueError:
                    pass

    # --- Extract project ---
    # Strategy 1: Explicit "Project:" label
    for line in lines:
        m = re.search(r'Project:\s*(.+)', line, re.IGNORECASE)
        if m:
            result['project'] = m.group(1).strip()
            break
    # Strategy 2: Title line right after "Rechnung RE..." (e.g. "WEMPE - HOUSE OF EXCELLENCE")
    if not result['project']:
        for i, line in enumerate(lines):
            if re.match(r'Rechnung\s+(RE\d+|INV[- ]\d+)', line, re.IGNORECASE):
                # Check next 1-2 lines for a project title
                for j in range(i + 1, min(i + 3, len(lines))):
                    candidate = lines[j].strip()
                    if not candidate or len(candidate) < 3:
                        continue
                    # Skip lines that look like dates, amounts, metadata, or "Deposit"
                    if re.match(r'^(Pos\.|Date:|Objective:|This invoice|Deliverables|Scope|\d+[./])', candidate, re.IGNORECASE):
                        continue
                    if re.search(r'Zwischensumme|Gesamtbetrag|Stunde|Stück|€|netto|brutto', candidate, re.IGNORECASE):
                        continue
                    if re.match(r'^\d+/\d+\s+Deposit', candidate, re.IGNORECASE):
                        # e.g. "1/2 Deposit Invoice (€2.000)" — skip, check next line
                        continue
                    result['project'] = candidate
                    break
                break

    # --- Extract amounts from Zwischensumme/Gesamtbetrag lines (most reliable) ---
    netto_val = None
    brutto_val = None
    for line in lines:
        ll = line.lower()
        nums = re.findall(r'(\d[\d.,]*\d)', line)
        if re.search(r'zwischensumme|subtotal|\bnetto\b', ll):
            for n in nums:
                try:
                    v = _parse_german_number(n)
                    if v > 1:
                        netto_val = v
                except ValueError:
                    pass
        if re.search(r'gesamtbetrag|gesamt\b.*€|\btotal\b', ll):
            for n in nums:
                try:
                    v = _parse_german_number(n)
                    if v > 1:
                        brutto_val = v
                except ValueError:
                    pass

    if netto_val:
        result['netto'] = netto_val
    if brutto_val:
        result['brutto'] = brutto_val
    elif netto_val:
        result['brutto'] = netto_val

    # Fallback: scan all € amounts if no labeled lines found
    if result['netto'] == 0:
        amounts = []
        for line in lines:
            if re.search(r'USt|St\.-Nr|tax.?id|UID|IBAN|BIC', line, re.IGNORECASE):
                continue
            for m in re.finditer(r'€\s*(\d[\d.,]*\d)', line):
                amounts.append(m.group(1))
            for m in re.finditer(r'(\d[\d.,]*\d)\s*€', line):
                amounts.append(m.group(1))
            for m in re.finditer(r'(\d[\d.,]*\d)\s+EUR\b', line):
                amounts.append(m.group(1))
        parsed = []
        for a in amounts:
            try:
                parsed.append(_parse_german_number(a))
            except ValueError:
                pass
        if parsed:
            result['netto'] = max(parsed)
            result['brutto'] = result['netto']

    # --- Detect category ---
    text_lower = text.lower()
    if 'animation' in text_lower:
        result['category'] = 'Animation'
    elif 'photo' in text_lower:
        result['category'] = 'Photography'
    elif 'video' in text_lower:
        result['category'] = 'Video Production'
    elif 'ai' in text_lower or 'studio' in text_lower:
        result['category'] = 'AI Studio'

    return result


def _rebuild_income_sum_formulas_gsheet(ws):
    """Rebuild SUM formulas for paid and unpaid total rows in Google Sheet."""
    formulas = ws.get('A1:I' + str(ws.row_count), value_render_option='FORMULA')

    paid_total_row = None
    unpaid_label_row = None
    unpaid_total_row = None

    for i, row in enumerate(formulas[1:], start=2):
        cell_a = row[0] if row else ''
        cell_g = row[6] if len(row) > 6 else ''
        cell_h = row[7] if len(row) > 7 else ''
        if cell_a and 'Unpaid' in str(cell_a):
            unpaid_label_row = i
            continue
        is_sum = str(cell_h).startswith('=SUM')
        is_total = 'Total' in str(cell_g) or 'TOTAL' in str(cell_g)
        if is_sum or is_total:
            if unpaid_label_row is None:
                paid_total_row = i
            else:
                unpaid_total_row = i

    updates = []
    if paid_total_row and paid_total_row > 2:
        first_data = 2
        last_data = paid_total_row - 1
        updates.append({'range': f'H{paid_total_row}', 'values': [[f'=SUM(H{first_data}:H{last_data})']]})
        updates.append({'range': f'I{paid_total_row}', 'values': [[f'=SUM(I{first_data}:I{last_data})']]})

    if unpaid_total_row and unpaid_label_row:
        unpaid_header = unpaid_label_row + 1
        first_data = unpaid_header + 1
        last_data = unpaid_total_row - 1
        if last_data >= first_data:
            updates.append({'range': f'H{unpaid_total_row}', 'values': [[f'=SUM(H{first_data}:H{last_data})']]})
            updates.append({'range': f'I{unpaid_total_row}', 'values': [[f'=SUM(I{first_data}:I{last_data})']]})
        else:
            updates.append({'range': f'H{unpaid_total_row}', 'values': [[0]]})
            updates.append({'range': f'I{unpaid_total_row}', 'values': [[0]]})

    if updates:
        ws.batch_update(updates, value_input_option='USER_ENTERED')


def append_expense_to_excel(expense_data):
    """Append a new expense row to the Google Sheet."""
    ws = _gsheet().worksheet('Expenses')
    col_a = ws.col_values(1)

    max_id = 0
    for val in col_a[1:]:
        try:
            v = int(float(val))
            if v > max_id:
                max_id = v
        except (ValueError, TypeError):
            continue
    new_id = max_id + 1

    date_val = expense_data['date']
    date_str = date_val.strftime('%d.%m.%Y') if isinstance(date_val, datetime) else str(date_val)

    ws.append_row([
        new_id,
        date_str,
        expense_data['month'],
        expense_data['recipient'],
        expense_data['category'],
        expense_data['currency'],
        expense_data.get('original_amount', expense_data['netto']),
        expense_data['netto'],
        expense_data['brutto'],
        expense_data.get('notes', ''),
    ], value_input_option='USER_ENTERED')


def remove_expense_from_excel(expense_id):
    """Remove an expense row from the Expenses sheet by Invoice-ID."""
    ws = _gsheet().worksheet('Expenses')
    col_a = ws.col_values(1)  # Invoice-ID column

    target_row = None
    search = str(expense_id).strip()
    for i, val in enumerate(col_a[1:], start=2):
        try:
            cell_str = str(int(float(val))) if val else ''
        except (ValueError, TypeError):
            cell_str = str(val).strip()
        if cell_str == search:
            target_row = i
            break

    if target_row is None:
        return False

    ws.delete_rows(target_row)
    return True


@st.dialog("Upload Expense")
def upload_expense_dialog():
    """Modal dialog for uploading a PDF expense and adding it to the spreadsheet."""
    uploaded = st.file_uploader("Upload PDF invoice", type=['pdf'], key='expense_pdf')

    if uploaded is None:
        st.info("Upload a PDF invoice to get started.")
        return

    extracted = extract_pdf_data(uploaded)

    # PDF extraction feedback (#4)
    if extracted.get('_warnings'):
        for w in extracted['_warnings']:
            st.warning(w, icon="\u26a0\ufe0f")

    st.markdown("---")
    st.markdown("**Expense Details** *(edit as needed)*")

    col1, col2 = st.columns(2)
    with col1:
        default_date = extracted['date'] if extracted['date'] else datetime.today()
        expense_date = st.date_input("Date of Payment", value=default_date, key='exp_date')
    with col2:
        recipient = st.text_input("Recipient / Vendor", value=extracted['vendor'], key='exp_recipient')

    col3, col4 = st.columns(2)
    with col3:
        cat_index = 0
        if extracted.get('category') and extracted['category'] in CATEGORIES:
            cat_index = CATEGORIES.index(extracted['category'])
        category = st.selectbox("Category", CATEGORIES, index=cat_index, key='exp_category')
    with col4:
        currency = st.selectbox("Currency", ['EUR', 'USD'],
                                index=0 if extracted['currency'] == 'EUR' else 1,
                                key='exp_currency')

    original_amount = None
    converted_netto = extracted['netto']
    converted_brutto = extracted.get('brutto', extracted['netto'])
    _rate = None

    if currency != 'EUR':
        original_amount = st.number_input(
            f"Original Amount ({currency})", min_value=0.0,
            value=extracted['netto'], step=0.01, format="%.2f",
            key='exp_orig')

        _rate, _rate_info = get_exchange_rate(currency, 'EUR')
        if _rate and original_amount > 0:
            converted_netto = round(original_amount * _rate, 2)
            converted_brutto = converted_netto
            st.info(
                f"**1 {currency} = {_rate:.4f} EUR** ({_rate_info})\n\n"
                f"**{currency} {original_amount:,.2f}  \u2192  \u20ac{converted_netto:,.2f}**"
            )
        elif original_amount > 0:
            st.warning(f"{_rate_info} \u2014 enter EUR values manually below")

        # Auto-update Netto when Original Amount changes
        _prev_key = '_prev_exp_orig'
        _prev_curr_key = '_prev_exp_currency'
        if (st.session_state.get(_prev_key) != original_amount
                or st.session_state.get(_prev_curr_key) != currency):
            st.session_state[_prev_key] = original_amount
            st.session_state[_prev_curr_key] = currency
            if _rate and original_amount > 0:
                st.session_state['exp_netto'] = converted_netto

    if currency != 'EUR':
        netto = st.number_input("Netto (\u20ac)", min_value=0.0,
                                value=converted_netto, step=0.01,
                                format="%.2f", key='exp_netto')
        brutto = netto
    else:
        col5, col6 = st.columns(2)
        with col5:
            netto = st.number_input("Netto (\u20ac)", min_value=0.0, value=extracted['netto'],
                                    step=0.01, format="%.2f", key='exp_netto')
        with col6:
            brutto = st.number_input("Brutto (\u20ac)", min_value=0.0,
                                     value=extracted.get('brutto', extracted['netto']),
                                     step=0.01, format="%.2f", key='exp_brutto')
        original_amount = netto

    notes = st.text_input("Notes (optional)", key='exp_notes')

    # Duplicate detection (#8)
    _dup_warning = None
    try:
        expenses_df = load_data().get('expenses', pd.DataFrame())
        if len(expenses_df) and recipient.strip():
            dup_mask = (
                (expenses_df['Recipient'].str.lower() == recipient.strip().lower())
                & (expenses_df['Netto (€)'].round(2) == round(netto, 2))
            )
            if 'Date of Payment' in expenses_df.columns:
                exp_dt_check = datetime(expense_date.year, expense_date.month, expense_date.day)
                dup_mask = dup_mask & (expenses_df['Date of Payment'] == pd.Timestamp(exp_dt_check))
            if dup_mask.any():
                _dup_warning = f"Similar expense already exists: {recipient.strip()} | {fmt_eur(netto)} on {expense_date.strftime('%d.%m.%Y')}"
    except Exception:
        pass

    if _dup_warning:
        st.warning(f"Possible duplicate: {_dup_warning}", icon="\u26a0\ufe0f")

    if st.button("Save Expense", type="primary", use_container_width=True):
        if not recipient.strip():
            st.error("Recipient is required.")
            return
        if netto <= 0:
            st.error("Netto amount must be greater than 0.")
            return

        expense_dt = datetime(expense_date.year, expense_date.month, expense_date.day)
        month_name = MONTHS[expense_dt.month - 1]

        with st.spinner("Saving..."):
            saved_path = save_expense_pdf(uploaded, expense_dt, category, recipient.strip())
            append_expense_to_excel({
                'date': expense_dt,
                'month': month_name,
                'recipient': recipient.strip(),
                'category': category,
                'currency': currency,
                'original_amount': original_amount,
                'netto': netto,
                'brutto': brutto,
                'notes': notes.strip(),
            })
            _invalidate_data_caches()

        _log_activity('Expense Added', f"{recipient.strip()} | {category} | {fmt_eur(netto)}")
        st.success(f"Expense saved! PDF uploaded as: `{saved_path}`")
        st.balloons()


@st.dialog("Delete Expense")
def delete_expense_dialog(expense):
    """Confirmation dialog for deleting an expense."""
    date_str = ''
    if pd.notna(expense.get('Date of Payment')):
        try:
            dt = pd.to_datetime(expense['Date of Payment'])
            date_str = dt.strftime('%d.%m.%Y')
        except Exception:
            date_str = str(expense['Date of Payment'])

    recipient = str(expense.get('Recipient', ''))
    amount = expense.get('Netto (€)', 0)
    category = str(expense.get('Category', ''))
    invoice_id = expense.get('Invoice-ID', '')

    st.markdown("**Are you sure you want to delete this expense?**")
    st.markdown(f"""
- **Date:** {date_str}
- **Recipient:** {recipient}
- **Category:** {category}
- **Amount:** {fmt_eur(amount)}
""")
    st.warning("This action cannot be undone. The spreadsheet row and associated PDF file will be permanently deleted.")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancel", use_container_width=True, key='del_cancel'):
            st.session_state['_return_to_expenses'] = True
            st.rerun()
    with col2:
        if st.button("Delete", type="primary", use_container_width=True, key='del_confirm'):
            with st.spinner("Deleting..."):
                # Delete PDF from Google Drive
                try:
                    pdf_info = find_expense_pdf(
                        expense['Date of Payment'], category, recipient
                    )
                    if pdf_info:
                        _drive_delete_file(pdf_info['id'])
                except Exception:
                    pass  # PDF deletion is best-effort

                # Delete from spreadsheet
                success = delete_expense_from_excel(invoice_id)
                if success:
                    _invalidate_data_caches()
                    _log_activity('Expense Deleted', f"ID {invoice_id} | {expense.get('Recipient', '')} | {fmt_eur(expense.get('Netto (€)', 0))}")
                    st.success("Expense deleted successfully.")
                    import time; time.sleep(0.5)
                    st.session_state['_return_to_expenses'] = True
                    st.rerun()
                else:
                    st.error("Could not find the expense row in the spreadsheet.")


@st.dialog("Mark Invoice as Paid")
def mark_invoice_paid_dialog(invoice_data):
    """Move an unpaid invoice to paid: update sheet row + rename PDF on Drive."""
    inv_num = str(invoice_data.get('Invoice Number', ''))
    client = str(invoice_data.get('Client', ''))
    netto = pd.to_numeric(invoice_data.get('Netto (€)', 0), errors='coerce')

    st.markdown("**Mark this invoice as paid?**")
    st.markdown(f"""
- **Invoice:** {inv_num}
- **Client:** {client}
- **Amount:** {fmt_eur(netto)}
""")
    st.info("This will move the invoice to the Paid section and rename the PDF on Google Drive (remove `notpaid_` prefix).")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancel", use_container_width=True, key='mark_paid_cancel'):
            st.rerun()
    with col2:
        if st.button("Mark as Paid", type="primary", use_container_width=True, key='mark_paid_confirm'):
            with st.spinner("Updating..."):
                # 1. Move row in spreadsheet
                ok = update_invoice_status_in_excel(inv_num, 'paid')
                # 2. Rename PDF on Drive (remove notpaid_ prefix)
                try:
                    inv_folder_id = _get_invoices_folder_id()
                    if inv_folder_id:
                        inv_key = str(int(float(inv_num))) if str(inv_num).replace('.', '').isdigit() else str(inv_num).strip()
                        files = _drive_list_files(inv_folder_id)
                        for f in files:
                            if f['name'].lower().startswith('notpaid_') and inv_key in f['name']:
                                new_name = f['name'].replace('notpaid_', '', 1)
                                _drive_rename_file(f['id'], new_name)
                                break
                except Exception:
                    pass  # PDF rename is best-effort
                _invalidate_data_caches()
                if ok:
                    _log_activity('Invoice Marked Paid', f"#{inv_num} | {client} | {fmt_eur(netto)}")
                    st.success("Invoice marked as paid!")
                else:
                    st.warning("PDF renamed but could not move spreadsheet row. Please check manually.")
                import time; time.sleep(0.8)
                st.rerun()


@st.dialog("Delete Invoice")
def delete_income_invoice_dialog(invoice_data, status):
    """Confirmation dialog for deleting an income invoice."""
    inv_num = str(invoice_data.get('Invoice Number', ''))
    client = str(invoice_data.get('Client', ''))
    netto = pd.to_numeric(invoice_data.get('Netto (€)', 0), errors='coerce')

    st.markdown("**Are you sure you want to delete this invoice?**")
    st.markdown(f"""
- **Invoice:** {inv_num}
- **Client:** {client}
- **Amount:** {fmt_eur(netto)}
- **Status:** {status.capitalize()}
""")
    st.warning("This will permanently delete the spreadsheet row and the PDF file.")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancel", use_container_width=True, key='inc_del_cancel'):
            st.rerun()
    with col2:
        if st.button("Delete", type="primary", use_container_width=True, key='inc_del_confirm'):
            with st.spinner("Deleting..."):
                remove_invoice_from_excel(inv_num)
                _delete_invoice_pdf(inv_num)
                _invalidate_data_caches()
                _log_activity('Invoice Deleted', f"#{inv_num} | {client} | {fmt_eur(netto)}")
                st.rerun()


@st.dialog("Edit Expense")
def edit_expense_dialog(expense):
    """Dialog for editing an existing expense."""
    invoice_id = expense.get('Invoice-ID', '')
    _n = st.session_state.get('_edit_nonce', 0)

    current_date = datetime.today()
    if pd.notna(expense.get('Date of Payment')):
        try:
            current_date = pd.to_datetime(expense['Date of Payment']).to_pydatetime()
        except Exception:
            pass

    col1, col2 = st.columns(2)
    with col1:
        new_date = st.date_input("Date of Payment", value=current_date, key=f'ed_date_{_n}')
    with col2:
        new_recipient = st.text_input(
            "Recipient / Vendor",
            value=str(expense.get('Recipient', '')),
            key=f'ed_recipient_{_n}'
        )

    col3, col4 = st.columns(2)
    with col3:
        current_cat = str(expense.get('Category', ''))
        cat_index = CATEGORIES.index(current_cat) if current_cat in CATEGORIES else 0
        new_category = st.selectbox("Category", CATEGORIES, index=cat_index, key=f'ed_category_{_n}')
    with col4:
        current_currency = str(expense.get('Original Currency', 'EUR'))
        new_currency = st.selectbox(
            "Currency", ['EUR', 'USD'],
            index=0 if current_currency == 'EUR' else 1,
            key=f'ed_currency_{_n}'
        )

    col5, col6 = st.columns(2)
    with col5:
        new_netto = st.number_input(
            "Netto (\u20ac)", min_value=0.0,
            value=float(expense.get('Netto (€)', 0)),
            step=0.01, format="%.2f", key=f'ed_netto_{_n}'
        )
    with col6:
        new_brutto = st.number_input(
            "Brutto (\u20ac)", min_value=0.0,
            value=float(expense.get('Brutto (€)', 0)),
            step=0.01, format="%.2f", key=f'ed_brutto_{_n}'
        )

    new_original_amount = new_netto
    if new_currency == 'USD':
        current_orig = float(expense.get('Original Amount', expense.get('Netto (€)', 0)))
        new_original_amount = st.number_input(
            "Original Amount (USD)", min_value=0.0,
            value=current_orig, step=0.01, format="%.2f", key=f'ed_orig_{_n}'
        )

    notes_val = expense.get('Notes', '')
    if pd.isna(notes_val):
        notes_val = ''
    new_notes = st.text_input(
        "Notes (optional)",
        value=str(notes_val),
        key=f'ed_notes_{_n}'
    )

    col_cancel, col_save = st.columns(2)
    with col_cancel:
        if st.button("Cancel", use_container_width=True, key=f'ed_cancel_{_n}'):
            st.session_state['_return_to_expenses'] = True
            st.rerun()
    with col_save:
        if st.button("Save Changes", type="primary", use_container_width=True, key=f'ed_save_{_n}'):
            if not new_recipient.strip():
                st.error("Recipient is required.")
                return
            if new_netto <= 0:
                st.error("Netto amount must be greater than 0.")
                return

            expense_dt = datetime(new_date.year, new_date.month, new_date.day)
            new_month_name = MONTHS[expense_dt.month - 1]

            with st.spinner("Saving changes..."):
                # Rename/move PDF if date, category, or recipient changed
                old_category = str(expense.get('Category', ''))
                old_recipient = str(expense.get('Recipient', ''))
                old_date = current_date

                date_changed = new_date != (old_date.date() if hasattr(old_date, 'date') else old_date)
                cat_changed = new_category != old_category
                recip_changed = new_recipient.strip() != old_recipient

                if date_changed or cat_changed or recip_changed:
                    try:
                        old_pdf = find_expense_pdf(old_date, old_category, old_recipient)
                        if old_pdf:
                            rename_expense_pdf(old_pdf, expense_dt, new_category, new_recipient.strip())
                    except Exception:
                        pass  # PDF rename is best-effort

                # Update Excel
                success = update_expense_in_excel(invoice_id, {
                    'date': expense_dt,
                    'month': new_month_name,
                    'recipient': new_recipient.strip(),
                    'category': new_category,
                    'currency': new_currency,
                    'original_amount': new_original_amount,
                    'netto': new_netto,
                    'brutto': new_brutto,
                    'notes': new_notes.strip(),
                })

                if success:
                    _invalidate_data_caches()
                    _log_activity('Expense Edited', f"ID {invoice_id} | {new_recipient.strip()} | {fmt_eur(new_netto)}")
                    st.success("Expense updated successfully.")
                    import time; time.sleep(0.5)
                    st.session_state['_return_to_expenses'] = True
                    st.rerun()
                else:
                    st.error("Could not find the expense row in the spreadsheet.")


@st.dialog("Update", width="large")
def sync_invoices_dialog():
    """Scan invoices and cost folders for changes and sync with spreadsheet."""
    with st.spinner("Scanning Google Drive for changes..."):
        changes = scan_invoice_changes()
        expense_changes = scan_expense_changes()

    # For NEW invoices, try to extract data from PDF
    for c in changes:
        if c['change_type'] == 'NEW' and c.get('drive_file_id'):
            pdf_data = extract_income_invoice_data(c['drive_file_id'])
            c['client'] = pdf_data.get('client', '') or ''
            c['netto'] = pdf_data.get('netto', 0) or 0
            c['brutto'] = pdf_data.get('brutto', 0) or 0
            c['date'] = pdf_data.get('date')
            c['category'] = pdf_data.get('category', '') or ''
            c['project'] = pdf_data.get('project', '') or ''

    # For NEW expenses, try to extract data from PDF
    for c in expense_changes:
        if c['change_type'] == 'NEW_EXPENSE' and c.get('drive_file_id'):
            try:
                pdf_bytes = _drive_download_bytes(c['drive_file_id'])
                import pdfplumber
                with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                    text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
                # Try to extract amount from PDF
                for pattern in [r'(?:total|gesamt|summe|brutto|amount)[:\s]*[€$]?\s*([\d.,]+)',
                                r'([\d.,]+)\s*(?:EUR|€)']:
                    m = re.search(pattern, text, re.IGNORECASE)
                    if m:
                        amt_str = m.group(1).replace('.', '').replace(',', '.')
                        try:
                            c['netto'] = float(amt_str)
                        except ValueError:
                            pass
                        break
            except Exception:
                pass

    all_changes = changes + expense_changes

    if not all_changes:
        st.markdown("**No changes detected.**")
        st.markdown("All invoices and cost files match the current spreadsheet data.")
        if st.button("Close", use_container_width=True, key='sync_close'):
            st.rerun()
        return

    _card = (f'background:{_t()["surface"]};border:1px solid {_t()["border"]};'
             f'border-radius:12px;padding:1rem;margin-bottom:0.5rem;'
             f'box-shadow:{_t()["card_shadow"]}')

    st.markdown(f"**Found {len(all_changes)} change(s)**")
    st.markdown("Review the changes below, then click **Apply Changes** to update the spreadsheet.")

    # ── Invoice changes ──
    if changes:
        st.markdown(f"##### Invoices ({len(changes)})")
    for c in changes:
        inv = c['invoice_number']
        amt = fmt_eur(c['netto']) if c['netto'] else 'unknown amount'
        client = c['client'] or 'Unknown Client'

        if c['change_type'] == 'STATUS':
            if c['new_status'] == 'paid':
                desc = f'Invoice {inv} ({amt} \u2014 {client}) will be moved from unpaid to paid invoices'
                accent = C_GREEN
            else:
                desc = f'Invoice {inv} ({amt} \u2014 {client}) will be moved from paid to unpaid invoices'
                accent = C_RED
            st.markdown(f'<div style="{_card};border-left:3px solid {accent}">'
                        f'<div style="color:{_t()["text"]};font-size:0.85rem">{desc}</div>'
                        f'</div>', unsafe_allow_html=True)

        elif c['change_type'] == 'NEW':
            status_word = 'unpaid' if c['new_status'] == 'unpaid' else 'paid'
            if c['netto']:
                desc = f'Invoice {inv} ({amt} \u2014 {client}) will be added to {status_word} invoices'
            else:
                desc = f'Invoice {inv} (new PDF found) will be added to {status_word} invoices'
            st.markdown(f'<div style="{_card};border-left:3px solid {C_BLUE}">'
                        f'<div style="color:{_t()["text"]};font-size:0.85rem">{desc}</div>'
                        f'<div style="font-size:0.75rem;color:{_t()["muted"]};margin-top:0.25rem">{c["filename"]}</div>'
                        f'</div>', unsafe_allow_html=True)

        elif c['change_type'] == 'MISSING':
            desc = f'Invoice {inv} ({amt} \u2014 {client}) is no longer in the folder and will be removed'
            st.markdown(f'<div style="{_card};border-left:3px solid {C_RED}">'
                        f'<div style="color:{_t()["text"]};font-size:0.85rem">{desc}</div>'
                        f'</div>', unsafe_allow_html=True)

    # ── Expense changes ──
    if expense_changes:
        st.markdown(f"##### Expenses ({len(expense_changes)})")
    for c in expense_changes:
        cat = c.get('category', '')
        recip = c.get('recipient', '') or 'Unknown'
        amt = fmt_eur(c['netto']) if c.get('netto') else ''

        if c['change_type'] == 'NEW_EXPENSE':
            amt_str = f" ({amt})" if amt else ""
            desc = f"New expense: {cat} \u2014 {recip}{amt_str} will be added"
            st.markdown(f'<div style="{_card};border-left:3px solid {C_BLUE}">'
                        f'<div style="color:{_t()["text"]};font-size:0.85rem">{desc}</div>'
                        f'<div style="font-size:0.75rem;color:{_t()["muted"]};margin-top:0.25rem">{c.get("folder", "")}/{c["filename"]}</div>'
                        f'</div>', unsafe_allow_html=True)

        elif c['change_type'] == 'MISSING_EXPENSE':
            amt_str = f" ({amt})" if amt else ""
            desc = f"Expense: {cat} \u2014 {recip}{amt_str} \u2014 PDF removed from Drive, will be deleted"
            st.markdown(f'<div style="{_card};border-left:3px solid {C_RED}">'
                        f'<div style="color:{_t()["text"]};font-size:0.85rem">{desc}</div>'
                        f'<div style="font-size:0.75rem;color:{_t()["muted"]};margin-top:0.25rem">{c.get("month", "")} \u2014 ID {c.get("sheet_id", "")}</div>'
                        f'</div>', unsafe_allow_html=True)

    # --- Action buttons ---
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancel", use_container_width=True, key='sync_cancel'):
            st.rerun()
    with col2:
        if st.button("Apply Changes", type="primary", use_container_width=True, key='sync_apply'):
            with st.spinner("Applying changes..."):
                success = 0
                errors = 0

                # Apply invoice changes
                for c in changes:
                    ok = False
                    if c['change_type'] == 'STATUS':
                        ok = update_invoice_status_in_excel(c['invoice_number'], c['new_status'])
                    elif c['change_type'] == 'MISSING':
                        ok = remove_invoice_from_excel(c['invoice_number'])
                    elif c['change_type'] == 'NEW':
                        inv_date = c.get('date') or datetime.now()
                        month_name = MONTHS[inv_date.month - 1] if inv_date else ''
                        ok = add_invoice_to_excel({
                            'id': '',
                            'invoice_number': c['invoice_number'],
                            'date': inv_date,
                            'month': month_name,
                            'client': c.get('client', ''),
                            'project': c.get('project', ''),
                            'category': c.get('category', ''),
                            'netto': c.get('netto', 0),
                            'brutto': c.get('brutto', 0),
                        }, status=c['new_status'])
                    if ok:
                        success += 1
                    else:
                        errors += 1

                # Apply expense changes
                # First: removals (MISSING_EXPENSE) — process in reverse ID order
                # so that row deletions don't shift earlier rows
                missing_exps = [c for c in expense_changes
                                if c['change_type'] == 'MISSING_EXPENSE' and c.get('sheet_id')]
                missing_exps.sort(key=lambda c: int(c['sheet_id']) if str(c['sheet_id']).isdigit() else 0,
                                  reverse=True)
                for c in missing_exps:
                    try:
                        ok = remove_expense_from_excel(c['sheet_id'])
                        if ok:
                            success += 1
                        else:
                            errors += 1
                    except Exception:
                        errors += 1

                # Then: additions (NEW_EXPENSE)
                for c in expense_changes:
                    if c['change_type'] != 'NEW_EXPENSE' or not c.get('drive_file_id'):
                        continue
                    try:
                        # Parse date from filename date_str (DD.MM) + year from folder
                        year = CURRENT_YEAR
                        m_folder = re.match(r'^(\d{2})_(\w+)_(\d{4})$', c.get('month_folder', ''))
                        if m_folder:
                            year = int(m_folder.group(3))
                        day, month_num = int(c['date_str'][:2]), int(c['date_str'][3:5])
                        expense_date = datetime(year, month_num, day)
                        month_name = MONTHS[month_num - 1]
                        append_expense_to_excel({
                            'date': expense_date,
                            'month': month_name,
                            'recipient': c.get('recipient', ''),
                            'category': c.get('category', ''),
                            'currency': 'EUR',
                            'original_amount': c.get('netto', 0),
                            'netto': c.get('netto', 0),
                            'brutto': c.get('netto', 0),
                            'notes': f"Auto-imported from {c['filename']}",
                        })
                        success += 1
                    except Exception:
                        errors += 1

                _invalidate_data_caches()
                _log_activity('Sync Applied', f"{success} change(s) applied, {errors} error(s)")
                if errors:
                    st.warning(f"Applied {success} change(s). {errors} could not be applied.")
                else:
                    st.success(f"All {success} change(s) applied successfully.")
                import time; time.sleep(0.8)
                st.rerun()


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    data = load_data()

    # ── Theme Toggle ──
    if 'theme' not in st.session_state:
        st.session_state['theme'] = 'light'

    # ── Header ──
    today = datetime.now()
    date_str = f"({today.strftime('%d.%m.%y')})"
    t = _t()

    h1, h2, h3, h4 = st.columns([4, 1.5, 1.5, 0.6])
    with h1:
        st.markdown(f"""
        <div style="text-align:left;padding-top:0.6rem">
            <span class="js-date">Cloud Finance Dashboard {date_str}</span>
        </div>
        """, unsafe_allow_html=True)
    with h2:
        update_clicked = st.button("Update", key="update_btn", type="primary")
    with h3:
        upload_clicked = st.button("+ Upload", key="upload_btn", type="primary")
    with h4:
        st.markdown('<div class="theme-toggle">', unsafe_allow_html=True)
        icon = '\u2600\ufe0f' if st.session_state['theme'] == 'dark' else '\U0001f319'
        if st.button(icon, key='theme_toggle', help='Toggle light/dark mode'):
            st.session_state['theme'] = 'dark' if st.session_state['theme'] == 'light' else 'light'
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    if update_clicked:
        sync_invoices_dialog()
    if upload_clicked:
        upload_expense_dialog()

    st.markdown(f'<div style="border-bottom:1px solid {t["border"]};margin-bottom:0.75rem"></div>',
                unsafe_allow_html=True)

    # ── Auto-scan for invoice changes on refresh ──
    auto_changes, scan_errors = _auto_scan_changes()
    if scan_errors:
        for err in scan_errors:
            st.warning(f"Scan error: {err}", icon="\u26a0\ufe0f")
    if auto_changes:
        n_status = sum(1 for c in auto_changes if c['change_type'] == 'STATUS')
        n_new = sum(1 for c in auto_changes if c['change_type'] == 'NEW')
        n_missing = sum(1 for c in auto_changes if c['change_type'] == 'MISSING')
        n_new_exp = sum(1 for c in auto_changes if c['change_type'] == 'NEW_EXPENSE')
        n_miss_exp = sum(1 for c in auto_changes if c['change_type'] == 'MISSING_EXPENSE')
        parts = []
        if n_status:
            parts.append(f"{n_status} status change{'s' if n_status > 1 else ''}")
        if n_new:
            parts.append(f"{n_new} new invoice{'s' if n_new > 1 else ''}")
        if n_missing:
            parts.append(f"{n_missing} missing invoice{'s' if n_missing > 1 else ''}")
        if n_new_exp:
            parts.append(f"{n_new_exp} new expense{'s' if n_new_exp > 1 else ''}")
        if n_miss_exp:
            parts.append(f"{n_miss_exp} removed expense{'s' if n_miss_exp > 1 else ''}")
        summary = ", ".join(parts)
        st.markdown(f"""
        <div style="background:rgba(232,101,26,0.12);border:1px solid rgba(232,101,26,0.3);
                    border-radius:8px;padding:0.6rem 1rem;margin-bottom:0.75rem;
                    font-size:0.85rem;color:{_t()["text"]}">
            <strong>{len(auto_changes)} change{'s' if len(auto_changes) > 1 else ''} detected:</strong>
            {summary}. Click <strong>Update</strong> to review and apply.
        </div>
        """, unsafe_allow_html=True)

    # ── Top KPIs ──
    overview = data['overview']
    total_income = overview['Income'].sum()
    total_expenses = overview['Expenses'].sum()
    net_pl = total_income - total_expenses
    goal_info = _parse_goal(data.get('goal_raw'))

    unpaid_total = 0
    unpaid_df = data.get('income_unpaid', pd.DataFrame())
    if len(unpaid_df):
        unpaid_total = pd.to_numeric(
            unpaid_df.get('Netto (€)', pd.Series(dtype=float)), errors='coerce'
        ).sum()

    # ── Tabs ──
    t1, t2, t3, t4, t5, t6, t7 = st.tabs([
        'OVERVIEW',
        'EXPENSES',
        'INCOME',
        'INVOICES / OFFERS',
        'GOAL TRACKER',
        'TAXES',
        '2025',
    ])

    # Auto-navigate back to Expenses tab after dialog actions
    if st.session_state.get('_return_to_expenses'):
        st.session_state['_return_to_expenses'] = False
        components.html("""
            <script>
                const tabs = window.parent.document.querySelectorAll('button[data-baseweb="tab"]');
                if (tabs.length > 1) tabs[1].click();
            </script>
        """, height=0)

    with t1:
        tab_overview(data)
    with t2:
        tab_expenses(data)
    with t3:
        tab_income(data)
    with t4:
        tab_invoices_offers(data)
    with t5:
        tab_goal(data)
    with t6:
        tab_taxes(data)
    with t7:
        tab_2025(data)

    # ── Activity Log (#10) ──
    log_entries = _load_activity_log()
    if log_entries:
        with st.expander(f"Activity Log ({len(log_entries)} entries)", expanded=False):
            _lt = _t()
            log_html = f'<div style="font-size:0.78rem;font-family:monospace;color:{_lt["text"]};max-height:300px;overflow-y:auto">'
            for entry in log_entries[:50]:
                log_html += (
                    f'<div style="padding:0.25rem 0;border-bottom:1px solid {_lt["row_border"]}">'
                    f'<span style="color:{_lt["text_secondary"]}">{entry["timestamp"]}</span> '
                    f'<span style="color:{C_ORANGE}">{entry["action"]}</span> '
                    f'<span>{entry["details"]}</span></div>'
                )
            log_html += '</div>'
            st.markdown(log_html, unsafe_allow_html=True)

    # ── Footer ──
    st.markdown(f"""
    <div class="js-footer">
        JOSEF SINDELKA — FINANCE DASHBOARD 2026 — DATA AUTO-REFRESHES EVERY 30S
    </div>
    """, unsafe_allow_html=True)


def _check_password():
    """Password gate for Streamlit Cloud. Returns True if authenticated."""
    if 'app_password' not in st.secrets:
        return True  # no password configured (local dev)
    if st.session_state.get('authenticated'):
        return True
    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:center;min-height:60vh">
        <div style="text-align:center;max-width:340px">
            <div style="font-size:2.5rem;margin-bottom:1rem">💼</div>
            <h2 style="color:{_t()["text"]};margin-bottom:0.5rem">Finance Dashboard</h2>
            <p style="color:{_t()["muted"]};font-size:0.85rem;margin-bottom:1.5rem">
                Enter password to continue</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    _, col, _ = st.columns([1, 1, 1])
    with col:
        pwd = st.text_input("Password", type="password", key="_login_pwd",
                            label_visibility="collapsed", placeholder="Password")
        if st.button("Log in", type="primary", use_container_width=True):
            if pwd == st.secrets['app_password']:
                st.session_state['authenticated'] = True
                st.rerun()
            else:
                st.error("Incorrect password")
    st.stop()


if __name__ == '__main__':
    _check_password()
    main()
