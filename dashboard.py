"""
Business Finance Dashboard 2026
================================
Interactive Streamlit dashboard for tracking business finances.
Reads live data from Google Sheets (2026_Business_Finance).

Run:  streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime
from copy import copy
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

MONTHS = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
]

# ─── Design Tokens (matching Finance_Dashboard_2026.html) ────────────────────
C_BG = '#040c16'
C_SURFACE = '#0a1628'
C_SURFACE2 = '#0e1e34'
C_SURFACE3 = '#142840'
C_BORDER = '#1a3050'
C_TEXT = '#f0f0f0'
C_MUTED = '#888'
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

FONT = "-apple-system, 'Helvetica Neue', Helvetica, Arial, sans-serif"

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
BASE_DIR = Path(__file__).parent / "2026"  # kept for legacy references only


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


def _gsheet():
    """Return the gspread Spreadsheet object."""
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

# ─── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="JS() Finance 2026",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─── Custom CSS ──────────────────────────────────────────────────────────────
# Glass design tokens
_GLASS_BG = 'rgba(255,255,255,0.025)'
_GLASS_BG_HOVER = 'rgba(255,255,255,0.05)'
_GLASS_BORDER = 'rgba(255,255,255,0.06)'
_GLASS_BORDER_HOVER = 'rgba(255,255,255,0.12)'
_GLASS_BLUR = '40px'

st.markdown(f"""
<style>
    /* ── Gradient Background ── */
    @keyframes gradientShift {{
        0%   {{ background-position: 0% 50%; }}
        50%  {{ background-position: 100% 50%; }}
        100% {{ background-position: 0% 50%; }}
    }}

    .stApp {{
        background: linear-gradient(160deg,
            #0c0a14 0%,
            #0a1428 12%,
            #0b2240 26%,
            #0e3558 40%,
            #0a2e50 52%,
            #082a4a 64%,
            #0a3048 75%,
            #071e38 88%,
            #040c16 100%
        );
        background-size: 300% 300%;
        animation: gradientShift 60s ease infinite;
        font-family: {FONT};
        min-height: 100vh;
    }}
    .stApp > header {{
        background-color: transparent;
    }}

    /* ── Film Grain ── */
    .stApp::before {{
        content: '';
        position: fixed;
        top: 0; left: 0;
        width: 100%; height: 100%;
        pointer-events: none;
        z-index: 1;
        opacity: 0.02;
        background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E");
        background-repeat: repeat;
        background-size: 256px 256px;
    }}

    .stMainBlockContainer {{
        max-width: 1400px;
        padding: 1.5rem 2.5rem 3rem 2.5rem;
        position: relative;
        z-index: 2;
    }}

    /* ── Date Header ── */
    .js-date {{
        font-family: {FONT};
        font-size: 0.7rem;
        color: rgba(255,255,255,0.4);
        letter-spacing: 0.08em;
        font-weight: 400;
    }}

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 0.35rem;
        background-color: transparent;
        border-bottom: none;
        padding: 0.5rem 0;
    }}
    .stTabs [data-baseweb="tab"] {{
        background: transparent;
        border: 1px solid transparent;
        border-radius: 999px;
        padding: 0.45rem 1.3rem;
        color: rgba(255,255,255,0.35);
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
        color: rgba(255,255,255,0.7);
        background: {_GLASS_BG};
    }}
    .stTabs [aria-selected="true"] {{
        background: {C_ORANGE} !important;
        border-color: transparent !important;
        color: #fff !important;
        font-weight: 600;
    }}
    .stTabs [data-baseweb="tab-highlight"] {{
        display: none;
    }}
    .stTabs [data-baseweb="tab-border"] {{
        display: none;
    }}

    /* ── Cards ── */
    .card {{
        background: {_GLASS_BG};
        backdrop-filter: blur({_GLASS_BLUR});
        -webkit-backdrop-filter: blur({_GLASS_BLUR});
        border: 1px solid {_GLASS_BORDER};
        border-radius: 16px;
        padding: 1.25rem 1.5rem;
        transition: border-color 0.3s ease;
        margin-bottom: 0.5rem;
    }}
    .card:hover {{
        border-color: {_GLASS_BORDER_HOVER};
    }}
    .card-label {{
        font-size: 0.55rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: rgba(255,255,255,0.35);
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
        color: {C_TEXT};
        white-space: nowrap;
    }}
    .card-sub {{
        font-size: 0.7rem;
        color: rgba(255,255,255,0.3);
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
        background: {_GLASS_BG};
        backdrop-filter: blur({_GLASS_BLUR});
        -webkit-backdrop-filter: blur({_GLASS_BLUR});
        border: 1px solid {_GLASS_BORDER};
        border-radius: 16px;
        padding: 1.75rem;
        margin-bottom: 1.5rem;
    }}
    .chart-title {{
        font-size: 0.6rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: rgba(255,255,255,0.35);
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
        color: rgba(255,255,255,0.3);
        border-bottom: 1px solid {_GLASS_BORDER};
        font-weight: 500;
    }}
    .data-table td {{
        padding: 0.85rem 1rem;
        border-bottom: 1px solid rgba(255,255,255,0.03);
        color: {C_TEXT};
    }}
    .data-table tr:hover td {{
        background: {_GLASS_BG};
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
        background: {_GLASS_BG_HOVER};
        font-weight: 600;
        border-top: 1px solid {_GLASS_BORDER};
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

    /* ── Progress Bar ── */
    .progress-bar-bg {{
        width: 100%;
        height: 10px;
        background: {_GLASS_BG};
        backdrop-filter: blur({_GLASS_BLUR});
        -webkit-backdrop-filter: blur({_GLASS_BLUR});
        border: 1px solid {_GLASS_BORDER};
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
        color: rgba(255,255,255,0.25);
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
        background: {_GLASS_BG};
        backdrop-filter: blur({_GLASS_BLUR});
        -webkit-backdrop-filter: blur({_GLASS_BLUR});
        border: 1px solid {_GLASS_BORDER};
        border-radius: 12px;
        padding: 0.75rem 0.5rem;
        text-align: center;
        transition: border-color 0.25s ease;
    }}
    .month-card:hover {{
        border-color: {_GLASS_BORDER_HOVER};
    }}
    .month-card .m-label {{
        font-size: 0.55rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: rgba(255,255,255,0.3);
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
        border-bottom: 1px solid rgba(255,255,255,0.03);
        font-size: 0.82rem;
        font-family: {FONT};
    }}
    .summary-row:last-child {{ border-bottom: none; }}
    .summary-row .s-label {{ color: rgba(255,255,255,0.35); font-weight: 400; }}
    .summary-row .s-value {{ font-weight: 600; }}

    /* ── Footer ── */
    .js-footer {{
        text-align: center;
        font-size: 0.55rem;
        color: rgba(255,255,255,0.2);
        letter-spacing: 0.12em;
        text-transform: uppercase;
        padding: 3rem 0 1.5rem 0;
        border-top: 1px solid {_GLASS_BORDER};
        margin-top: 3rem;
        font-family: {FONT};
        font-weight: 400;
    }}

    /* ── Section Headers ── */
    .section-hdr {{
        font-size: 0.6rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: rgba(255,255,255,0.35);
        margin: 2rem 0 1.25rem 0;
        font-family: {FONT};
        font-weight: 500;
    }}

    /* ── Override Streamlit defaults ── */
    #MainMenu {{visibility: hidden;}}
    header[data-testid="stHeader"] {{background: transparent; pointer-events: none;}}
    header[data-testid="stHeader"] * {{pointer-events: none;}}
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
        border-radius: 999px;
        border: 1px solid {_GLASS_BORDER};
        background: {_GLASS_BG};
        backdrop-filter: blur({_GLASS_BLUR});
        -webkit-backdrop-filter: blur({_GLASS_BLUR});
        color: rgba(255,255,255,0.7);
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
        background: {_GLASS_BG_HOVER};
        color: #fff;
        border-color: {_GLASS_BORDER_HOVER};
    }}
    .stButton > button[kind="primary"] {{
        background: {C_ORANGE};
        border: 1px solid transparent;
        color: #fff;
        font-weight: 600;
    }}
    .stButton > button[kind="primary"]:hover {{
        background: {C_ORANGE_LIGHT};
        border-color: transparent;
    }}

    /* ── Transaction rows ── */
    .tx-row {{
        display: flex;
        align-items: center;
        padding: 0.7rem 1rem;
        border-bottom: 1px solid rgba(255,255,255,0.03);
        font-size: 0.82rem;
        color: {C_TEXT};
        font-family: {FONT};
    }}
    .tx-row:hover {{
        background: {_GLASS_BG};
    }}
    .tx-header {{
        display: flex;
        align-items: center;
        padding: 0.85rem 1rem;
        border-bottom: 1px solid {_GLASS_BORDER};
        font-size: 0.55rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: rgba(255,255,255,0.3);
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

    /* ── Dialog ── */
    div[data-testid="stModal"] > div {{
        background: rgba(6,12,24,0.92);
        backdrop-filter: blur(50px);
        -webkit-backdrop-filter: blur(50px);
        border: 1px solid {_GLASS_BORDER};
        border-radius: 16px;
    }}

    /* ── Inputs ── */
    .stSelectbox > div > div,
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stDateInput > div > div > input {{
        background: {_GLASS_BG};
        border-color: {_GLASS_BORDER};
        border-radius: 10px;
        color: {C_TEXT};
        font-family: {FONT};
    }}

    /* ── Plotly ── */
    .js-plotly-plot .plotly .main-svg {{
        background: transparent !important;
    }}
</style>
""", unsafe_allow_html=True)


# ─── Data Loading ────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_data():
    """Load all data from Google Sheets with 30s cache."""
    try:
        sh = _gsheet()
    except Exception as e:
        import traceback
        st.error(f"Cannot connect to Google Sheet: {type(e).__name__}: {e}")
        st.code(traceback.format_exc())
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

def dark_layout(fig, height=400, **kw):
    """Apply consistent dark styling to a Plotly figure matching the design."""
    fig.update_layout(
        height=height,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color=C_MUTED, size=12, family=FONT),
        margin=dict(l=40, r=20, t=40, b=40),
        legend=dict(
            bgcolor='rgba(0,0,0,0)', bordercolor='rgba(0,0,0,0)',
            font=dict(color=C_MUTED, size=11, family=FONT),
        ),
        xaxis=dict(gridcolor='rgba(255,255,255,0.04)', zerolinecolor='rgba(255,255,255,0.06)'),
        yaxis=dict(gridcolor='rgba(255,255,255,0.04)', zerolinecolor='rgba(255,255,255,0.06)'),
        **kw,
    )
    return fig


def metric_card(label, value, sub=None, color_class=''):
    """Render a styled metric card matching the HTML design."""
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


def fmt_eur(v):
    """Format a number as Euro currency."""
    if v < 0:
        return f"\u2212\u20ac{abs(v):,.2f}"
    return f"\u20ac{v:,.2f}"


def badge_html(text, category=None):
    """Generate an inline badge span for a category."""
    colors = BADGE_STYLES.get(category or text, (C_MUTED, C_SURFACE3))
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
        textfont=dict(size=13, color=C_TEXT, family=FONT),
        hovertemplate='<b>%{label}</b><br>\u20ac%{value:,.2f}<br>%{percent}<extra></extra>',
    )])
    dark_layout(fig, height=650)
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
        textposition='outside', textfont=dict(color=C_TEXT, size=10, family=FONT),
    ))
    dark_layout(fig_vendor, height=max(300, len(vendor) * 32))
    st.plotly_chart(fig_vendor, use_container_width=True)

    # --- Transaction History Table with Edit/Delete ---
    section_title('Transaction History')

    st.markdown("""<div class="tx-header">
        <span style="flex:0.4">&#35;</span>
        <span style="flex:1.2">DATE</span>
        <span style="flex:2.2">RECIPIENT</span>
        <span style="flex:1.8">CATEGORY</span>
        <span style="flex:1.4;text-align:right">AMOUNT</span>
        <span style="flex:1.6;text-align:center">ACTIONS</span>
    </div>""", unsafe_allow_html=True)

    sorted_exp = df.sort_values('Date of Payment', ascending=False).reset_index(drop=True)
    total_count = len(sorted_exp)

    for idx, (_, r) in enumerate(sorted_exp.iterrows(), 1):
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
        cell_style = 'font-size:0.82rem;color:#f0f0f0;padding:0.3rem 0;font-family:-apple-system,Helvetica Neue,Helvetica,Arial,sans-serif;'
        with cols[0]:
            st.markdown(f'<div style="{cell_style}color:rgba(255,255,255,0.35)">{display_num}</div>', unsafe_allow_html=True)
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
                textfont=dict(size=12, color=C_TEXT, family=FONT),
            )])
            dark_layout(fig, height=320)
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
                textposition='outside', textfont=dict(color=C_TEXT, size=11, family=FONT),
            ))
            dark_layout(fig2, height=max(250, len(cr) * 55))
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

        _cs = 'font-size:0.82rem;color:#f0f0f0;padding:0.3rem 0;font-family:-apple-system,Helvetica Neue,Helvetica,Arial,sans-serif;'
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
                st.markdown(f'<div style="{_cs}color:rgba(255,255,255,0.35)">{idx}</div>', unsafe_allow_html=True)
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
            <span style="flex:0.5">STATUS</span>
            <span style="flex:0.6;text-align:center">ACTION</span>
        </div>""", unsafe_allow_html=True)

        _cs2 = 'font-size:0.82rem;color:#f0f0f0;padding:0.3rem 0;font-family:-apple-system,Helvetica Neue,Helvetica,Arial,sans-serif;'
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

            cols = st.columns([0.7, 0.7, 1.2, 1.0, 0.9, 0.6, 0.5, 0.6])
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
                st.markdown(f'<div style="{_cs2}"><span class="badge" style="color:{C_RED};background:{C_RED_DIM}">Unpaid</span></div>', unsafe_allow_html=True)
            with cols[7]:
                st.markdown('<div class="tx-actions tx-del">', unsafe_allow_html=True)
                if st.button("Delete", key=f"del_unpaid_{inv_num}"):
                    delete_income_invoice_dialog(r.to_dict(), 'unpaid')
                st.markdown('</div>', unsafe_allow_html=True)

    elif total_unpaid == 0:
        st.markdown(f"""
        <div class="card" style="border-color: rgba(74,222,128,0.2); background: linear-gradient(135deg, {C_GREEN_DIM} 0%, {C_SURFACE} 100%);">
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
        st.markdown(f"""
        <div class="card" style="border-color: rgba(232,93,38,0.2); background: linear-gradient(135deg, {C_SURFACE} 0%, rgba(232,93,38,0.04) 100%);">
            <div class="card-label">Annual Goal</div>
            <div class="card-value accent">\u20ac{goal:,.0f}</div>
            <div class="card-sub">Sep 2025 \u2013 Dec 2026</div>
        </div>
        """, unsafe_allow_html=True)
    with k2:
        metric_card('Acquired', acquired, sub=f'{pct:.1f}% achieved', color_class='positive')
    with k3:
        metric_card('Remaining', remaining, sub=f'{100-pct:.1f}% to go')
    with k4:
        st.markdown(f"""
        <div class="card">
            <div class="card-label">Monthly Target</div>
            <div class="card-value" style="color:{C_ORANGE_LIGHT}">{fmt_eur(monthly_target)}</div>
            <div class="card-sub">Avg needed over remaining {months_left} months</div>
        </div>
        """, unsafe_allow_html=True)

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
            line=dict(color=C_SURFACE3, width=2, dash='dash'),
        ))
        fig.update_layout(
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        )
        dark_layout(fig, height=350)
        fig.update_yaxes(range=[0, goal * 1.1])
        st.plotly_chart(fig, use_container_width=True)


# ─── TAB 5 — 2025 ───────────────────────────────────────────────────────────

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
    dark_layout(fig, height=380)
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
        dark_layout(fig2, height=380)
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
    """Extract date, amount, vendor, and category from a PDF invoice."""
    result = {'date': None, 'netto': 0.0, 'vendor': '', 'currency': 'EUR', 'category': None}
    try:
        import pdfplumber
    except ImportError:
        return result

    try:
        uploaded_file.seek(0)
        with pdfplumber.open(uploaded_file) as pdf:
            text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
    except Exception:
        return result

    if not text.strip():
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

    return result


def save_expense_pdf(uploaded_file, date, category, recipient):
    """Upload PDF to Google Drive in the correct monthly folder."""
    month_num = date.month
    month_name = MONTHS[month_num - 1]

    folder_2026 = _drive_find_folder.__wrapped__(DRIVE_ROOT_FOLDER, '2026')
    if not folder_2026:
        folder_2026 = _drive_get_or_create_folder(DRIVE_ROOT_FOLDER, '2026')
    month_folder_name = f"{month_num:02d}_{month_name}_{date.year}"
    month_folder = _drive_get_or_create_folder(folder_2026, month_folder_name)
    cost_folder = _drive_get_or_create_folder(month_folder, '04_Irregular Cost')
    _drive_get_or_create_folder(month_folder, '03_Regular Cost')

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
    """
    if not isinstance(date, datetime):
        try:
            date = pd.to_datetime(date)
        except Exception:
            return None

    month_num = date.month
    month_name = MONTHS[month_num - 1]
    month_folder_name = f"{month_num:02d}_{month_name}_{date.year}"

    folder_2026 = _drive_find_folder.__wrapped__(DRIVE_ROOT_FOLDER, '2026')
    if not folder_2026:
        return None
    month_folder = _drive_find_folder.__wrapped__(folder_2026, month_folder_name)
    if not month_folder:
        return None

    day_prefix = f"{date.day:02d}.{date.month:02d}."
    cat_code = CATEGORY_FILE_MAP.get(category, category.replace(' ', '_'))
    clean_recipient = re.sub(r'[^\w\s-]', '', recipient).strip().replace(' ', '_')

    for subfolder_name in ['04_Irregular Cost', '03_Regular Cost']:
        subfolder_id = _drive_find_folder.__wrapped__(month_folder, subfolder_name)
        if not subfolder_id:
            continue
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

    folder_2026 = _drive_find_folder.__wrapped__(DRIVE_ROOT_FOLDER, '2026')
    if not folder_2026:
        return None
    month_folder_name = f"{new_month_num:02d}_{new_month_name}_{new_date.year}"
    month_folder = _drive_get_or_create_folder(folder_2026, month_folder_name)
    cost_folder = _drive_get_or_create_folder(month_folder, '04_Irregular Cost')

    cat_code = CATEGORY_FILE_MAP.get(new_category, new_category.replace(' ', '_'))
    clean_recipient = re.sub(r'[^\w\s-]', '', new_recipient).strip().replace(' ', '_')
    new_name = f"{new_date.day:02d}.{new_date.month:02d}._{cat_code}_{clean_recipient}.pdf"

    new_parent = cost_folder if cost_folder != old_pdf_info.get('folder_id') else None
    _drive_rename_file(old_pdf_info['id'], new_name, new_parent)
    return new_name


# ─── Invoice Sync Helpers ────────────────────────────────────────────────────

INVOICES_DIR = BASE_DIR / "INVOICES 2026"  # legacy reference


def _get_invoices_folder_id():
    """Get the Google Drive folder ID for INVOICES 2026."""
    folder_2026 = _drive_find_folder.__wrapped__(DRIVE_ROOT_FOLDER, '2026')
    if not folder_2026:
        return None
    return _drive_find_folder.__wrapped__(folder_2026, 'INVOICES 2026')


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
    """Cached auto-scan for Drive invoice changes on page load."""
    try:
        return scan_invoice_changes()
    except Exception:
        return []


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


@st.dialog("Upload Expense")
def upload_expense_dialog():
    """Modal dialog for uploading a PDF expense and adding it to the spreadsheet."""
    uploaded = st.file_uploader("Upload PDF invoice", type=['pdf'], key='expense_pdf')

    if uploaded is None:
        st.info("Upload a PDF invoice to get started.")
        return

    extracted = extract_pdf_data(uploaded)

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
            st.cache_data.clear()

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
                    st.cache_data.clear()
                    st.success("Expense deleted successfully.")
                    import time; time.sleep(0.5)
                    st.session_state['_return_to_expenses'] = True
                    st.rerun()
                else:
                    st.error("Could not find the expense row in the spreadsheet.")


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
                st.cache_data.clear()
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
                    st.cache_data.clear()
                    st.success("Expense updated successfully.")
                    import time; time.sleep(0.5)
                    st.session_state['_return_to_expenses'] = True
                    st.rerun()
                else:
                    st.error("Could not find the expense row in the spreadsheet.")


@st.dialog("Update Invoices", width="large")
def sync_invoices_dialog():
    """Scan INVOICES 2026/ for changes and sync with Excel."""
    changes = scan_invoice_changes()

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

    if not changes:
        st.markdown("**No changes detected.**")
        st.markdown("All invoice filenames in `INVOICES 2026/` match the current Excel data.")
        if st.button("Close", use_container_width=True, key='sync_close'):
            st.rerun()
        return

    _card = (f'background:{_GLASS_BG};backdrop-filter:blur({_GLASS_BLUR});'
             f'-webkit-backdrop-filter:blur({_GLASS_BLUR});border:1px solid {_GLASS_BORDER};'
             f'border-radius:12px;padding:1rem;margin-bottom:0.5rem')

    st.markdown(f"**Found {len(changes)} change(s)**")
    st.markdown("Review the changes below, then click **Apply Changes** to update the spreadsheet.")

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
                        f'<div style="color:{C_TEXT};font-size:0.85rem">{desc}</div>'
                        f'</div>', unsafe_allow_html=True)

        elif c['change_type'] == 'NEW':
            status_word = 'unpaid' if c['new_status'] == 'unpaid' else 'paid'
            if c['netto']:
                desc = f'Invoice {inv} ({amt} \u2014 {client}) will be added to {status_word} invoices'
            else:
                desc = f'Invoice {inv} (new PDF found) will be added to {status_word} invoices'
            st.markdown(f'<div style="{_card};border-left:3px solid {C_BLUE}">'
                        f'<div style="color:{C_TEXT};font-size:0.85rem">{desc}</div>'
                        f'<div style="font-size:0.75rem;color:{C_MUTED};margin-top:0.25rem">{c["filename"]}</div>'
                        f'</div>', unsafe_allow_html=True)

        elif c['change_type'] == 'MISSING':
            desc = f'Invoice {inv} ({amt} \u2014 {client}) is no longer in the folder and will be removed'
            st.markdown(f'<div style="{_card};border-left:3px solid {C_RED}">'
                        f'<div style="color:{C_TEXT};font-size:0.85rem">{desc}</div>'
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

                st.cache_data.clear()
                if errors:
                    st.warning(f"Applied {success} change(s). {errors} could not be applied.")
                else:
                    st.success(f"All {success} change(s) applied successfully.")
                import time; time.sleep(0.8)
                st.rerun()


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    data = load_data()

    # ── Header ──
    today = datetime.now()
    date_str = f"({today.strftime('%d.%m.%y')})"

    h1, h2, h3 = st.columns([5, 1.5, 1.5])
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

    if update_clicked:
        sync_invoices_dialog()
    if upload_clicked:
        upload_expense_dialog()

    st.markdown('<div style="border-bottom:1px solid rgba(255,255,255,0.06);margin-bottom:0.75rem"></div>',
                unsafe_allow_html=True)

    # ── Auto-scan for invoice changes on refresh ──
    auto_changes = _auto_scan_changes()
    if auto_changes:
        n_status = sum(1 for c in auto_changes if c['change_type'] == 'STATUS')
        n_new = sum(1 for c in auto_changes if c['change_type'] == 'NEW')
        n_missing = sum(1 for c in auto_changes if c['change_type'] == 'MISSING')
        parts = []
        if n_status:
            parts.append(f"{n_status} status change{'s' if n_status > 1 else ''}")
        if n_new:
            parts.append(f"{n_new} new invoice{'s' if n_new > 1 else ''}")
        if n_missing:
            parts.append(f"{n_missing} missing invoice{'s' if n_missing > 1 else ''}")
        summary = ", ".join(parts)
        st.markdown(f"""
        <div style="background:rgba(232,101,26,0.12);border:1px solid rgba(232,101,26,0.3);
                    border-radius:8px;padding:0.6rem 1rem;margin-bottom:0.75rem;
                    font-size:0.85rem;color:{C_TEXT}">
            <strong>{len(auto_changes)} invoice change{'s' if len(auto_changes) > 1 else ''} detected:</strong>
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
    t1, t2, t3, t4, t5 = st.tabs([
        'OVERVIEW',
        'EXPENSES',
        'INCOME',
        'GOAL TRACKER',
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
        tab_goal(data)
    with t5:
        tab_2025(data)

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
            <h2 style="color:{C_TEXT};margin-bottom:0.5rem">Finance Dashboard</h2>
            <p style="color:{C_MUTED};font-size:0.85rem;margin-bottom:1.5rem">
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
