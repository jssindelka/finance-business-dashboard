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
from datetime import datetime, timedelta
import re
import os
import json
import io
import base64
import tempfile
import streamlit.components.v1 as components
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from fpdf import FPDF

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
        'surface3': '#F0F0F0',
        'border': '#E5E5E5',
        'border_hover': '#D4D4D4',
        'text': '#111111',
        'text_secondary': '#525252',
        'muted': '#737373',
        'card_shadow': 'none',
        'card_shadow_hover': 'none',
        'chart_grid': '#F5F5F5',
        'chart_zeroline': '#E5E5E5',
        'row_hover': '#FAFAFA',
        'row_border': '#F5F5F5',
        'sidebar_bg': '#111111',
        'sidebar_text': '#FFFFFF',
        'sidebar_text_dim': '#A3A3A3',
        'sidebar_section': '#525252',
        'sidebar_border': '#404040',
        'sidebar_hover': '#262626',
    },
    'dark': {
        'bg': '#0A0A0A',
        'surface': '#141414',
        'surface2': '#1A1A1A',
        'surface3': '#262626',
        'border': '#2A2A2A',
        'border_hover': '#404040',
        'text': '#F0F0F0',
        'text_secondary': '#A3A3A3',
        'muted': '#737373',
        'card_shadow': 'none',
        'card_shadow_hover': 'none',
        'chart_grid': '#1A1A1A',
        'chart_zeroline': '#2A2A2A',
        'row_hover': 'rgba(255,255,255,0.03)',
        'row_border': '#1A1A1A',
        'sidebar_bg': '#0A0A0A',
        'sidebar_text': '#F0F0F0',
        'sidebar_text_dim': '#737373',
        'sidebar_section': '#404040',
        'sidebar_border': '#1A1A1A',
        'sidebar_hover': '#141414',
    },
}

def _t():
    """Return current theme dict based on session state."""
    return THEMES[st.session_state.get('theme', 'light')]

def _chart_primary():
    return '#111111' if st.session_state.get('theme', 'light') == 'light' else '#F0F0F0'

def _chart_primary_dim():
    return '#D4D4D4' if st.session_state.get('theme', 'light') == 'light' else '#404040'

# ─── Accent Colors (shared across themes) ───────────────────────────────────
C_PRIMARY = '#111111'
C_PRIMARY_LIGHT = '#404040'
C_GREEN = '#065F46'
C_GREEN_BG = '#D1FAE5'
C_GREEN_DIM = '#D1FAE5'
C_RED = '#C0392B'
C_RED_DIM = '#FEE2E2'
C_AMBER = '#92400E'
C_AMBER_BG = '#FEF3C7'
C_BLUE = '#525252'

CHART_COLORS = [
    '#111111', '#404040', '#737373', '#A3A3A3', '#D4D4D4',
    '#525252', '#8B8B8B', '#BFBFBF', '#E5E5E5', '#2A2A2A', '#666666',
]

# Badge color mapping: category → (text_color, bg_color)
BADGE_STYLES = {
    'AI Software': ('#525252', '#E5E5E5'),
    'AI Studio': ('#525252', '#E5E5E5'),
    'Accounting': ('#525252', '#E5E5E5'),
    'Insurance': ('#525252', '#E5E5E5'),
    'Office': ('#525252', '#E5E5E5'),
    'Miles': ('#525252', '#E5E5E5'),
    'Education': ('#525252', '#E5E5E5'),
    'Restaurants': ('#525252', '#E5E5E5'),
    'Travel Cost': ('#525252', '#E5E5E5'),
    'Gewerbe': ('#525252', '#E5E5E5'),
    'Gear': ('#525252', '#E5E5E5'),
    'Gear Rental': ('#525252', '#E5E5E5'),
    'Animation': ('#525252', '#E5E5E5'),
    'Photography': ('#525252', '#E5E5E5'),
    'Video Production': ('#525252', '#E5E5E5'),
    'Software': ('#525252', '#E5E5E5'),
}

FONT = "'Helvetica Neue', Helvetica, Arial, sans-serif"

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

INCOME_CATEGORIES = ['Photography', 'Animation', 'Video Production', 'AI Studio',
                     'AI Software', 'Software', 'Education']

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
    """Return the gspread Spreadsheet object (cached 5 min).
    Kept shorter than client TTL to ensure fresh auth tokens on Cloud."""
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
    initial_sidebar_state="expanded"
)

# ─── Custom CSS (theme-aware) ────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def _build_css(theme_name):
    """Build the CSS string for a given theme. Cached — only rebuilds on theme change."""
    t = THEMES[theme_name]
    is_dark = theme_name == 'dark'
    positive_color = '#4ADE80' if is_dark else '#065F46'
    negative_color = '#F87171' if is_dark else '#C0392B'
    badge_paid_color = '#4ADE80' if is_dark else '#065F46'
    badge_paid_bg = '#262626' if is_dark else '#D1FAE5'
    badge_sent_color = '#A3A3A3' if is_dark else '#92400E'
    badge_sent_bg = '#262626' if is_dark else '#FEF3C7'
    badge_draft_color = '#737373' if is_dark else '#525252'
    badge_draft_bg = '#262626' if is_dark else '#E5E5E5'
    return f"""
<style>

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
        max-width: 1100px;
        padding: 40px 48px;
    }}

    /* ── Date Header ── */
    .js-date {{
        font-family: {FONT};
        font-size: 13px;
        color: {t['text_secondary']};
        letter-spacing: 0.3px;
        font-weight: 400;
    }}

    /* ── Tabs (pill style) ── */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 4px;
        background: {t['surface']};
        border: 1px solid {t['border']};
        border-radius: 3px;
        padding: 4px;
        border-bottom: none;
    }}
    .stTabs [data-baseweb="tab"] {{
        background: transparent;
        border: none;
        border-radius: 3px;
        padding: 6px 14px;
        color: {t['text_secondary']};
        font-family: {FONT};
        font-size: 12px;
        font-weight: 500;
        letter-spacing: 0.3px;
        text-transform: uppercase;
        transition: all 0.15s ease;
        white-space: nowrap;
        height: auto;
    }}
    .stTabs [data-baseweb="tab"]:hover {{
        color: {t['text']};
        background: transparent;
    }}
    .stTabs [aria-selected="true"] {{
        background: {t['text']} !important;
        color: {t['surface']} !important;
        font-weight: 600;
        border-radius: 3px;
        box-shadow: none;
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
        border-radius: 3px;
        padding: 20px;
        box-shadow: none;
        transition: border-color 0.2s ease;
        margin-bottom: 16px;
    }}
    .card:hover {{
        box-shadow: none;
        border-color: {t['border_hover']};
    }}
    .card-label {{
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: {t['muted']};
        margin-bottom: 4px;
        font-family: {FONT};
        font-weight: 600;
        line-height: 1.4;
    }}
    .card-value {{
        font-size: 24px;
        font-weight: 700;
        font-family: {FONT};
        letter-spacing: -0.3px;
        color: {t['text']};
        white-space: nowrap;
        font-variant-numeric: tabular-nums;
    }}
    .card-sub {{
        font-size: 13px;
        color: {t['muted']};
        margin-top: 4px;
        font-family: {FONT};
        font-weight: 400;
    }}
    .positive {{ color: {positive_color}; }}
    .negative {{ color: {negative_color}; }}
    .accent {{ color: {t['text']}; }}
    .blue {{ color: {C_BLUE}; }}

    /* ── Chart Containers ── */
    .chart-card {{
        background: {t['surface']};
        border: 1px solid {t['border']};
        border-radius: 3px;
        padding: 28px;
        margin-bottom: 20px;
        box-shadow: none;
    }}
    .chart-title {{
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: {t['muted']};
        margin-bottom: 16px;
        font-family: {FONT};
        font-weight: 600;
    }}

    /* ── Data Tables ── */
    .data-table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
        font-family: {FONT};
    }}
    .data-table th {{
        text-align: left;
        padding: 10px 12px;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: {t['muted']};
        border-bottom: 2px solid {t['border']};
        font-weight: 600;
    }}
    .data-table td {{
        padding: 12px;
        border-bottom: 1px solid {t['row_border']};
        color: {t['text']};
        font-size: 13px;
    }}
    .data-table tr:hover td {{
        background: {t['row_hover']};
    }}
    .data-table .num {{
        font-family: {FONT};
        text-align: right;
        font-size: 13px;
        font-weight: 600;
        letter-spacing: -0.02em;
        font-variant-numeric: tabular-nums;
    }}
    .data-table .total-row td {{
        background: {t['surface2']};
        font-weight: 600;
        border-top: 2px solid {t['border']};
    }}

    /* ── Badges ── */
    .badge {{
        display: inline-block;
        padding: 3px 10px;
        border-radius: 2px;
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.5px;
        text-transform: uppercase;
    }}
    .badge-paid {{ color: {badge_paid_color}; background: {badge_paid_bg}; }}
    .badge-sent {{ color: {badge_sent_color}; background: {badge_sent_bg}; }}
    .badge-draft {{ color: {badge_draft_color}; background: {badge_draft_bg}; }}

    /* ── Filter Pills (horizontal radio in dashboard) ── */
    [data-testid="stRadio"] > div[role="radiogroup"] {{
        gap: 0.4rem;
        flex-wrap: wrap;
    }}
    [data-testid="stRadio"] > div[role="radiogroup"] > label {{
        background: {t['surface']} !important;
        border: 1px solid {t['border_hover']} !important;
        border-radius: 3px !important;
        padding: 6px 14px !important;
        font-size: 12px !important;
        font-weight: 500;
        font-family: {FONT};
        color: {t['text_secondary']} !important;
        transition: all 0.15s ease;
        cursor: pointer;
    }}
    [data-testid="stRadio"] > div[role="radiogroup"] > label > div:last-child {{
        color: {t['text_secondary']} !important;
    }}
    [data-testid="stRadio"] > div[role="radiogroup"] > label:hover {{
        border-color: {t['text']} !important;
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
        padding: 6px 12px;
        font-size: 12px;
    }}

    /* ── Container borders (form cards) ── */
    [data-testid="stVerticalBlockBorderWrapper"] {{
        border-radius: 3px !important;
        border-color: {t['border']} !important;
    }}

    /* ── Progress Bar ── */
    .progress-bar-bg {{
        width: 100%;
        height: 10px;
        background: {t['surface3']};
        border: 1px solid {t['border']};
        border-radius: 3px;
        overflow: hidden;
        position: relative;
    }}
    .progress-bar-fill {{
        height: 100%;
        border-radius: 3px;
        background: {t['text']};
        font-size: 0;
        transition: width 1.5s ease;
    }}
    .goal-milestones {{
        display: flex;
        justify-content: space-between;
        margin-top: 8px;
        font-size: 11px;
        color: {t['muted']};
        font-family: {FONT};
        letter-spacing: 0.3px;
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
        border-radius: 3px;
        padding: 16px 12px;
        text-align: center;
        box-shadow: none;
        transition: border-color 0.15s ease;
    }}
    .month-card:hover {{
        border-color: {t['border_hover']};
    }}
    .month-card .m-label {{
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: {t['muted']};
        margin-bottom: 4px;
        font-family: {FONT};
        font-weight: 600;
    }}
    .month-card .m-value {{
        font-family: {FONT};
        font-size: 14px;
        font-weight: 600;
        letter-spacing: -0.02em;
        white-space: nowrap;
        font-variant-numeric: tabular-nums;
    }}

    /* ── Summary Row ── */
    .summary-row {{
        display: flex;
        justify-content: space-between;
        padding: 12px 0;
        border-bottom: 1px solid {t['row_border']};
        font-size: 13px;
        font-family: {FONT};
    }}
    .summary-row:last-child {{ border-bottom: none; }}
    .summary-row .s-label {{ color: {t['muted']}; font-weight: 400; }}
    .summary-row .s-value {{ font-weight: 600; color: {t['text']}; font-variant-numeric: tabular-nums; }}

    /* ── Footer ── */
    .js-footer {{
        text-align: center;
        font-size: 11px;
        color: {t['muted']};
        letter-spacing: 0.8px;
        text-transform: uppercase;
        padding: 40px 0 24px 0;
        border-top: 1px solid {t['border']};
        margin-top: 40px;
        font-family: {FONT};
        font-weight: 400;
    }}

    /* ── Section Headers ── */
    .section-hdr {{
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: {t['muted']};
        margin: 32px 0 16px 0;
        font-family: {FONT};
        font-weight: 600;
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
        color: {t['text_secondary']} !important;
        font-family: {FONT};
        font-weight: 600;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.8px;
    }}
    .stSelectbox label p,
    .stTextInput label p,
    .stNumberInput label p,
    .stDateInput label p,
    .stTextArea label p,
    .stFileUploader label p {{
        color: {t['text_secondary']} !important;
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
        border-radius: 3px;
        overflow: hidden;
    }}

    /* ── Buttons ── */
    .stButton > button {{
        border-radius: 3px;
        border: 1px solid {t['border_hover']};
        background: {t['surface']};
        color: {t['text']};
        font-family: {FONT};
        font-weight: 600;
        font-size: 13px;
        letter-spacing: 0.3px;
        text-transform: none;
        padding: 10px 20px;
        transition: all 0.15s ease;
        white-space: nowrap;
        box-shadow: none;
    }}
    .stButton > button:hover {{
        background: {t['surface']};
        color: {t['text']};
        border-color: {t['text']};
    }}
    .stButton > button[kind="primary"] {{
        background: {t['text']};
        border: 1px solid {t['text']};
        color: {t['surface']};
        font-weight: 600;
        box-shadow: none;
    }}
    .stButton > button[kind="primary"]:hover {{
        background: {t['text']};
        border-color: {t['text']};
        box-shadow: none;
        opacity: 0.85;
    }}

    /* ── Transaction rows ── */
    .tx-row {{
        display: flex;
        align-items: center;
        padding: 12px;
        border-bottom: 1px solid {t['row_border']};
        font-size: 13px;
        color: {t['text']};
        font-family: {FONT};
    }}
    .tx-row:hover {{
        background: {t['row_hover']};
    }}
    .tx-header {{
        display: flex;
        align-items: center;
        padding: 10px 12px;
        border-bottom: 2px solid {t['border']};
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: {t['muted']};
        font-weight: 600;
        font-family: {FONT};
    }}
    .tx-actions .stButton > button {{
        padding: 6px 12px;
        font-size: 12px;
        min-height: 0;
        line-height: 1;
        border-radius: 3px;
    }}
    .tx-del .stButton > button {{
        border-color: #FEE2E2;
        color: #C0392B;
    }}
    .tx-del .stButton > button:hover {{
        background: #FEE2E2;
        border-color: #C0392B;
        color: #C0392B;
    }}

    /* ── Theme toggle button ── */
    .theme-toggle .stButton > button {{
        border-radius: 3px;
        padding: 6px 12px;
        font-size: 13px;
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
        border-radius: 3px;
    }}

    /* ── Inputs ── */
    .stSelectbox > div > div,
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stDateInput > div > div > input {{
        background: {t['surface']};
        border-color: {t['border_hover']};
        border-radius: 3px;
        color: {t['text']};
        font-family: {FONT};
        font-size: 14px;
    }}
    .stTextInput > div > div > input:focus,
    .stNumberInput > div > div > input:focus {{
        border-color: {t['text']} !important;
        box-shadow: none !important;
    }}

    /* ── Text area ── */
    .stTextArea textarea {{
        background: {t['surface']};
        border-color: {t['border_hover']};
        border-radius: 3px;
        color: {t['text']};
        font-family: {FONT};
        font-size: 14px;
    }}

    /* ── Expander ── */
    .streamlit-expanderHeader {{
        background: {t['surface']};
        border-radius: 3px;
        border: 1px solid {t['border']};
        color: {t['text']};
    }}

    /* ── Plotly ── */
    .js-plotly-plot .plotly .main-svg {{
        background: transparent !important;
    }}

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {{
        background: {t['sidebar_bg']} !important;
        min-width: 220px !important;
        max-width: 220px !important;
    }}
    [data-testid="stSidebar"] [data-testid="stSidebarContent"] {{
        background: {t['sidebar_bg']} !important;
        padding-top: 32px !important;
    }}
    section[data-testid="stSidebar"] .stButton > button {{
        background: transparent !important;
        border: none !important;
        color: {t['sidebar_text_dim']} !important;
        text-align: left !important;
        justify-content: flex-start !important;
        font-size: 13px !important;
        font-weight: 500 !important;
        padding: 10px 24px !important;
        border-radius: 0 !important;
        width: 100% !important;
        letter-spacing: 0.3px !important;
        text-transform: none !important;
        min-height: 0 !important;
        line-height: 1.4 !important;
    }}
    section[data-testid="stSidebar"] .stButton > button:hover {{
        background: {t['sidebar_hover']} !important;
        color: {t['sidebar_text']} !important;
    }}
    section[data-testid="stSidebar"] .stButton > button:focus {{
        box-shadow: none !important;
    }}
    /* Active nav button */
    section[data-testid="stSidebar"] .nav-active .stButton > button {{
        background: {t['sidebar_hover']} !important;
        color: {t['sidebar_text']} !important;
        font-weight: 500 !important;
    }}
    /* Nav item (inactive) - ensure left-alignment */
    section[data-testid="stSidebar"] .nav-item .stButton > button {{
        justify-content: flex-start !important;
    }}
    /* Sidebar action buttons (UPDATE/UPLOAD) */
    section[data-testid="stSidebar"] .sidebar-actions .stButton > button {{
        background: transparent !important;
        border: 1px solid {t['sidebar_border']} !important;
        color: {t['sidebar_text']} !important;
        font-size: 11px !important;
        font-weight: 600 !important;
        letter-spacing: 0.05em !important;
        text-transform: uppercase !important;
        padding: 8px 12px !important;
        border-radius: 3px !important;
    }}
    section[data-testid="stSidebar"] .sidebar-actions .stButton > button:hover {{
        background: {t['sidebar_hover']} !important;
        border-color: {t['sidebar_text_dim']} !important;
    }}
    /* Reduce gap between sidebar elements */
    [data-testid="stSidebar"] .stElementContainer {{
        margin-bottom: 0 !important;
    }}
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {{
        gap: 0 !important;
    }}
    [data-testid="stSidebar"] .sidebar-actions {{
        padding: 0 16px;
    }}

    /* ── Mobile Responsive (iPhone 16/17 Pro = 393px) ── */
    @media (max-width: 480px) {{
        .stMainBlockContainer {{
            padding: 0.5rem 0.75rem;
        }}
        .card {{
            padding: 14px 16px;
            border-radius: 3px;
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
            border-radius: 3px;
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
"""


def _inject_css():
    """Inject cached theme CSS. Only rebuilds the string when theme changes."""
    theme_name = st.session_state.get('theme', 'light')
    css_html = _build_css(theme_name)
    st.markdown(css_html, unsafe_allow_html=True)

_inject_css()


# ─── Data Loading ────────────────────────────────────────────────────────────

def _invalidate_data_caches():
    """Clear only the data-related caches after a mutation (save/delete/edit).
    Preserves long-lived caches like _drive_find_folder and get_exchange_rate."""
    load_data.clear()
    _auto_scan_changes.clear()
    try:
        _load_documents.clear()
    except Exception:
        pass
    try:
        _load_clients_cached.clear()
    except Exception:
        pass

@st.cache_data(ttl=300)
def load_data():
    """Load all data from Google Sheets with 5-min cache."""
    try:
        sh = _gsheet()
    except Exception as e:
        st.error(f"Cannot connect to Google Sheet: {type(e).__name__}: {e}")
        st.stop()

    data = {}

    # 1. EXPENSES — clean tabular structure
    # Retry once on APIError (stale cached auth token)
    try:
        ws_exp = sh.worksheet('Expenses')
    except gspread.exceptions.APIError:
        _gsheet.clear()
        _get_gspread_client.clear()
        sh = _gsheet()
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


def metric_card(label, value, sub=None, color_class='', prefix='\u20ac'):
    """Render a styled metric card."""
    if isinstance(value, float):
        val_str = f"{prefix}{value:,.2f}"
    elif isinstance(value, int):
        val_str = f"{prefix}{value:,}"
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


def gauge_card(label, value, max_val, fmt_value=None, sub=None, color=None):
    """Render a circular gauge KPI card (SVG-based)."""
    if color is None:
        color = _chart_primary()
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
                font-size="18" font-weight="700" font-family="Helvetica Neue, Helvetica, Arial, sans-serif">{display}</text>
            <text x="60" y="74" text-anchor="middle" fill="{t['muted']}"
                font-size="11" font-family="Helvetica Neue, Helvetica, Arial, sans-serif">{int(pct*100)}%</text>
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
    """Generate inline badge span for a category."""
    t = _t()
    is_dark = st.session_state.get('theme', 'light') == 'dark'
    if is_dark:
        colors = ('#A3A3A3', '#262626')
    else:
        colors = BADGE_STYLES.get(category or text, ('#525252', '#E5E5E5'))
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
        marker_color=_chart_primary(),
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

    # Pagination controls (using on_click callbacks to avoid double-rerun)
    if total_pages > 1:
        pc1, pc2, pc3 = st.columns([1, 2, 1])
        with pc1:
            if page > 0:
                st.button("← Prev", key='exp_prev',
                          on_click=lambda: st.session_state.update({'exp_page': page - 1}))
        with pc2:
            st.markdown(
                f'<div style="text-align:center;color:{_t()["text_secondary"]};font-size:0.8rem;padding-top:0.4rem">'
                f'Page {page + 1} of {total_pages} · {total_count} expenses</div>',
                unsafe_allow_html=True)
        with pc3:
            if page < total_pages - 1:
                st.button("Next →", key='exp_next',
                          on_click=lambda: st.session_state.update({'exp_page': page + 1}))


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
                marker=dict(colors=CHART_COLORS[:len(cat_totals)], line=dict(width=0)),
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
            colors_list = CHART_COLORS[:4]
            fig2 = go.Figure(go.Bar(
                y=cr['Client'], x=cr['_n'], orientation='h',
                marker_color=colors_list[:len(cr)] if len(cr) <= 4 else _chart_primary(),
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
                   sub=f'of \u20ac{goal:,.0f} goal', color=_chart_primary())
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
            line=dict(color=_chart_primary(), width=3),
            marker=dict(size=6, color=_chart_primary()),
            fill='tozeroy', fillcolor='rgba(17,17,17,0.06)' if st.session_state.get('theme', 'light') == 'light' else 'rgba(240,240,240,0.06)',
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
            marker_color=_chart_primary(),
            marker=dict(cornerradius=4),
        ))
        fig_proj.add_trace(go.Bar(
            name='Projected Income', x=[m[:3] for m in MONTHS],
            y=projected,
            marker_color=_chart_primary_dim(),
            marker=dict(cornerradius=4, line=dict(color=_chart_primary(), width=1)),
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
        <div class="card" style="border-left: 3px solid {_t()['text']};">
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
            <div class="card" style="border-left: 3px solid {_t()['text']};">
                <div class="card-label">Total Tax Burden (Projected)</div>
                <div class="card-value negative">{fmt_eur(proj_total_tax)}</div>
                <div class="card-sub">Income tax + VAT for full year</div>
            </div>
            """, unsafe_allow_html=True)

    # --- Tax rates info ---
    st.markdown(f"""
    <div style="margin-top:1.5rem;padding:0.8rem 1rem;background:{_t()["surface2"]};border-radius:3px;
                border:1px solid {_t()["border"]};font-size:0.78rem;color:{_t()["muted"]}">
        <strong style="color:{_t()["text"]}">Tax Rates Used:</strong>
        Income Tax: {TAX_RATE_INCOME:.0%} (combined Einkommensteuer + Soli) &middot;
        VAT: {VAT_RATE:.0%} (Umsatzsteuer) &middot;
        These are estimates only &mdash; consult your Steuerberater for exact figures.
        Rates can be adjusted in the dashboard configuration (TAX_RATE_INCOME, VAT_RATE).
    </div>
    """, unsafe_allow_html=True)




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
        marker_color=_chart_primary(),
        marker=dict(cornerradius=6),
        text=[fmt_eur(v) for v in vals],
        textposition='outside',
        textfont=dict(color=_chart_primary(), size=10, family=FONT),
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
            marker_color=_chart_primary_dim(),
            marker=dict(cornerradius=4),
        ))
        fig2.add_trace(go.Bar(
            name='2026', x=[m[:3] for m in MONTHS],
            y=[ov_dict.get(m, 0) for m in MONTHS],
            marker_color=_chart_primary(),
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


@st.cache_data(ttl=300)
def _auto_scan_changes():
    """Cached auto-scan for Drive invoice + expense changes on page load (5-min cache)."""
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
            st.session_state['active_page'] = 'Expenses'
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
                    st.session_state['active_page'] = 'Expenses'
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
            st.session_state['active_page'] = 'Expenses'
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
                    st.session_state['active_page'] = 'Expenses'
                    st.rerun()
                else:
                    st.error("Could not find the expense row in the spreadsheet.")


def _check_documents_drive_integrity():
    """Read-only integrity check: compare Documents sheet against actual Drive files.
    Returns a list of discrepancy dicts: {type, message, folder, severity}."""
    discrepancies = []

    # Load documents from sheet
    try:
        docs = _load_documents()
    except Exception as e:
        return [{'type': 'error', 'message': f'Could not load Documents sheet: {e}',
                 'folder': '', 'severity': 'error'}]

    # Get year folder
    year_folder = _get_year_folder()
    if not year_folder:
        return [{'type': 'error', 'message': 'Could not find year folder on Drive.',
                 'folder': '', 'severity': 'error'}]

    # List files in both Drive folders
    drive_files_invoices = {}  # {filename: file_id}
    drive_files_offers = {}

    inv_folder_id = _drive_find_folder(year_folder, INVOICES_FOLDER)
    off_folder_id = _drive_find_folder(year_folder, OFFERS_FOLDER)

    if inv_folder_id:
        for f in _drive_list_files(inv_folder_id):
            drive_files_invoices[f['name']] = f['id']

    if off_folder_id:
        for f in _drive_list_files(off_folder_id):
            drive_files_offers[f['name']] = f['id']

    # Build lookup of DB records that have Drive references
    db_files_invoices = {}  # {filename: doc_record}
    db_files_offers = {}
    db_by_file_id = {}  # {drive_file_id: doc_record}

    for doc in docs:
        drive_fn = doc.get('Drive_Filename', '').strip()
        drive_id = doc.get('Drive_File_ID', '').strip()
        doc_type = str(doc.get('Type', '')).lower()
        doc_num = doc.get('Number', '')

        if not drive_fn and not drive_id:
            # Draft without a generated PDF — skip, not expected on Drive
            continue

        is_invoice = doc_type in ('rechnung', 'invoice')
        is_offer = doc_type in ('angebot', 'offer')

        if is_invoice:
            db_files_invoices[drive_fn] = doc
        elif is_offer:
            db_files_offers[drive_fn] = doc

        if drive_id:
            db_by_file_id[drive_id] = doc

    # ── Check 1: Files in DB but missing from Drive ──
    for fn, doc in db_files_invoices.items():
        drive_id = doc.get('Drive_File_ID', '').strip()
        if fn and fn not in drive_files_invoices:
            # Also check by file ID in case renamed
            found_by_id = False
            if drive_id:
                for d_name, d_id in drive_files_invoices.items():
                    if d_id == drive_id and d_name != fn:
                        discrepancies.append({
                            'type': 'renamed',
                            'message': f'File renamed in {INVOICES_FOLDER}: expected "{fn}", found "{d_name}".',
                            'folder': INVOICES_FOLDER,
                            'severity': 'warning',
                        })
                        found_by_id = True
                        break
            if not found_by_id:
                discrepancies.append({
                    'type': 'missing_from_drive',
                    'message': f'File missing from {INVOICES_FOLDER}: {fn}',
                    'folder': INVOICES_FOLDER,
                    'severity': 'warning',
                })

    for fn, doc in db_files_offers.items():
        drive_id = doc.get('Drive_File_ID', '').strip()
        if fn and fn not in drive_files_offers:
            found_by_id = False
            if drive_id:
                for d_name, d_id in drive_files_offers.items():
                    if d_id == drive_id and d_name != fn:
                        discrepancies.append({
                            'type': 'renamed',
                            'message': f'File renamed in {OFFERS_FOLDER}: expected "{fn}", found "{d_name}".',
                            'folder': OFFERS_FOLDER,
                            'severity': 'warning',
                        })
                        found_by_id = True
                        break
            if not found_by_id:
                discrepancies.append({
                    'type': 'missing_from_drive',
                    'message': f'File missing from {OFFERS_FOLDER}: {fn}',
                    'folder': OFFERS_FOLDER,
                    'severity': 'warning',
                })

    # ── Check 2: Files on Drive but not in DB (orphaned / added externally) ──
    db_invoice_names = set(db_files_invoices.keys())
    db_offer_names = set(db_files_offers.keys())
    # Also gather all known Drive file IDs from DB
    db_drive_ids = set(db_by_file_id.keys())

    for d_name, d_id in drive_files_invoices.items():
        if d_name not in db_invoice_names and d_id not in db_drive_ids:
            discrepancies.append({
                'type': 'not_in_db',
                'message': f'File on Drive not in database ({INVOICES_FOLDER}): {d_name}',
                'folder': INVOICES_FOLDER,
                'severity': 'info',
            })

    for d_name, d_id in drive_files_offers.items():
        if d_name not in db_offer_names and d_id not in db_drive_ids:
            discrepancies.append({
                'type': 'not_in_db',
                'message': f'File on Drive not in database ({OFFERS_FOLDER}): {d_name}',
                'folder': OFFERS_FOLDER,
                'severity': 'info',
            })

    # ── Check 3: Status / prefix mismatch ──
    # For invoices: if DB status is 'paid' the file should NOT have notpaid_ prefix
    # If DB status is not 'paid', the file SHOULD have notpaid_ prefix
    for fn, doc in db_files_invoices.items():
        if fn not in drive_files_invoices:
            continue  # Already reported as missing
        status = str(doc.get('Status', '')).lower()
        has_notpaid = fn.startswith('notpaid_')
        if status == 'paid' and has_notpaid:
            discrepancies.append({
                'type': 'status_mismatch',
                'message': (f'Status mismatch in {INVOICES_FOLDER}: '
                            f'database says "{doc.get("Number", "")}" is paid, '
                            f'but file still has notpaid_ prefix: {fn}'),
                'folder': INVOICES_FOLDER,
                'severity': 'warning',
            })
        elif status != 'paid' and status != 'draft' and not has_notpaid:
            discrepancies.append({
                'type': 'status_mismatch',
                'message': (f'Status mismatch in {INVOICES_FOLDER}: '
                            f'database says "{doc.get("Number", "")}" is {status}, '
                            f'but file is missing notpaid_ prefix: {fn}'),
                'folder': INVOICES_FOLDER,
                'severity': 'warning',
            })

    return discrepancies


@st.dialog("Update", width="large")
def sync_invoices_dialog():
    """Scan invoices and cost folders for changes and sync with spreadsheet."""
    with st.spinner("Scanning Google Drive for changes..."):
        changes = scan_invoice_changes()
        expense_changes = scan_expense_changes()
        doc_discrepancies = _check_documents_drive_integrity()

    # ── Documents integrity check (read-only) ──
    t = _t()
    _card_base = (f'background:{t["surface"]};border:1px solid {t["border"]};'
                  f'border-radius:3px;padding:1rem;margin-bottom:0.5rem;box-shadow:none')

    if not doc_discrepancies:
        st.markdown(
            f'<div style="{_card_base};border-left:3px solid {C_GREEN}">'
            f'<div style="color:{t["text"]};font-size:0.85rem;font-weight:600">'
            f'Everything is OK \u2014 all files correspond to Google Drive.</div>'
            f'<div style="font-size:0.75rem;color:{t["muted"]};margin-top:4px">'
            f'Checked {INVOICES_FOLDER} and {OFFERS_FOLDER} against the Documents database.</div>'
            f'</div>', unsafe_allow_html=True)
    else:
        error_items = [d for d in doc_discrepancies if d['severity'] == 'error']
        warn_items = [d for d in doc_discrepancies if d['severity'] == 'warning']
        info_items = [d for d in doc_discrepancies if d['severity'] == 'info']

        st.markdown(
            f'<div style="{_card_base};border-left:3px solid {C_RED}">'
            f'<div style="color:{t["text"]};font-size:0.85rem;font-weight:600">'
            f'Warning \u2014 {len(doc_discrepancies)} change(s) detected between Documents database and Google Drive:</div>'
            f'</div>', unsafe_allow_html=True)

        for d in error_items:
            st.markdown(
                f'<div style="{_card_base};border-left:3px solid {C_RED};margin-left:12px">'
                f'<div style="color:{C_RED};font-size:0.82rem">{d["message"]}</div>'
                f'</div>', unsafe_allow_html=True)

        for d in warn_items:
            st.markdown(
                f'<div style="{_card_base};border-left:3px solid {C_AMBER};margin-left:12px">'
                f'<div style="color:{t["text"]};font-size:0.82rem">{d["message"]}</div>'
                f'</div>', unsafe_allow_html=True)

        for d in info_items:
            st.markdown(
                f'<div style="{_card_base};border-left:3px solid {C_BLUE};margin-left:12px">'
                f'<div style="color:{t["text"]};font-size:0.82rem">{d["message"]}</div>'
                f'</div>', unsafe_allow_html=True)

        st.markdown(
            f'<div style="font-size:0.75rem;color:{t["muted"]};margin:4px 0 12px 12px;font-style:italic">'
            f'This is a read-only check. No files or records have been modified.</div>',
            unsafe_allow_html=True)

    st.markdown(f'<hr style="border:none;border-top:1px solid {t["border"]};margin:16px 0">', unsafe_allow_html=True)

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
             f'border-radius:3px;padding:1rem;margin-bottom:0.5rem;'
             f'box-shadow:none')

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


# ─── Offer / Invoice / Client System ─────────────────────────────────────────

# ── Helpers: European number formatting ──────────────────────────────────────

def _fmt_eur(val):
    """Format a number in European style: 1.234,56 EUR"""
    if val is None or val == '':
        return '0,00 EUR'
    try:
        n = float(val)
    except (ValueError, TypeError):
        return '0,00 EUR'
    sign = '-' if n < 0 else ''
    n = abs(n)
    integer_part = int(n)
    decimal_part = round((n - integer_part) * 100)
    if decimal_part >= 100:
        integer_part += 1
        decimal_part = 0
    int_str = f'{integer_part:,}'.replace(',', '.')
    return f'{sign}{int_str},{decimal_part:02d} EUR'


def _fmt_eur_symbol(val):
    """Format a number in European style with € symbol: 1.234,56 €"""
    s = _fmt_eur(val)
    return s.replace(' EUR', ' €')


def _parse_eur_input(s):
    """Parse a European-formatted price string into a float.
    Handles: '700,00' '1.234,56' '700' '700.00' etc."""
    if s is None or str(s).strip() == '':
        return 0.0
    s = str(s).strip().replace('€', '').replace('EUR', '').strip()
    # European format: period as thousands sep, comma as decimal
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


# ── Helpers: Client data loading ─────────────────────────────────────────────

def _load_clients_from_sheet():
    """Load clients from the 'Clients' worksheet. Creates it if missing."""
    try:
        sh = _gsheet()
        try:
            ws = sh.worksheet('Clients')
        except gspread.WorksheetNotFound:
            # Create the Clients worksheet matching existing schema
            ws = sh.add_worksheet(title='Clients', rows=100, cols=6)
            headers = ['ID', 'Name', 'Address', 'Notes', 'Country', 'Added']
            ws.append_row(headers, value_input_option='RAW')
            for c in SEED_CLIENTS:
                ws.append_row([
                    c['id'], c['name'], c.get('address', ''),
                    c.get('notes', ''), c.get('country', ''),
                    datetime.now().strftime('%Y-%m-%d'),
                ], value_input_option='RAW')
            _load_clients_cached.clear()
            return _load_clients_cached()

        rows = ws.get_all_records()
        clients = []
        for r in rows:
            # Support both 'ID' and 'Client_ID' header variants
            cid = str(r.get('ID', '') or r.get('Client_ID', ''))
            clients.append({
                'id': cid,
                'name': str(r.get('Name', '')),
                'address': str(r.get('Address', '')),
                'notes': str(r.get('Notes', '')),
                'country': str(r.get('Country', '')),
            })
        return clients
    except Exception as e:
        st.warning(f"Could not load clients: {e}")
        return list(SEED_CLIENTS)


@st.cache_data(ttl=300)
def _load_clients_cached():
    """Cached client list loader (5-min cache)."""
    return _load_clients_from_sheet()


def _get_next_client_number():
    """Get the next available K-number by scanning existing clients."""
    clients = _load_clients_cached()
    max_num = 10000
    for c in clients:
        cid = str(c.get('id', ''))
        if cid.startswith('K'):
            try:
                num = int(cid[1:])
                if num > max_num:
                    max_num = num
            except ValueError:
                pass
    return f'K{max_num + 1:05d}'


def _add_client_to_sheet(client_data):
    """Add a new client to the Clients sheet.
    Sheet columns: ID, Name, Address, Notes, Country, Added"""
    try:
        sh = _gsheet()
        ws = sh.worksheet('Clients')
        ws.append_row([
            client_data['id'],
            client_data['name'],
            client_data.get('address', ''),
            client_data.get('notes', ''),
            client_data.get('country', ''),
            datetime.now().strftime('%Y-%m-%d'),
        ], value_input_option='RAW')
        _load_clients_cached.clear()
        return True
    except Exception as e:
        st.error(f"Failed to save client: {e}")
        return False


# ── Helpers: Document numbering ──────────────────────────────────────────────

def _get_next_doc_number(doc_type):
    """Get the next available document number by scanning the Drive folder.
    doc_type: 'offer' or 'invoice'"""
    try:
        year_folder = _get_year_folder()
        if not year_folder:
            # Fallback to simple counter
            return f'{"AG" if doc_type == "offer" else "RE"}{CURRENT_YEAR}001'

        if doc_type == 'offer':
            folder_name = OFFERS_FOLDER
            prefix = 'AG'
            pattern = r'AG(\d+)'
        else:
            folder_name = INVOICES_FOLDER
            prefix = 'RE'
            pattern = r'RE(\d+)'

        folder_id = _drive_find_folder(year_folder, folder_name)
        if not folder_id:
            return f'{prefix}{CURRENT_YEAR}001'

        files = _drive_list_files(folder_id)
        max_num = 0
        for f in files:
            match = re.search(pattern, f['name'])
            if match:
                try:
                    num = int(match.group(1))
                    if num > max_num:
                        max_num = num
                except ValueError:
                    pass

        if max_num == 0:
            # No existing files — start at YEAR + 001
            return f'{prefix}{CURRENT_YEAR}001'
        else:
            return f'{prefix}{max_num + 1:0{len(str(max_num))}d}'
    except Exception:
        return f'{"AG" if doc_type == "offer" else "RE"}{CURRENT_YEAR}001'


# ── Helpers: Documents worksheet ─────────────────────────────────────────────

def _get_or_create_documents_sheet():
    """Get or create the Documents worksheet for storing offers/invoices."""
    sh = _gsheet()
    try:
        ws = sh.worksheet('Documents')
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title='Documents', rows=500, cols=25)
        headers = [
            'ID', 'Type', 'Number', 'Date', 'Client_ID', 'Client_Name',
            'Client_Address', 'Project_Title', 'Project_Description',
            'Category', 'Status', 'Line_Items_JSON', 'Netto', 'USt',
            'Brutto', 'Validity_Days', 'Service_Date', 'Payment_Terms',
            'Drive_File_ID', 'Drive_Filename', 'Source_Offer_Number',
            'Created_At', 'Updated_At'
        ]
        ws.append_row(headers, value_input_option='RAW')
    return ws


@st.cache_data(ttl=300)
def _load_documents():
    """Load all documents from the Documents sheet (5-min cache)."""
    try:
        ws = _get_or_create_documents_sheet()
        rows = ws.get_all_records()
        return rows
    except Exception as e:
        st.warning(f"Could not load documents: {e}")
        return []


def _invalidate_documents_cache():
    """Clear the documents cache."""
    _load_documents.clear()


def _import_existing_invoices_to_documents():
    """One-time import: sync existing invoices from Income sheet + Google Drive
    into the Documents sheet so they appear in the Offers & Invoices view.
    Only imports invoices not already present in Documents.
    Returns number of imported records."""
    try:
        # Load what's already in the Documents sheet
        existing_docs = _load_documents()
        existing_numbers = {str(d.get('Number', '')) for d in existing_docs}

        # Read Income sheet
        sh = _gsheet()
        ws_inc = sh.worksheet('Income')
        inc_vals = ws_inc.get_all_values()
        if not inc_vals:
            return 0

        inc_headers = inc_vals[0]
        inc_df = pd.DataFrame(inc_vals[1:], columns=inc_headers)
        if 'Date' in inc_df.columns:
            inc_df['Date'] = pd.to_datetime(inc_df['Date'], dayfirst=True, errors='coerce')

        paid_df = _parse_income_section(inc_df, 0)
        unpaid_df = _parse_income_section(inc_df, 1)

        # Load clients for ID lookup (name → client_id, address)
        clients = _load_clients_from_sheet()
        client_lookup = {}
        for c in clients:
            client_lookup[c['name'].strip().lower()] = c

        # List Drive files in INVOICES folder for file ID matching
        year_folder = _get_year_folder()
        inv_folder_id = _drive_find_folder(year_folder, INVOICES_FOLDER) if year_folder else None
        drive_files = {}
        if inv_folder_id:
            for f in _drive_list_files(inv_folder_id):
                drive_files[f['name']] = f['id']

        # Prepare Documents worksheet — collect all rows, then batch-append
        ws_docs = _get_or_create_documents_sheet()
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        rows_to_add = []

        for status_label, section_df in [('paid', paid_df), ('unpaid', unpaid_df)]:
            if len(section_df) == 0 or 'Invoice Number' not in section_df.columns:
                continue
            for _, row in section_df.iterrows():
                inv_num_raw = row.get('Invoice Number')
                if pd.isna(inv_num_raw):
                    continue
                inv_num = str(int(inv_num_raw)) if isinstance(inv_num_raw, (int, float)) else str(inv_num_raw).strip()
                if not inv_num or inv_num in existing_numbers:
                    continue

                # Build RE-prefixed doc number
                doc_number = f'RE{inv_num}' if not inv_num.startswith('RE') else inv_num

                # Already imported?
                if doc_number in existing_numbers:
                    continue

                client_name = str(row.get('Client', ''))
                project = str(row.get('Project', ''))
                category = str(row.get('Category', ''))
                date_val = row.get('Date')
                date_str = date_val.strftime('%d.%m.%Y') if pd.notna(date_val) else ''
                month_str = str(row.get('Month', ''))

                netto = pd.to_numeric(_clean_currency(row.get('Netto (€)', 0)), errors='coerce')
                brutto = pd.to_numeric(_clean_currency(row.get('Brutto (€)', 0)), errors='coerce')
                if pd.isna(netto):
                    netto = 0.0
                if pd.isna(brutto):
                    brutto = 0.0
                ust = brutto - netto

                # Client lookup
                client_info = client_lookup.get(client_name.strip().lower(), {})
                client_id = client_info.get('id', '')
                client_address = client_info.get('address', '')

                # Map status
                doc_status = 'paid' if status_label == 'paid' else 'pending'

                # Find Drive file
                drive_file_id = ''
                drive_filename = ''
                for fname, fid in drive_files.items():
                    _, normalized = _extract_invoice_id_from_filename(fname)
                    if normalized == inv_num or fname.endswith(f'_{doc_number}.pdf') or fname.endswith(f'_{doc_number}.pdf'.replace('RE', '')):
                        drive_file_id = fid
                        drive_filename = fname
                        break

                doc_id = f"Rechnung_{doc_number}_{now_str.replace(' ', '_')}_{len(rows_to_add)}"

                rows_to_add.append([
                    doc_id,                   # ID
                    'Rechnung',               # Type
                    doc_number,               # Number
                    date_str,                 # Date
                    client_id,                # Client_ID
                    client_name,              # Client_Name
                    client_address,           # Client_Address
                    project,                  # Project_Title
                    '',                       # Project_Description
                    category,                 # Category
                    doc_status,               # Status
                    '[]',                     # Line_Items_JSON (not available)
                    f'{netto:.2f}',           # Netto
                    f'{ust:.2f}',             # USt
                    f'{brutto:.2f}',          # Brutto
                    '',                       # Validity_Days
                    month_str,                # Service_Date
                    '',                       # Payment_Terms
                    drive_file_id,            # Drive_File_ID
                    drive_filename,           # Drive_Filename
                    '',                       # Source_Offer_Number
                    now_str,                  # Created_At
                    now_str,                  # Updated_At
                ])
                existing_numbers.add(doc_number)
                existing_numbers.add(inv_num)

        # Single batch write instead of N individual append_row calls
        if rows_to_add:
            ws_docs.append_rows(rows_to_add, value_input_option='RAW')
            _invalidate_documents_cache()
        return len(rows_to_add)
    except Exception as e:
        st.warning(f"Could not import existing invoices: {e}")
        return 0


def _save_document_to_sheet(form_data, status='draft', doc_number=None):
    """Save a document (offer/invoice) to the Documents sheet.
    Returns the document number on success, None on failure."""
    try:
        ws = _get_or_create_documents_sheet()

        # Generate document number if not provided
        if not doc_number:
            doc_type = 'offer' if form_data['mode'] == 'offer' else 'invoice'
            doc_number = _get_next_doc_number(doc_type)

        # Generate a unique ID
        doc_id = f"{form_data['type']}_{doc_number}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Serialize line items to JSON
        line_items_json = json.dumps(form_data.get('line_items', []), ensure_ascii=False)

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        row = [
            doc_id,                                          # ID
            form_data.get('type', ''),                       # Type (Angebot/Rechnung)
            doc_number,                                      # Number
            form_data.get('date', ''),                       # Date
            form_data.get('client_id', ''),                  # Client_ID
            form_data.get('client_name', ''),                # Client_Name
            form_data.get('client_address', ''),             # Client_Address
            form_data.get('project_title', ''),              # Project_Title
            form_data.get('project_description', ''),        # Project_Description
            form_data.get('category', ''),                   # Category
            status,                                          # Status
            line_items_json,                                 # Line_Items_JSON
            f"{form_data.get('netto', 0):.2f}",             # Netto
            f"{form_data.get('ust', 0):.2f}",               # USt
            f"{form_data.get('brutto', 0):.2f}",            # Brutto
            str(form_data.get('validity_days', '')),         # Validity_Days
            form_data.get('service_date', ''),               # Service_Date
            form_data.get('payment_terms', ''),              # Payment_Terms
            '',                                              # Drive_File_ID
            '',                                              # Drive_Filename
            form_data.get('source_offer_number', ''),        # Source_Offer_Number
            now_str,                                         # Created_At
            now_str,                                         # Updated_At
        ]

        ws.append_row(row, value_input_option='RAW')
        _invalidate_documents_cache()
        _log_activity('DRAFT_SAVED', f'{form_data["type"]} {doc_number} saved as draft')
        return doc_number

    except Exception as e:
        st.error(f"Failed to save document: {e}")
        return None


def _update_document_in_sheet(doc_number, updates):
    """Update an existing document in the Documents sheet.
    updates: dict of column_name -> new_value. Uses batch_update for speed."""
    try:
        ws = _get_or_create_documents_sheet()
        all_records = ws.get_all_records()
        headers = ws.row_values(1)

        for idx, record in enumerate(all_records):
            if str(record.get('Number', '')) == str(doc_number):
                row_num = idx + 2  # +1 for header, +1 for 1-based indexing
                # Add Updated_At timestamp
                updates_with_time = dict(updates)
                updates_with_time['Updated_At'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Build batch of cell updates
                cells_to_update = []
                for col_name, new_val in updates_with_time.items():
                    if col_name in headers:
                        col_idx = headers.index(col_name) + 1
                        cells_to_update.append(gspread.Cell(row_num, col_idx, str(new_val)))
                if cells_to_update:
                    ws.update_cells(cells_to_update)
                _invalidate_documents_cache()
                return True

        return False
    except Exception as e:
        st.error(f"Failed to update document: {e}")
        return False


# ── PDF Generation ───────────────────────────────────────────────────────────

def _fmt_eur_pdf(val):
    """Format a number in European style for PDF: 1.234,56"""
    if val is None:
        return '0,00'
    try:
        n = float(val)
    except (ValueError, TypeError):
        return '0,00'
    sign = '-' if n < 0 else ''
    n = abs(n)
    integer_part = int(n)
    decimal_part = round((n - integer_part) * 100)
    if decimal_part >= 100:
        integer_part += 1
        decimal_part = 0
    int_str = f'{integer_part:,}'.replace(',', '.')
    return f'{sign}{int_str},{decimal_part:02d}'


def _sanitize_for_pdf(text):
    """Replace Unicode characters unsupported by Helvetica with ASCII equivalents."""
    if not isinstance(text, str):
        text = str(text)
    replacements = {
        '\u2013': '-',   # en-dash →  hyphen
        '\u2014': '-',   # em-dash →  hyphen
        '\u2018': "'",   # left single quote
        '\u2019': "'",   # right single quote
        '\u201C': '"',   # left double quote
        '\u201D': '"',   # right double quote
        '\u2026': '...', # ellipsis
        '\u20ac': 'EUR', # euro sign
        '\u00b4': "'",   # acute accent
        '\u2022': '-',   # bullet
        '\u00ad': '-',   # soft hyphen
        '\u200b': '',    # zero-width space
        '\u00a0': ' ',   # non-breaking space
    }
    for char, repl in replacements.items():
        text = text.replace(char, repl)
    # Fallback: strip any remaining non-latin1 characters
    return text.encode('latin-1', errors='replace').decode('latin-1')


def _generate_document_pdf(form_data, doc_number):
    """Generate a PDF document matching the reference invoice layout.
    Returns PDF bytes on success, None on failure."""
    biz = BIZ_INFO
    is_invoice = form_data['mode'] == 'invoice'
    doc_type_label = 'INVOICE' if is_invoice else 'ESTIMATE'

    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=25)
    pdf.add_page()

    # Page dimensions
    pw = 210  # A4 width
    ml = 25   # left margin
    mr = 20   # right margin
    usable = pw - ml - mr

    # ── Green accent line at very top ──
    pdf.set_fill_color(11, 71, 20)  # #0B4714
    pdf.rect(0, 0, pw, 3, 'F')

    # ── Header: Josef Sindelka (top right) ──
    pdf.set_y(15)
    pdf.set_font('Helvetica', 'B', 24)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(usable, 10, biz['name'], align='R', new_x='LMARGIN', new_y='NEXT')
    pdf.set_font('Helvetica', '', 9)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(usable, 4, biz['email'], align='R', new_x='LMARGIN', new_y='NEXT')
    pdf.cell(usable, 4, biz['website'], align='R', new_x='LMARGIN', new_y='NEXT')

    # ── Sender line (small, above client) ──
    pdf.set_y(45)
    pdf.set_font('Helvetica', '', 7)
    pdf.set_text_color(100, 100, 100)
    sender_line = f"{biz['name']}  -  {biz['street']}  -  {biz['city']}"
    pdf.cell(usable / 2, 4, _sanitize_for_pdf(sender_line), new_x='LMARGIN', new_y='NEXT')

    # ── Client block (left side) ──
    pdf.set_y(52)
    pdf.set_font('Helvetica', 'B', 11)
    pdf.set_text_color(0, 0, 0)
    client_name = _sanitize_for_pdf(form_data.get('client_name', ''))
    pdf.cell(usable / 2, 6, client_name, new_x='LMARGIN', new_y='NEXT')
    pdf.set_font('Helvetica', '', 10)
    client_addr = _sanitize_for_pdf(form_data.get('client_address', ''))
    for line in client_addr.split(','):
        line = line.strip()
        if line:
            pdf.cell(usable / 2, 5, line, new_x='LMARGIN', new_y='NEXT')

    # ── Document metadata (right side, aligned with client block) ──
    meta_x = ml + usable / 2 + 10
    meta_w_label = 35
    meta_w_value = usable / 2 - 10 - meta_w_label
    meta_y = 52

    pdf.set_font('Helvetica', '', 10)
    pdf.set_text_color(80, 80, 80)

    meta_items = []
    if is_invoice:
        meta_items.append(('Invoice No.', doc_number))
    else:
        meta_items.append(('Estimate No.', doc_number))
    meta_items.append(('Client No.', form_data.get('client_id', '')))
    meta_items.append(('Date', form_data.get('date', '')))

    if is_invoice:
        sd = form_data.get('service_date', '')
        if sd:
            meta_items.append(('Delivery', sd))
    else:
        validity = form_data.get('validity_days', 30)
        date_obj = form_data.get('date_obj')
        if date_obj:
            valid_until = date_obj + timedelta(days=int(validity))
            meta_items.append(('Valid until', valid_until.strftime('%d.%m.%Y')))

    for label, value in meta_items:
        pdf.set_xy(meta_x, meta_y)
        pdf.set_font('Helvetica', '', 10)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(meta_w_label, 6, label, new_x='END')
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(meta_w_value, 6, _sanitize_for_pdf(str(value)), align='R')
        meta_y += 7

    # ── Document type heading ──
    heading_y = max(pdf.get_y(), meta_y) + 15
    pdf.set_y(heading_y)
    pdf.set_font('Helvetica', 'B', 20)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(usable, 10, doc_type_label, new_x='LMARGIN', new_y='NEXT')

    # ── Project subtitle ──
    project_title = form_data.get('project_title', '')
    if project_title or client_name:
        pdf.set_font('Helvetica', 'B', 11)
        pdf.set_text_color(0, 0, 0)
        subtitle = _sanitize_for_pdf(project_title if project_title else client_name)
        pdf.cell(usable, 7, subtitle, new_x='LMARGIN', new_y='NEXT')
        pdf.ln(2)

    # ── Project description ──
    desc = form_data.get('project_description', '')
    if desc:
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(80, 80, 80)
        pdf.multi_cell(usable, 4, _sanitize_for_pdf(desc), new_x='LMARGIN', new_y='NEXT')
        pdf.ln(4)

    # ── Line items table ──
    line_items = form_data.get('line_items', [])
    col_widths = [12, usable * 0.40, 18, 25, 30, 30]  # Pos, Desc, Qty, Unit, Price, Total
    # Adjust description width to fill remaining space
    col_widths[1] = usable - col_widths[0] - col_widths[2] - col_widths[3] - col_widths[4] - col_widths[5]

    # Table header
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_text_color(80, 80, 80)
    pdf.set_fill_color(245, 245, 245)
    headers = ['Pos.', 'Description', 'Qty', 'Unit', 'Unit Price', 'Total EUR']
    aligns = ['L', 'L', 'R', 'L', 'R', 'R']
    for i, (hdr, w, a) in enumerate(zip(headers, col_widths, aligns)):
        pdf.cell(w, 8, hdr, align=a, fill=True,
                 new_x='END' if i < len(headers) - 1 else 'LMARGIN',
                 new_y='LAST' if i < len(headers) - 1 else 'NEXT')

    # Header bottom border
    pdf.set_draw_color(200, 200, 200)
    pdf.line(ml, pdf.get_y(), pw - mr, pdf.get_y())

    # Table rows
    pdf.set_font('Helvetica', '', 10)
    pdf.set_text_color(0, 0, 0)
    for idx, item in enumerate(line_items):
        desc_text = _sanitize_for_pdf(item.get('desc', ''))
        qty_val = _parse_eur_input(item.get('qty', '0'))
        price_val = _parse_eur_input(item.get('price', '0'))
        unit_text = _sanitize_for_pdf(item.get('unit', 'pcs'))
        line_total = qty_val * price_val

        row_data = [
            str(idx + 1),
            desc_text,
            _fmt_eur_pdf(qty_val) if qty_val != int(qty_val) else str(int(qty_val)) if qty_val == int(qty_val) and qty_val < 1000 else _fmt_eur_pdf(qty_val),
            unit_text,
            _fmt_eur_pdf(price_val),
            _fmt_eur_pdf(line_total),
        ]

        # Format qty nicely
        if qty_val == int(qty_val):
            row_data[2] = f'{int(qty_val):,}'.replace(',', '.')
        else:
            row_data[2] = _fmt_eur_pdf(qty_val)

        y_before = pdf.get_y()
        for i, (val, w, a) in enumerate(zip(row_data, col_widths, aligns)):
            pdf.cell(w, 8, val, align=a,
                     new_x='END' if i < len(row_data) - 1 else 'LMARGIN',
                     new_y='LAST' if i < len(row_data) - 1 else 'NEXT')

        # Row bottom border
        pdf.set_draw_color(235, 235, 235)
        pdf.line(ml, pdf.get_y(), pw - mr, pdf.get_y())

    # ── Totals ──
    pdf.ln(6)
    totals_x = pw - mr - 80
    netto = form_data.get('netto', 0)
    ust = form_data.get('ust', 0)
    brutto = form_data.get('brutto', 0)

    # Subtotal
    pdf.set_xy(totals_x, pdf.get_y())
    pdf.set_font('Helvetica', '', 10)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(40, 7, 'Subtotal (Netto)', align='R', new_x='END')
    pdf.set_text_color(0, 0, 0)
    pdf.cell(40, 7, f'{_fmt_eur_pdf(netto)} EUR', align='R', new_x='LMARGIN', new_y='NEXT')

    # USt
    pdf.set_xy(totals_x, pdf.get_y())
    pdf.set_text_color(80, 80, 80)
    pdf.cell(40, 7, 'USt. 19%', align='R', new_x='END')
    pdf.set_text_color(0, 0, 0)
    pdf.cell(40, 7, f'{_fmt_eur_pdf(ust)} EUR', align='R', new_x='LMARGIN', new_y='NEXT')

    # Divider
    pdf.set_draw_color(0, 0, 0)
    pdf.line(totals_x, pdf.get_y() + 1, pw - mr, pdf.get_y() + 1)
    pdf.ln(3)

    # Grand total
    pdf.set_xy(totals_x, pdf.get_y())
    pdf.set_font('Helvetica', 'B', 12)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(40, 8, 'Total (Brutto)', align='R', new_x='END')
    pdf.cell(40, 8, f'{_fmt_eur_pdf(brutto)} EUR', align='R', new_x='LMARGIN', new_y='NEXT')

    # ── Payment terms (invoice only) ──
    if is_invoice:
        pt = form_data.get('payment_terms', 'Zahlbar sofort, rein netto')
        if pt:
            pdf.ln(10)
            pdf.set_font('Helvetica', '', 9)
            pdf.set_text_color(80, 80, 80)
            pdf.cell(usable, 5, _sanitize_for_pdf(f'Payment terms: {pt}'), new_x='LMARGIN', new_y='NEXT')

    # ── Offer validity (offer only) ──
    if not is_invoice:
        validity = form_data.get('validity_days', 30)
        date_obj = form_data.get('date_obj')
        pdf.ln(10)
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(80, 80, 80)
        if date_obj:
            valid_until = date_obj + timedelta(days=int(validity))
            pdf.cell(usable, 5, f'This offer is valid until {valid_until.strftime("%d.%m.%Y")} ({validity} days).', new_x='LMARGIN', new_y='NEXT')
        else:
            pdf.cell(usable, 5, f'This offer is valid for {validity} days from the date above.', new_x='LMARGIN', new_y='NEXT')
        pdf.cell(usable, 5, 'Payment terms: Zahlbar sofort, rein netto.', new_x='LMARGIN', new_y='NEXT')

    # ── Footer ──
    # Position footer at bottom of page
    footer_y = 260
    if pdf.get_y() > footer_y - 20:
        pdf.add_page()
        footer_y = 260

    pdf.set_y(footer_y)

    # Footer divider
    pdf.set_draw_color(200, 200, 200)
    pdf.line(ml, footer_y, pw - mr, footer_y)
    pdf.ln(4)

    # 3-column footer
    col_w = usable / 3
    footer_y = pdf.get_y()

    # Column 1: Business details
    pdf.set_xy(ml, footer_y)
    pdf.set_font('Helvetica', 'B', 7)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(col_w, 3.5, biz['name'], new_x='LMARGIN', new_y='NEXT')
    pdf.set_font('Helvetica', '', 7)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(col_w, 3.5, biz['street'], new_x='LMARGIN', new_y='NEXT')
    pdf.cell(col_w, 3.5, biz['city'], new_x='LMARGIN', new_y='NEXT')

    # Column 2: Tax information
    pdf.set_xy(ml + col_w, footer_y)
    pdf.set_font('Helvetica', 'B', 7)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(col_w, 3.5, 'Tax Information')
    pdf.set_xy(ml + col_w, footer_y + 3.5)
    pdf.set_font('Helvetica', '', 7)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(col_w, 3.5, f'USt-IdNr.: {biz["ust_id"]}')
    pdf.set_xy(ml + col_w, footer_y + 7)
    pdf.cell(col_w, 3.5, f'Steuernr.: {biz["steuernummer"]}')

    # Column 3: Bank details (always on invoices, optionally on offers)
    if is_invoice:
        pdf.set_xy(ml + col_w * 2, footer_y)
        pdf.set_font('Helvetica', 'B', 7)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(col_w, 3.5, 'Bank Details')
        pdf.set_xy(ml + col_w * 2, footer_y + 3.5)
        pdf.set_font('Helvetica', '', 7)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(col_w, 3.5, f'Bank: {biz["bank"]}')
        pdf.set_xy(ml + col_w * 2, footer_y + 7)
        pdf.cell(col_w, 3.5, f'IBAN: {biz["iban"]}')
        pdf.set_xy(ml + col_w * 2, footer_y + 10.5)
        pdf.cell(col_w, 3.5, f'BIC: {biz["bic"]}')

    return pdf.output()


def _generate_and_process_pdf(form_data):
    """Generate PDF, trigger download, upload to Drive, and save to sheet.
    Returns (doc_number, success) tuple."""
    is_invoice = form_data['mode'] == 'invoice'

    # Get next document number
    doc_type = 'invoice' if is_invoice else 'offer'
    doc_number = _get_next_doc_number(doc_type)

    # Generate PDF
    try:
        pdf_bytes = _generate_document_pdf(form_data, doc_number)
    except Exception as e:
        st.error(f"PDF generation failed: {e}")
        return None, False

    # Build filename
    if is_invoice:
        filename = f"notpaid_RECHNUNG_JosefSindelka_{doc_number}.pdf"
        folder_name = INVOICES_FOLDER
        status = 'pending'
    else:
        filename = f"Angebot_JosefSindelka_{doc_number}.pdf"
        folder_name = OFFERS_FOLDER
        status = 'sent'

    # Trigger browser download via Streamlit
    b64 = base64.b64encode(pdf_bytes).decode()
    st.markdown(f'''
        <a href="data:application/pdf;base64,{b64}" download="{filename}"
           style="display:none" id="pdf-download-{doc_number}">download</a>
        <script>
            document.getElementById('pdf-download-{doc_number}').click();
        </script>
    ''', unsafe_allow_html=True)
    st.download_button(
        label=f"Download {filename}",
        data=pdf_bytes,
        file_name=filename,
        mime='application/pdf',
        key=f'dl_{doc_number}'
    )

    # Upload to Google Drive
    drive_file_id = ''
    try:
        year_folder = _get_year_folder()
        if year_folder:
            folder_id = _drive_find_folder(year_folder, folder_name)
            if not folder_id:
                folder_id = _drive_get_or_create_folder(year_folder, folder_name)
            if folder_id:
                result = _drive_upload_bytes(folder_id, filename, pdf_bytes, 'application/pdf')
                drive_file_id = result.get('id', '') if result else ''
                if drive_file_id:
                    st.success(f'{filename} uploaded to Google Drive.')
                else:
                    st.warning('PDF generated but Drive upload returned no ID.')
            else:
                st.warning(f'Could not find or create {folder_name} folder on Drive.')
        else:
            st.warning('Could not find year folder on Drive. PDF was downloaded but not uploaded.')
    except Exception as e:
        st.warning(f'Drive upload failed: {e}. PDF was downloaded locally.')

    # Save to Documents sheet
    form_data_with_drive = dict(form_data)
    doc_number_saved = _save_document_to_sheet(
        form_data_with_drive, status=status, doc_number=doc_number
    )
    if doc_number_saved:
        # Update with Drive info
        if drive_file_id:
            _update_document_in_sheet(doc_number, {
                'Drive_File_ID': drive_file_id,
                'Drive_Filename': filename,
            })

    _log_activity('PDF_GENERATED', f'{form_data["type"]} {doc_number} — {filename}')
    return doc_number, True


# ── Form state management ────────────────────────────────────────────────────

def _init_form_state(mode):
    """Initialize or reset form state for offer/invoice creation.
    mode: 'offer' or 'invoice'"""
    prefix = f'df_{mode}_'

    # Only init if not already set (prevents resetting on rerun)
    if f'{prefix}initialized' not in st.session_state:
        st.session_state[f'{prefix}initialized'] = True
        st.session_state[f'{prefix}client_idx'] = 0
        st.session_state[f'{prefix}lines'] = [
            {'desc': '', 'qty': '1', 'unit': 'pcs', 'price': ''}
        ]
        st.session_state[f'{prefix}nonce'] = 0


def _reset_form_state(mode):
    """Force reset form state."""
    prefix = f'df_{mode}_'
    keys_to_remove = [k for k in st.session_state if k.startswith(prefix)]
    for k in keys_to_remove:
        del st.session_state[k]


# ── Main form renderer ───────────────────────────────────────────────────────

def _render_doc_form(mode, data):
    """Render the offer/invoice creation form.
    mode: 'offer' or 'invoice'"""
    t = _t()
    prefix = f'df_{mode}_'

    _init_form_state(mode)

    # Title and subtitle
    if mode == 'offer':
        title = 'New Offer'
        subtitle = 'Create a new offer for a client'
    else:
        title = 'New Invoice'
        subtitle = 'Create a new invoice for a client'

    # Check if we're editing an existing draft
    editing_id = st.session_state.get(f'{prefix}editing_id')
    if editing_id:
        doc_number = st.session_state.get(f'{prefix}editing_number', '')
        title = f'Edit {"Offer" if mode == "offer" else "Invoice"} {doc_number}'
        subtitle = f'Editing existing {"offer" if mode == "offer" else "invoice"}'

    st.markdown(f'''
        <h1 style="font-size:24px;font-weight:700;letter-spacing:-0.3px;
                   color:{t['text']};margin-bottom:8px;font-family:{FONT}">
            {title}
        </h1>
        <p style="font-size:13px;color:{t['muted']};margin-bottom:32px;font-family:{FONT}">
            {subtitle}
        </p>
    ''', unsafe_allow_html=True)

    # Load clients
    clients = _load_clients_cached()
    client_options = ['— Select Client —'] + [
        f"{c['name']} ({c['id']})" for c in clients
    ] + ['— New Client —']

    # ── Start the form card ──
    st.markdown(f'''
        <div style="background:{t['surface']};border:1px solid {t['border']};
                    border-radius:3px;padding:28px;margin-bottom:20px">
    ''', unsafe_allow_html=True)

    # ── Row 1: Client + Address ──
    col_client, col_addr = st.columns(2, gap="medium")
    with col_client:
        client_sel = st.selectbox(
            'CLIENT NAME / COMPANY',
            options=client_options,
            key=f'{prefix}client_sel',
            label_visibility='visible'
        )

    # Auto-fill address when client changes
    selected_client = None
    if client_sel and client_sel not in ('— Select Client —', '— New Client —'):
        for c in clients:
            if f"{c['name']} ({c['id']})" == client_sel:
                selected_client = c
                break
    prev_sel_key = f'{prefix}prev_client_sel'
    addr_key = f'{prefix}client_addr'
    if client_sel != st.session_state.get(prev_sel_key):
        st.session_state[prev_sel_key] = client_sel
        if selected_client:
            st.session_state[addr_key] = selected_client.get('address', '')
        elif client_sel in ('— Select Client —', '— New Client —'):
            st.session_state[addr_key] = ''

    with col_addr:
        client_address = st.text_input(
            'CLIENT ADDRESS',
            key=addr_key,
            placeholder='e.g. New-York-Ring 6, 22297 Hamburg'
        )

    # ── New Client inline form ──
    if client_sel == '— New Client —':
        st.markdown(f'''
            <div style="background:{t['surface2']};border:1px solid {t['border']};
                        border-radius:3px;padding:16px;margin:8px 0 16px 0">
                <div style="font-size:11px;font-weight:600;text-transform:uppercase;
                            letter-spacing:0.8px;color:{t['muted']};margin-bottom:12px;
                            font-family:{FONT}">
                    NEW CLIENT
                </div>
        ''', unsafe_allow_html=True)
        nc_col1, nc_col2 = st.columns(2)
        with nc_col1:
            new_client_name = st.text_input('COMPANY NAME', key=f'{prefix}new_client_name',
                                            placeholder='e.g. Acme GmbH')
        with nc_col2:
            new_client_address = st.text_input('ADDRESS', key=f'{prefix}new_client_address',
                                               placeholder='e.g. Musterstr. 1, 20095 Hamburg')
        nc_col3, nc_col4 = st.columns(2)
        with nc_col3:
            new_client_contact = st.text_input('CONTACT PERSON', key=f'{prefix}new_client_contact',
                                               placeholder='e.g. Max Mustermann')
        with nc_col4:
            new_client_email = st.text_input('EMAIL', key=f'{prefix}new_client_email',
                                             placeholder='e.g. info@acme.de')

        next_k = _get_next_client_number()
        if st.button(f'Create Client ({next_k})', key=f'{prefix}create_client_btn'):
            name = st.session_state.get(f'{prefix}new_client_name', '').strip()
            if name:
                new_client = {
                    'id': next_k,
                    'name': name,
                    'address': st.session_state.get(f'{prefix}new_client_address', ''),
                    'contact_person': st.session_state.get(f'{prefix}new_client_contact', ''),
                    'email': st.session_state.get(f'{prefix}new_client_email', ''),
                }
                if _add_client_to_sheet(new_client):
                    st.success(f'Client {next_k} created.')
                    st.rerun()
            else:
                st.warning('Please enter a company name.')
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Row 2: Project Title + Date ──
    col_title, col_date = st.columns(2, gap="medium")
    with col_title:
        project_title = st.text_input(
            'PROJECT TITLE',
            key=f'{prefix}project_title',
            placeholder='e.g. HR Recruitment Photography'
        )
    with col_date:
        doc_date = st.date_input(
            'DATE',
            value=datetime.now().date(),
            key=f'{prefix}date',
            format='DD.MM.YYYY'
        )

    # ── Project Description ──
    project_desc = st.text_area(
        'PROJECT DESCRIPTION',
        key=f'{prefix}project_desc',
        placeholder='Brief project description...',
        height=100
    )

    # ── Internal Category ──
    category = st.selectbox(
        'INTERNAL CATEGORY',
        options=INCOME_CATEGORIES,
        key=f'{prefix}category',
        help='Internal only — will not appear on the PDF'
    )

    # ── Mode-specific fields ──
    if mode == 'offer':
        validity = st.number_input(
            'VALIDITY (DAYS)',
            min_value=1, max_value=365, value=30,
            key=f'{prefix}validity'
        )
    else:
        # Invoice-specific fields
        inv_col1, inv_col2 = st.columns(2, gap="medium")
        with inv_col1:
            service_date = st.text_input(
                'SERVICE / DELIVERY DATE OR PERIOD',
                key=f'{prefix}service_date',
                placeholder='e.g. 15.02.2026 or 01.02.–15.02.2026'
            )
        with inv_col2:
            payment_terms = st.text_input(
                'PAYMENT TERMS',
                value='Zahlbar sofort, rein netto',
                key=f'{prefix}payment_terms'
            )

    # ── Line Items ──
    st.markdown(f'''
        <h2 style="font-size:18px;font-weight:600;color:{t['text']};
                   margin-top:24px;margin-bottom:16px;font-family:{FONT}">
            Line Items
        </h2>
    ''', unsafe_allow_html=True)

    lines_key = f'{prefix}lines'
    lines = st.session_state.get(lines_key, [{'desc': '', 'qty': '1', 'unit': 'pcs', 'price': ''}])

    # Table header
    st.markdown(f'''
        <div style="display:grid;grid-template-columns:5% 40% 12% 15% 18% 5%;
                    gap:0;padding:8px;border-bottom:2px solid {t['border']};
                    font-size:10px;font-weight:600;text-transform:uppercase;
                    letter-spacing:0.8px;color:{t['muted']};font-family:{FONT}">
            <div>#</div>
            <div>Description</div>
            <div>Qty</div>
            <div>Unit</div>
            <div>Unit Price (EUR)</div>
            <div></div>
        </div>
    ''', unsafe_allow_html=True)

    # Render each line item
    subtotal = 0.0
    lines_to_keep = []
    remove_idx = None

    for i, line in enumerate(lines):
        cols = st.columns([0.5, 4, 1.2, 1.5, 1.8, 0.5])
        with cols[0]:
            st.markdown(f'''
                <div style="padding:10px 0;font-size:13px;color:{t['text']};
                            font-weight:600;font-family:{FONT}">{i + 1}</div>
            ''', unsafe_allow_html=True)
        with cols[1]:
            desc = st.text_input('desc', value=line.get('desc', ''),
                                 key=f'{prefix}line_desc_{i}',
                                 placeholder='Service description',
                                 label_visibility='collapsed')
        with cols[2]:
            qty = st.text_input('qty', value=str(line.get('qty', '1')),
                                key=f'{prefix}line_qty_{i}',
                                placeholder='1',
                                label_visibility='collapsed')
        with cols[3]:
            unit = st.text_input('unit', value=line.get('unit', 'pcs'),
                                 key=f'{prefix}line_unit_{i}',
                                 placeholder='pcs/hrs/days',
                                 label_visibility='collapsed')
        with cols[4]:
            price = st.text_input('price', value=line.get('price', ''),
                                  key=f'{prefix}line_price_{i}',
                                  placeholder='0,00',
                                  label_visibility='collapsed')
        with cols[5]:
            if st.button('×', key=f'{prefix}line_rm_{i}', help='Remove line'):
                remove_idx = i

        # Calculate line total
        qty_val = _parse_eur_input(qty) if qty else 0
        price_val = _parse_eur_input(price) if price else 0
        line_total = qty_val * price_val
        subtotal += line_total

        lines_to_keep.append({'desc': desc, 'qty': qty, 'unit': unit, 'price': price})

    # Handle remove
    if remove_idx is not None and len(lines_to_keep) > 1:
        lines_to_keep.pop(remove_idx)
        st.session_state[lines_key] = lines_to_keep
        st.rerun()
    else:
        st.session_state[lines_key] = lines_to_keep

    # Add Line button
    if st.button('+ Add Line', key=f'{prefix}add_line'):
        st.session_state[lines_key].append({'desc': '', 'qty': '1', 'unit': 'pcs', 'price': ''})
        st.rerun()

    # ── Totals ──
    vat = subtotal * VAT_RATE
    brutto = subtotal + vat

    st.markdown(f'''
        <div style="display:flex;justify-content:flex-end;margin-top:20px">
            <table style="width:280px;border-collapse:collapse;font-family:{FONT}">
                <tr>
                    <td style="text-align:right;padding:6px 16px 6px 0;
                               color:{t['text_secondary']};font-size:13px">Subtotal (Netto)</td>
                    <td style="text-align:right;font-weight:600;font-size:14px;
                               font-variant-numeric:tabular-nums;color:{t['text']}">{_fmt_eur(subtotal)}</td>
                </tr>
                <tr>
                    <td style="text-align:right;padding:6px 16px 6px 0;
                               color:{t['text_secondary']};font-size:13px">USt. 19%</td>
                    <td style="text-align:right;font-weight:600;font-size:14px;
                               font-variant-numeric:tabular-nums;color:{t['text']}">{_fmt_eur(vat)}</td>
                </tr>
                <tr>
                    <td style="text-align:right;padding:14px 16px 6px 0;
                               color:{t['text']};font-size:13px;font-weight:600;
                               border-top:2px solid {t['text']}">Total (Brutto)</td>
                    <td style="text-align:right;font-weight:700;font-size:16px;
                               font-variant-numeric:tabular-nums;color:{t['text']};
                               padding-top:14px;border-top:2px solid {t['text']}">{_fmt_eur(brutto)}</td>
                </tr>
            </table>
        </div>
    ''', unsafe_allow_html=True)

    # ── Close the card div ──
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Action Buttons ──
    btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 4])
    with btn_col1:
        generate_clicked = st.button('Generate PDF', key=f'{prefix}generate_pdf',
                                     type='primary', use_container_width=True)
    with btn_col2:
        draft_clicked = st.button('Save as Draft', key=f'{prefix}save_draft',
                                  use_container_width=True)

    # ── Collect form data ──
    form_data = {
        'mode': mode,
        'type': 'Angebot' if mode == 'offer' else 'Rechnung',
        'client_selection': client_sel,
        'client_id': selected_client['id'] if selected_client else '',
        'client_name': selected_client['name'] if selected_client else (
            st.session_state.get(f'{prefix}new_client_name', '') if client_sel == '— New Client —' else ''
        ),
        'client_address': client_address,
        'project_title': project_title,
        'project_description': project_desc,
        'category': category,
        'date': doc_date.strftime('%d.%m.%Y') if doc_date else '',
        'date_obj': doc_date,
        'line_items': lines_to_keep,
        'netto': subtotal,
        'ust': vat,
        'brutto': brutto,
    }

    if mode == 'offer':
        form_data['validity_days'] = validity
    else:
        form_data['service_date'] = st.session_state.get(f'{prefix}service_date', '')
        form_data['payment_terms'] = st.session_state.get(f'{prefix}payment_terms', 'Zahlbar sofort, rein netto')

    # Handle button clicks
    if generate_clicked:
        if not form_data['client_name']:
            st.warning('Please select or create a client.')
        elif subtotal <= 0:
            st.warning('Please add at least one line item with a price.')
        else:
            with st.spinner('Generating PDF...'):
                doc_num, success = _generate_and_process_pdf(form_data)
                if success:
                    st.success(f'{form_data["type"]} {doc_num} generated successfully.')

    if draft_clicked:
        if not form_data['client_name']:
            st.warning('Please select or create a client.')
        else:
            # Check if editing an existing draft
            editing_id = st.session_state.get(f'{prefix}editing_id')
            if editing_id:
                # Update existing document
                updates = {
                    'Client_ID': form_data.get('client_id', ''),
                    'Client_Name': form_data.get('client_name', ''),
                    'Client_Address': form_data.get('client_address', ''),
                    'Project_Title': form_data.get('project_title', ''),
                    'Project_Description': form_data.get('project_description', ''),
                    'Category': form_data.get('category', ''),
                    'Date': form_data.get('date', ''),
                    'Line_Items_JSON': json.dumps(form_data.get('line_items', []), ensure_ascii=False),
                    'Netto': f"{form_data.get('netto', 0):.2f}",
                    'USt': f"{form_data.get('ust', 0):.2f}",
                    'Brutto': f"{form_data.get('brutto', 0):.2f}",
                    'Validity_Days': str(form_data.get('validity_days', '')),
                    'Service_Date': form_data.get('service_date', ''),
                    'Payment_Terms': form_data.get('payment_terms', ''),
                }
                doc_number = st.session_state.get(f'{prefix}editing_number', '')
                if _update_document_in_sheet(doc_number, updates):
                    st.success(f'Draft {doc_number} updated.')
                    _log_activity('DRAFT_UPDATED', f'{form_data["type"]} {doc_number} updated')
            else:
                # Save new draft
                doc_number = _save_document_to_sheet(form_data, status='draft')
                if doc_number:
                    st.success(f'Draft saved as {doc_number}.')

    return form_data


# ── Tab functions for new views ──────────────────────────────────────────────

def tab_new_offer(data):
    """New Offer creation form."""
    _render_doc_form('offer', data)


def tab_new_invoice(data):
    """New Invoice creation form."""
    _render_doc_form('invoice', data)


def tab_offers_invoices(data):
    """Offers & Invoices management view — combined table for offers and invoices."""
    t = _t()
    is_dark = st.session_state.get('theme', 'light') == 'dark'

    st.markdown(f'<h1 style="font-size:24px;font-weight:700;letter-spacing:-0.3px;color:{t["text"]};margin-bottom:8px;font-family:{FONT}">Offers & Invoices</h1>', unsafe_allow_html=True)
    st.markdown(f'<p style="font-size:13px;color:{t["muted"]};margin-bottom:32px;font-family:{FONT}">Manage all your offers and invoices</p>', unsafe_allow_html=True)

    # ── Load documents (auto-import existing invoices on first visit) ──
    docs = _load_documents()
    if not docs and not st.session_state.get('_docs_import_attempted'):
        with st.spinner('Importing existing invoices from Google Drive...'):
            imported = _import_existing_invoices_to_documents()
        st.session_state['_docs_import_attempted'] = True
        if imported > 0:
            docs = _load_documents()

    # ── Type toggle: All / Offers / Invoices ──
    if 'oi_type_filter' not in st.session_state:
        st.session_state['oi_type_filter'] = 'all'
    if 'oi_status_filter' not in st.session_state:
        st.session_state['oi_status_filter'] = 'all'

    # Type filter row (on_click callbacks to avoid double-rerun)
    type_options = ['All', 'Offers', 'Invoices']
    tcols = st.columns(len(type_options) + 6)
    for i, opt in enumerate(type_options):
        key_val = opt.lower()
        is_active = st.session_state['oi_type_filter'] == key_val
        tcols[i].button(
            opt,
            key=f'oi_type_{key_val}',
            type='primary' if is_active else 'secondary',
            use_container_width=True,
            on_click=lambda v=key_val: st.session_state.update({'oi_type_filter': v}),
        )

    # Status filter row (on_click callbacks to avoid double-rerun)
    status_options = ['All', 'Draft', 'Sent', 'Pending', 'Paid']
    scols = st.columns(len(status_options) + 5)
    for i, opt in enumerate(status_options):
        key_val = opt.lower()
        is_active = st.session_state['oi_status_filter'] == key_val
        scols[i].button(
            opt,
            key=f'oi_status_{key_val}',
            type='primary' if is_active else 'secondary',
            use_container_width=True,
            on_click=lambda v=key_val: st.session_state.update({'oi_status_filter': v}),
        )

    # ── Filter documents ──
    type_filter = st.session_state['oi_type_filter']
    status_filter = st.session_state['oi_status_filter']

    filtered = docs
    if type_filter == 'offers':
        filtered = [d for d in filtered if str(d.get('Type', '')).lower() in ('angebot', 'offer')]
    elif type_filter == 'invoices':
        filtered = [d for d in filtered if str(d.get('Type', '')).lower() in ('rechnung', 'invoice')]

    if status_filter != 'all':
        filtered = [d for d in filtered if str(d.get('Status', '')).lower() == status_filter]

    # Sort by Created_At descending (newest first)
    filtered.sort(key=lambda d: d.get('Created_At', ''), reverse=True)

    # ── Badge helper ──
    def _status_badge(status_str):
        s = str(status_str).lower().strip()
        badge_map = {
            'draft': ('badge-draft', 'Draft'),
            'sent': ('badge-sent', 'Sent'),
            'pending': ('badge-sent', 'Pending'),
            'paid': ('badge-paid', 'Paid'),
        }
        css_cls, label = badge_map.get(s, ('badge-draft', status_str.title()))
        return f'<span class="badge {css_cls}" style="text-transform:uppercase;font-size:11px;font-weight:600;letter-spacing:0.5px;padding:3px 10px;border-radius:2px;">{label}</span>'

    def _type_label(type_str):
        s = str(type_str).lower().strip()
        if s in ('angebot', 'offer'):
            return 'Offer'
        elif s in ('rechnung', 'invoice'):
            return 'Invoice'
        return type_str.title()

    # ── Render table ──
    if not filtered:
        st.markdown(f'''
        <div style="text-align:center;color:{t["muted"]};padding:40px;font-size:14px;font-family:{FONT};
            background:{t["surface"]};border:1px solid {t["border"]};border-radius:3px;">
            No documents found.
        </div>''', unsafe_allow_html=True)
    else:
        # Table header
        header_style = f'font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;color:{t["muted"]};padding:10px 0;border-bottom:2px solid {t["border"]};font-family:{FONT}'
        row_style = f'font-size:13px;color:{t["text"]};border-bottom:1px solid {t["row_border"]};font-family:{FONT}'

        table_rows = []
        for doc in filtered:
            num = doc.get('Number', '')
            doc_type = _type_label(doc.get('Type', ''))
            client = doc.get('Client_Name', '')
            project = doc.get('Project_Title', '') or '\u2014'
            netto = doc.get('Netto', 0)
            try:
                netto_f = float(str(netto).replace(',', '.'))
            except (ValueError, TypeError):
                netto_f = 0
            amount_str = _fmt_eur(netto_f)
            date_str = doc.get('Date', '')
            status = doc.get('Status', 'draft')
            badge = _status_badge(status)

            table_rows.append(f'''
            <tr style="transition:background 0.1s;">
                <td style="{row_style};font-weight:600;padding:12px 8px 12px 0;">{num}</td>
                <td style="{row_style};padding:12px 8px;">{doc_type}</td>
                <td style="{row_style};padding:12px 8px;">{client}</td>
                <td style="{row_style};padding:12px 8px;">{project}</td>
                <td style="{row_style};font-variant-numeric:tabular-nums;padding:12px 8px;">{amount_str}</td>
                <td style="{row_style};padding:12px 8px;">{date_str}</td>
                <td style="{row_style};padding:12px 8px;">{badge}</td>
            </tr>''')

        table_html = f'''
        <div style="background:{t["surface"]};border:1px solid {t["border"]};border-radius:3px;padding:0 20px;overflow-x:auto;">
            <table style="width:100%;border-collapse:collapse;">
                <thead>
                    <tr>
                        <th style="{header_style};text-align:left;">Number</th>
                        <th style="{header_style};text-align:left;">Type</th>
                        <th style="{header_style};text-align:left;">Client</th>
                        <th style="{header_style};text-align:left;">Project</th>
                        <th style="{header_style};text-align:left;">Amount</th>
                        <th style="{header_style};text-align:left;">Date</th>
                        <th style="{header_style};text-align:left;">Status</th>
                    </tr>
                </thead>
                <tbody>{"".join(table_rows)}</tbody>
            </table>
        </div>'''
        st.markdown(table_html, unsafe_allow_html=True)

    st.markdown('<div style="height:16px;"></div>', unsafe_allow_html=True)

    # ── Action section for selected document ──
    if filtered:
        st.markdown(f'<p style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;color:{t["muted"]};margin:16px 0 8px;font-family:{FONT}">Actions</p>', unsafe_allow_html=True)

        # Document selector
        doc_options = [f"{d.get('Number','')} — {_type_label(d.get('Type',''))} — {d.get('Client_Name','')}" for d in filtered]
        selected_idx = st.selectbox('Select document', range(len(doc_options)), format_func=lambda i: doc_options[i], key='oi_selected_doc', label_visibility='collapsed')

        if selected_idx is not None and selected_idx < len(filtered):
            sel_doc = filtered[selected_idx]
            doc_num = sel_doc.get('Number', '')
            doc_status = str(sel_doc.get('Status', '')).lower()
            doc_type_raw = str(sel_doc.get('Type', '')).lower()
            is_offer = doc_type_raw in ('angebot', 'offer')

            acols = st.columns(6)

            # ── Edit button ──
            with acols[0]:
                if st.button('Edit', key=f'oi_edit_{doc_num}', use_container_width=True):
                    # Load doc data into the form and navigate to edit page
                    mode = 'offer' if is_offer else 'invoice'
                    prefix = f'df_{mode}_'
                    # Clear existing form state
                    for k in list(st.session_state.keys()):
                        if k.startswith(prefix):
                            del st.session_state[k]
                    # Set editing state
                    st.session_state[f'{prefix}editing_id'] = sel_doc.get('ID', '')
                    st.session_state[f'{prefix}editing_number'] = doc_num
                    # Pre-fill form
                    try:
                        items = json.loads(sel_doc.get('Line_Items_JSON', '[]'))
                    except (json.JSONDecodeError, TypeError):
                        items = []
                    st.session_state[f'{prefix}lines'] = items if items else [{'desc': '', 'qty': '1', 'unit': 'pcs', 'price': ''}]
                    st.session_state[f'{prefix}initialized'] = True
                    st.session_state[f'{prefix}nonce'] = 0
                    st.session_state[f'{prefix}project_title'] = sel_doc.get('Project_Title', '')
                    st.session_state[f'{prefix}project_desc'] = sel_doc.get('Project_Description', '')
                    st.session_state[f'{prefix}category'] = sel_doc.get('Category', '')
                    st.session_state[f'{prefix}client_address'] = sel_doc.get('Client_Address', '')
                    # Navigate
                    st.session_state['active_page'] = 'New Offer' if is_offer else 'New Invoice'
                    st.rerun()

            # ── Status change button ──
            with acols[1]:
                if st.button('Status', key=f'oi_status_{doc_num}', use_container_width=True):
                    st.session_state['oi_status_modal'] = doc_num

            # ── Re-generate PDF ──
            with acols[2]:
                if st.button('PDF', key=f'oi_pdf_{doc_num}', use_container_width=True):
                    # Re-generate PDF from stored data
                    try:
                        items = json.loads(sel_doc.get('Line_Items_JSON', '[]'))
                    except (json.JSONDecodeError, TypeError):
                        items = []
                    netto_val = float(str(sel_doc.get('Netto', 0)).replace(',', '.')) if sel_doc.get('Netto') else 0
                    ust_val = float(str(sel_doc.get('USt', 0)).replace(',', '.')) if sel_doc.get('USt') else 0
                    brutto_val = float(str(sel_doc.get('Brutto', 0)).replace(',', '.')) if sel_doc.get('Brutto') else 0
                    # Parse date
                    date_str = sel_doc.get('Date', '')
                    date_obj = None
                    try:
                        from datetime import datetime as dt_cls
                        date_obj = dt_cls.strptime(date_str, '%d.%m.%Y').date()
                    except Exception:
                        date_obj = date.today()
                    form_data = {
                        'mode': 'offer' if is_offer else 'invoice',
                        'type': sel_doc.get('Type', ''),
                        'client_id': sel_doc.get('Client_ID', ''),
                        'client_name': sel_doc.get('Client_Name', ''),
                        'client_address': sel_doc.get('Client_Address', ''),
                        'project_title': sel_doc.get('Project_Title', ''),
                        'project_description': sel_doc.get('Project_Description', ''),
                        'date': date_str,
                        'date_obj': date_obj,
                        'line_items': items,
                        'netto': netto_val,
                        'ust': ust_val,
                        'brutto': brutto_val,
                        'validity_days': sel_doc.get('Validity_Days', 30),
                        'service_date': sel_doc.get('Service_Date', ''),
                        'payment_terms': sel_doc.get('Payment_Terms', ''),
                    }
                    with st.spinner('Generating PDF...'):
                        try:
                            pdf_bytes = _generate_document_pdf(form_data, doc_num)
                            if is_offer:
                                fn = f"Angebot_JosefSindelka_{doc_num}.pdf"
                            else:
                                prefix_fn = 'notpaid_' if doc_status != 'paid' else ''
                                fn = f"{prefix_fn}RECHNUNG_JosefSindelka_{doc_num}.pdf"
                            st.download_button(
                                label=f"Download {fn}",
                                data=pdf_bytes,
                                file_name=fn,
                                mime='application/pdf',
                                key=f'oi_dl_{doc_num}'
                            )
                        except Exception as e:
                            st.error(f"PDF generation failed: {e}")

            # ── Convert to Invoice (offers only) ──
            with acols[3]:
                if is_offer:
                    if st.button('→ Invoice', key=f'oi_convert_{doc_num}', use_container_width=True):
                        st.session_state['oi_convert_modal'] = doc_num

            # ── Delete ──
            with acols[4]:
                if st.button('Delete', key=f'oi_del_{doc_num}', use_container_width=True):
                    st.session_state['oi_delete_modal'] = doc_num

    # ── Status change modal ──
    if 'oi_status_modal' in st.session_state and st.session_state['oi_status_modal']:
        modal_doc_num = st.session_state['oi_status_modal']
        st.markdown(f'<hr style="border:none;border-top:1px solid {t["border"]};margin:16px 0;">', unsafe_allow_html=True)
        st.markdown(f'<p style="font-size:16px;font-weight:600;color:{t["text"]};font-family:{FONT}">Update Status — {modal_doc_num}</p>', unsafe_allow_html=True)
        new_status = st.selectbox('New Status', ['draft', 'sent', 'pending', 'paid'], key='oi_new_status')
        mcols = st.columns([1, 1, 4])
        with mcols[0]:
            if st.button('Update', key='oi_confirm_status', type='primary', use_container_width=True):
                if _update_document_in_sheet(modal_doc_num, {'Status': new_status}):
                    # If invoice and status changed to/from paid, rename on Drive
                    doc_match = [d for d in docs if d.get('Number') == modal_doc_num]
                    if doc_match:
                        dm = doc_match[0]
                        drive_id = dm.get('Drive_File_ID', '')
                        old_fn = dm.get('Drive_Filename', '')
                        doc_type_raw = str(dm.get('Type', '')).lower()
                        if drive_id and old_fn and doc_type_raw in ('rechnung', 'invoice'):
                            if new_status == 'paid' and old_fn.startswith('notpaid_'):
                                new_fn = old_fn.replace('notpaid_', '', 1)
                                try:
                                    _drive_rename_file(drive_id, new_fn)
                                    _update_document_in_sheet(modal_doc_num, {'Drive_Filename': new_fn})
                                except Exception:
                                    pass
                            elif new_status != 'paid' and not old_fn.startswith('notpaid_'):
                                new_fn = 'notpaid_' + old_fn
                                try:
                                    _drive_rename_file(drive_id, new_fn)
                                    _update_document_in_sheet(modal_doc_num, {'Drive_Filename': new_fn})
                                except Exception:
                                    pass
                    st.success(f'Status of {modal_doc_num} updated to {new_status}.')
                    _log_activity('STATUS_CHANGED', f'{modal_doc_num} → {new_status}')
                    st.session_state['oi_status_modal'] = None
                    st.rerun()
        with mcols[1]:
            if st.button('Cancel', key='oi_cancel_status', use_container_width=True):
                st.session_state['oi_status_modal'] = None
                st.rerun()

    # ── Convert to Invoice modal ──
    if 'oi_convert_modal' in st.session_state and st.session_state['oi_convert_modal']:
        modal_doc_num = st.session_state['oi_convert_modal']
        st.markdown(f'<hr style="border:none;border-top:1px solid {t["border"]};margin:16px 0;">', unsafe_allow_html=True)
        st.markdown(f'<p style="font-size:16px;font-weight:600;color:{t["text"]};font-family:{FONT}">Convert {modal_doc_num} to Invoice</p>', unsafe_allow_html=True)
        st.markdown(f'<p style="font-size:13px;color:{t["muted"]};font-family:{FONT}">This will create a new invoice pre-filled with the offer data. The original offer will remain unchanged.</p>', unsafe_allow_html=True)
        ccols = st.columns([1, 1, 4])
        with ccols[0]:
            if st.button('Convert', key='oi_confirm_convert', type='primary', use_container_width=True):
                # Find the source offer
                src = [d for d in docs if d.get('Number') == modal_doc_num]
                if src:
                    s = src[0]
                    # Load into invoice form
                    prefix = 'df_invoice_'
                    for k in list(st.session_state.keys()):
                        if k.startswith(prefix):
                            del st.session_state[k]
                    try:
                        items = json.loads(s.get('Line_Items_JSON', '[]'))
                    except (json.JSONDecodeError, TypeError):
                        items = []
                    st.session_state[f'{prefix}lines'] = items if items else [{'desc': '', 'qty': '1', 'unit': 'pcs', 'price': ''}]
                    st.session_state[f'{prefix}initialized'] = True
                    st.session_state[f'{prefix}nonce'] = 0
                    st.session_state[f'{prefix}project_title'] = s.get('Project_Title', '')
                    st.session_state[f'{prefix}project_desc'] = s.get('Project_Description', '')
                    st.session_state[f'{prefix}category'] = s.get('Category', '')
                    st.session_state[f'{prefix}client_address'] = s.get('Client_Address', '')
                    st.session_state[f'{prefix}source_offer'] = modal_doc_num
                    st.session_state['oi_convert_modal'] = None
                    st.session_state['active_page'] = 'New Invoice'
                    st.rerun()
        with ccols[1]:
            if st.button('Cancel', key='oi_cancel_convert', use_container_width=True):
                st.session_state['oi_convert_modal'] = None
                st.rerun()

    # ── Delete modal ──
    if 'oi_delete_modal' in st.session_state and st.session_state['oi_delete_modal']:
        modal_doc_num = st.session_state['oi_delete_modal']
        st.markdown(f'<hr style="border:none;border-top:1px solid {t["border"]};margin:16px 0;">', unsafe_allow_html=True)
        st.markdown(f'<p style="font-size:16px;font-weight:600;color:{t["text"]};font-family:{FONT}">Delete {modal_doc_num}</p>', unsafe_allow_html=True)
        st.markdown(f'<p style="font-size:13px;color:{t["muted"]};font-family:{FONT}">Are you sure? This action cannot be undone.</p>', unsafe_allow_html=True)
        dcols = st.columns([1, 1, 4])
        with dcols[0]:
            if st.button('Delete', key='oi_confirm_del', type='primary', use_container_width=True):
                # Delete from sheet
                try:
                    ws = _get_or_create_documents_sheet()
                    all_records = ws.get_all_records()
                    for idx, rec in enumerate(all_records):
                        if str(rec.get('Number', '')) == str(modal_doc_num):
                            ws.delete_rows(idx + 2)
                            break
                    # Also try to delete from Drive
                    doc_match = [d for d in docs if d.get('Number') == modal_doc_num]
                    if doc_match and doc_match[0].get('Drive_File_ID'):
                        try:
                            _drive_delete_file(doc_match[0]['Drive_File_ID'])
                        except Exception:
                            pass
                    _invalidate_documents_cache()
                    _log_activity('DOC_DELETED', f'{modal_doc_num} deleted')
                    st.success(f'{modal_doc_num} deleted.')
                    st.session_state['oi_delete_modal'] = None
                    st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")
        with dcols[1]:
            if st.button('Cancel', key='oi_cancel_del', use_container_width=True):
                st.session_state['oi_delete_modal'] = None
                st.rerun()


def tab_clients(data):
    """Clients management view — table + add/edit."""
    t = _t()
    is_dark = st.session_state.get('theme', 'light') == 'dark'

    st.markdown(f'<h1 style="font-size:24px;font-weight:700;letter-spacing:-0.3px;color:{t["text"]};margin-bottom:8px;font-family:{FONT}">Clients</h1>', unsafe_allow_html=True)
    st.markdown(f'<p style="font-size:13px;color:{t["muted"]};margin-bottom:32px;font-family:{FONT}">Manage your client database</p>', unsafe_allow_html=True)

    # ── Load clients and documents ──
    clients = _load_clients_cached()
    docs = _load_documents()

    # Count documents per client
    doc_counts = {}
    for d in docs:
        cid = d.get('Client_ID', '')
        if cid:
            doc_counts[cid] = doc_counts.get(cid, 0) + 1

    # ── Render clients table ──
    if not clients:
        st.markdown(f'''
        <div style="text-align:center;color:{t["muted"]};padding:40px;font-size:14px;font-family:{FONT};
            background:{t["surface"]};border:1px solid {t["border"]};border-radius:3px;">
            No clients yet. Add your first client below.
        </div>''', unsafe_allow_html=True)
    else:
        header_style = f'font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;color:{t["muted"]};padding:10px 0;border-bottom:2px solid {t["border"]};font-family:{FONT}'
        row_style = f'font-size:13px;color:{t["text"]};border-bottom:1px solid {t["row_border"]};font-family:{FONT}'

        table_rows = []
        for c in clients:
            cid = c.get('id', '')
            name = c.get('name', '')
            address = c.get('address', '') or '\u2014'
            notes = c.get('notes', '') or '\u2014'
            country = c.get('country', '') or ''
            num_docs = doc_counts.get(cid, 0)

            table_rows.append(f'''
            <tr>
                <td style="{row_style};font-weight:600;padding:12px 8px 12px 0;">{cid}</td>
                <td style="{row_style};padding:12px 8px;">{name}</td>
                <td style="{row_style};padding:12px 8px;">{address}</td>
                <td style="{row_style};padding:12px 8px;font-size:12px;color:{t["muted"]};max-width:250px;">{notes}</td>
                <td style="{row_style};padding:12px 8px;">{country}</td>
                <td style="{row_style};padding:12px 8px;font-variant-numeric:tabular-nums;">{num_docs}</td>
            </tr>''')

        table_html = f'''
        <div style="background:{t["surface"]};border:1px solid {t["border"]};border-radius:3px;padding:0 20px;overflow-x:auto;">
            <table style="width:100%;border-collapse:collapse;">
                <thead>
                    <tr>
                        <th style="{header_style};text-align:left;">Client No.</th>
                        <th style="{header_style};text-align:left;">Name</th>
                        <th style="{header_style};text-align:left;">Address</th>
                        <th style="{header_style};text-align:left;">Notes</th>
                        <th style="{header_style};text-align:left;">Country</th>
                        <th style="{header_style};text-align:left;">Docs</th>
                    </tr>
                </thead>
                <tbody>{"".join(table_rows)}</tbody>
            </table>
        </div>'''
        st.markdown(table_html, unsafe_allow_html=True)

    st.markdown('<div style="height:16px;"></div>', unsafe_allow_html=True)

    # ── Edit existing client ──
    if clients:
        st.markdown(f'<p style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;color:{t["muted"]};margin:16px 0 8px;font-family:{FONT}">Edit Client</p>', unsafe_allow_html=True)
        client_options = [f"{c.get('id','')} — {c.get('name','')}" for c in clients]
        sel_idx = st.selectbox('Select client to edit', range(len(client_options)), format_func=lambda i: client_options[i], key='cl_edit_sel', label_visibility='collapsed')

        if sel_idx is not None and sel_idx < len(clients):
            sel_client = clients[sel_idx]
            ec1, ec2 = st.columns(2)
            with ec1:
                edit_name = st.text_input('Name', value=sel_client.get('name', ''), key='cl_edit_name')
            with ec2:
                edit_country = st.text_input('Country', value=sel_client.get('country', ''), key='cl_edit_country')
            edit_address = st.text_input('Address', value=sel_client.get('address', ''), key='cl_edit_address')
            edit_notes = st.text_input('Notes', value=sel_client.get('notes', ''), key='cl_edit_notes')

            if st.button('Save Changes', key='cl_save_edit', use_container_width=False):
                # Update in Google Sheet (columns: ID, Name, Address, Notes, Country, Added)
                try:
                    ws = _gsheet().worksheet('Clients')
                    all_records = ws.get_all_records()
                    headers = ws.row_values(1)
                    target_id = sel_client.get('id', '')
                    for idx, rec in enumerate(all_records):
                        rec_id = str(rec.get('ID', '') or rec.get('Client_ID', ''))
                        if rec_id == str(target_id):
                            row_num = idx + 2
                            updates = {
                                'Name': edit_name.strip(),
                                'Address': edit_address.strip(),
                                'Notes': edit_notes.strip(),
                                'Country': edit_country.strip(),
                            }
                            cells_to_update = []
                            for col_name, val in updates.items():
                                if col_name in headers:
                                    col_idx = headers.index(col_name) + 1
                                    cells_to_update.append(gspread.Cell(row_num, col_idx, str(val)))
                            if cells_to_update:
                                ws.update_cells(cells_to_update)
                            _load_clients_cached.clear()
                            _log_activity('CLIENT_UPDATED', f'{target_id} — {edit_name}')
                            st.success(f'Client {target_id} updated.')
                            st.rerun()
                except Exception as e:
                    st.error(f"Failed to update client: {e}")

    # ── Add new client ──
    st.markdown(f'<hr style="border:none;border-top:1px solid {t["border"]};margin:20px 0;">', unsafe_allow_html=True)
    st.markdown(f'<p style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;color:{t["muted"]};margin:0 0 8px;font-family:{FONT}">Add New Client</p>', unsafe_allow_html=True)

    nc1, nc2 = st.columns(2)
    with nc1:
        new_name = st.text_input('Client / Company Name', key='cl_new_name')
    with nc2:
        new_country = st.text_input('Country', key='cl_new_country')
    new_address = st.text_input('Address', key='cl_new_address')
    new_notes = st.text_input('Notes', key='cl_new_notes')

    if st.button('+ Add Client', key='cl_add_new', use_container_width=False):
        if not new_name.strip():
            st.warning('Please enter a client name.')
        else:
            try:
                next_id = _get_next_client_number()
                client_data = {
                    'id': next_id,
                    'name': new_name.strip(),
                    'address': new_address.strip(),
                    'notes': new_notes.strip(),
                    'country': new_country.strip(),
                }
                _add_client_to_sheet(client_data)
                _log_activity('CLIENT_ADDED', f'{next_id} — {new_name.strip()}')
                st.success(f'Client {next_id} — {new_name.strip()} added.')
                # Clear fields
                for k in ['cl_new_name', 'cl_new_address', 'cl_new_notes', 'cl_new_country']:
                    if k in st.session_state:
                        st.session_state[k] = ''
                st.rerun()
            except Exception as e:
                st.error(f"Failed to add client: {e}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    # One-time category migrations
    if not st.session_state.get('_migration_categories_done'):
        try:
            _recategorize_invoices_by_client('GenHQ', 'AI Software', 'Education')
            _recategorize_invoices_by_client('Figma', 'AI Software', 'Software', exclude_description_substring='Weavy')
            st.session_state['_migration_categories_done'] = True
        except Exception:
            pass

    data = load_data()

    # ── Theme Toggle ──
    if 'theme' not in st.session_state:
        st.session_state['theme'] = 'light'

    # ── Header ──
    today = datetime.now()
    t = _t()

    # ── Sidebar Navigation ──
    _do_update = False
    _do_upload = False
    with st.sidebar:
        # Logo
        st.markdown(f'''
            <div style="padding:0 24px 32px;font-size:15px;font-weight:700;
                        text-transform:uppercase;letter-spacing:0.5px;color:{t['sidebar_text']};
                        font-family:{FONT};border-bottom:1px solid {t['sidebar_border']};
                        margin-bottom:16px">
                JS STUDIO
            </div>
        ''', unsafe_allow_html=True)

        # Update / Upload buttons
        st.markdown('<div class="sidebar-actions">', unsafe_allow_html=True)
        col_upd, col_upl = st.columns(2)
        with col_upd:
            _do_update = st.button("UPDATE", key="sidebar_update", use_container_width=True)
        with col_upl:
            _do_upload = st.button("UPLOAD", key="sidebar_upload", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # Section label
        st.markdown(f'''
            <div style="padding:20px 24px 6px;font-size:10px;text-transform:uppercase;
                        letter-spacing:1.5px;color:{t['sidebar_section']};font-family:{FONT};
                        font-weight:500">
                OVERVIEW
            </div>
        ''', unsafe_allow_html=True)

        # Nav items
        if 'active_page' not in st.session_state:
            st.session_state['active_page'] = 'Dashboard'

        OVERVIEW_PAGES = ['Dashboard', 'Expenses', 'Income', 'Goal Tracker', 'Taxes', '2025']
        CREATE_PAGES = ['New Offer', 'New Invoice']
        MANAGE_PAGES = ['Offers & Invoices', 'Clients']

        def _sidebar_nav_item(page_name, key_suffix=None):
            is_active = st.session_state['active_page'] == page_name
            container_class = 'nav-active' if is_active else 'nav-item'
            btn_key = f"nav_{key_suffix or page_name}"
            with st.container():
                st.markdown(f'<div class="{container_class}">', unsafe_allow_html=True)
                st.button(page_name, key=btn_key, use_container_width=True,
                          on_click=lambda p=page_name: st.session_state.update({'active_page': p}))
                st.markdown('</div>', unsafe_allow_html=True)

        # OVERVIEW nav items
        for page_name in OVERVIEW_PAGES:
            _sidebar_nav_item(page_name)

        # CREATE section
        st.markdown(f'''
            <div style="padding:20px 24px 6px;font-size:10px;text-transform:uppercase;
                        letter-spacing:1.5px;color:{t['sidebar_section']};font-family:{FONT};
                        font-weight:500">
                CREATE
            </div>
        ''', unsafe_allow_html=True)
        for page_name in CREATE_PAGES:
            _sidebar_nav_item(page_name)

        # MANAGE section
        st.markdown(f'''
            <div style="padding:20px 24px 6px;font-size:10px;text-transform:uppercase;
                        letter-spacing:1.5px;color:{t['sidebar_section']};font-family:{FONT};
                        font-weight:500">
                MANAGE
            </div>
        ''', unsafe_allow_html=True)
        for page_name in MANAGE_PAGES:
            _sidebar_nav_item(page_name)

        # Bottom section: theme toggle + date
        st.markdown(f'<div style="border-top:1px solid {t["sidebar_border"]};margin:20px 0 0 0;padding-top:12px"></div>', unsafe_allow_html=True)

        theme_label = "Light Mode" if st.session_state.get('theme', 'light') == 'dark' else "Dark Mode"
        def _toggle_theme():
            st.session_state['theme'] = 'dark' if st.session_state.get('theme', 'light') == 'light' else 'light'
        st.button(theme_label, key='sidebar_theme_toggle', use_container_width=True,
                  on_click=_toggle_theme)

        st.markdown(f'''
            <div style="padding:8px 24px;font-size:11px;color:{t['sidebar_text_dim']};font-family:{FONT}">
                {today.strftime('%d.%m.%Y')}
            </div>
        ''', unsafe_allow_html=True)

    # Handle button actions
    if _do_update:
        sync_invoices_dialog()
    if _do_upload:
        upload_expense_dialog()

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
        <div style="background:{t['surface3']};border:1px solid {t['border']};
                    border-radius:3px;padding:0.6rem 1rem;margin-bottom:0.75rem;
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

    # Route to active page
    active = st.session_state.get('active_page', 'Dashboard')
    if active == 'Dashboard':
        tab_overview(data)
    elif active == 'Expenses':
        tab_expenses(data)
    elif active == 'Income':
        tab_income(data)
    elif active == 'Goal Tracker':
        tab_goal(data)
    elif active == 'Taxes':
        tab_taxes(data)
    elif active == '2025':
        tab_2025(data)
    elif active == 'New Offer':
        tab_new_offer(data)
    elif active == 'New Invoice':
        tab_new_invoice(data)
    elif active == 'Offers & Invoices':
        tab_offers_invoices(data)
    elif active == 'Clients':
        tab_clients(data)

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
                    f'<span style="color:{_lt["text"]};font-weight:600">{entry["action"]}</span> '
                    f'<span>{entry["details"]}</span></div>'
                )
            log_html += '</div>'
            st.markdown(log_html, unsafe_allow_html=True)

    # ── Footer ──
    st.markdown(f"""
    <div class="js-footer">
        JOSEF SINDELKA — FINANCE DASHBOARD 2026 — DATA AUTO-REFRESHES EVERY 5 MIN
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
