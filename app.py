import streamlit as st
import pandas as pd
import plotly.express as px
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime
import gspread

# -------------------- CONFIG --------------------
st.set_page_config(page_title="Shopee Charging Report Dashboard", layout="wide")
st.title("📊 Shopee Charging Report Dashboard")

# Google Drive & Sheets Setup
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
]

# Folder IDs untuk masing-masing store (sumber data)
FOLDER_IDS = {
    "Shopee Bali": "1QyrDV3Hp3DDM_hGadpvlyjiDf9qFFj12",
    "Shopee Medan": "1rlaw2zcHmPWxXsNezT0qBrOy4lwJOUla",
    "Shopee Makassar": "1es6yRaVvXGt0Fs06jsx4-pj_UVjWSg-P",
    "Shopee Surabaya": "1WXRqjLiXk5P-BNozr_qgkRM09oRTQR1W",
    "Shopee Semarang": "13T9Wtw9qXaKTj52rsh9kdX-N9JIHCzzC"
}

# Google Sheets Config
GOOGLE_SHEET_ID = "1KfSLfk9lkTzJhpkEpo98SBGvsi3G0R0GcM_-aWgjSh8"
SHEET_NAME = "Master_Charging_Report"

# -------------------- AUTHENTICATION --------------------
@st.cache_resource
def get_credentials():
    """Get credentials from secrets."""
    return service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES
    )

@st.cache_resource
def get_drive_service():
    """Authenticate and return Google Drive service."""
    credentials = get_credentials()
    return build('drive', 'v3', credentials=credentials)

@st.cache_resource
def get_gsheet_client():
    """Authenticate and return gspread client."""
    credentials = get_credentials()
    return gspread.authorize(credentials)

# -------------------- HELPER FUNCTIONS --------------------
def list_excel_files_in_folder(service, folder_id):
    """List semua file Excel (.xlsx) di dalam folder."""
    query = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false"
    results = service.files().list(
        q=query,
        fields="files(id, name)",
        pageSize=1000,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    return results.get('files', [])

def download_file(service, file_id):
    """Download file from Google Drive and return as BytesIO."""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh

def process_excel(file_bytes, store_name, file_name):
    """Extract data from 'Charging Report Summary' sheet."""
    try:
        df = pd.read_excel(file_bytes, sheet_name='Charging Report Summary', header=0)
        df = df[df['CRT ID'].notna()]
        
        if df.empty:
            return pd.DataFrame()
        
        df['Store'] = store_name
        df['Source File'] = file_name
        
        if 'Waktu Periode Dimulai' in df.columns:
            df['Periode'] = pd.to_datetime(df['Waktu Periode Dimulai']).dt.to_period('M').astype(str)
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Error processing {store_name}/{file_name}: {str(e)}")
        return pd.DataFrame()

def load_from_gsheet(client):
    """Load data dari Google Sheets."""
    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sheet.worksheet(SHEET_NAME)
        data = worksheet.get_all_records()
        
        if data:
            df = pd.DataFrame(data)
            
            # Convert numeric columns
            numeric_columns = [
                'Total Order Sold Qty', 'Total MTSKU Sold Qty', 'Total sebelum Pajak', 'Pajak',
                'Total setelah Pajak', 'Amount after tax (Confirmed)', 'Commission Fees',
                'Commission Fees (Confirmed)', 'Storage Fees', 'Storage Fees (Confirmed)',
                'Warehouse Handling Fees', 'Warehouse Handling Fees (Confirmed)', 'Logistics Fees',
                'Logistics Fees (Confirmed)', 'Inbound Penalty Fees', 'Inbound Penalty Fees (Confirmed)',
                'Other Fees', 'Other Fees (Confirmed)', 'Settlement Amount'
            ]
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Get last modified info
            try:
                last_updated = worksheet.acell('A1').value
                if last_updated and last_updated.startswith('Last Updated:'):
                    modified_time = last_updated.replace('Last Updated: ', '')
                else:
                    modified_time = "Unknown"
            except:
                modified_time = "Unknown"
            
            return df, modified_time
        else:
            return None, None
    except gspread.exceptions.WorksheetNotFound:
        return None, None
    except Exception as e:
        st.warning(f"⚠️ Gagal load dari Google Sheets: {str(e)}")
        return None, None

def compile_all_reports(service, client, force_refresh=False):
    """Baca semua file dari semua store, compile menjadi satu DataFrame."""
    if not force_refresh:
        cached_df, modified_time = load_from_gsheet(client)
        if cached_df is not None and not cached_df.empty:
            st.info(f"📦 Menggunakan data cache dari Google Sheets (Last Update: {modified_time})")
            return cached_df, modified_time
    
    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    total_files = 0
    processed_files = 0
    
    for store_name, folder_id in FOLDER_IDS.items():
        files = list_excel_files_in_folder(service, folder_id)
        total_files += len(files)
    
    if total_files == 0:
        status_text.text("⚠️ Tidak ada file Excel ditemukan.")
        progress_bar.empty()
        return pd.DataFrame(), None
    
    for store_name, folder_id in FOLDER_IDS.items():
        status_text.text(f"📂 Memproses store: {store_name}...")
        files = list_excel_files_in_folder(service, folder_id)
        
        for file in files:
            try:
                file_bytes = download_file(service, file['id'])
                df = process_excel(file_bytes, store_name, file['name'])
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                st.warning(f"⚠️ Gagal download {store_name}/{file['name']}: {str(e)}")
            
            processed_files += 1
            progress_bar.progress(processed_files / total_files)
    
    status_text.text(f"✅ Selesai memproses {total_files} file!")
    progress_bar.empty()
    
    if all_data:
        compiled_df = pd.concat(all_data, ignore_index=True)
        
        numeric_columns = [
            'Total Order Sold Qty', 'Total MTSKU Sold Qty', 'Total sebelum Pajak', 'Pajak',
            'Total setelah Pajak', 'Amount after tax (Confirmed)', 'Commission Fees',
            'Commission Fees (Confirmed)', 'Storage Fees', 'Storage Fees (Confirmed)',
            'Warehouse Handling Fees', 'Warehouse Handling Fees (Confirmed)', 'Logistics Fees',
            'Logistics Fees (Confirmed)', 'Inbound Penalty Fees', 'Inbound Penalty Fees (Confirmed)',
            'Other Fees', 'Other Fees (Confirmed)', 'Settlement Amount'
        ]
        for col in numeric_columns:
            if col in compiled_df.columns:
                compiled_df[col] = pd.to_numeric(compiled_df[col], errors='coerce')
        
        return compiled_df, None
    else:
        return pd.DataFrame(), None

def save_to_gsheet(client, df):
    """Simpan DataFrame ke Google Sheets."""
    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        
        # Coba dapatkan worksheet, buat jika belum ada
        try:
            worksheet = sheet.worksheet(SHEET_NAME)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(
                title=SHEET_NAME,
                rows=max(1000, len(df)+10),
                cols=max(30, len(df.columns)+5)
            )
        
        # Update timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        worksheet.update('A1', [[f'Last Updated: {timestamp}']], value_input_option='USER_ENTERED')
        
        # Siapkan data
        df_clean = df.fillna('')
        headers = df_clean.columns.tolist()
        
        data = [headers]
        for _, row in df_clean.iterrows():
            row_list = [str(val) if str(val) != 'nan' else '' for val in row.tolist()]
            data.append(row_list)
        
        # Update data
        if data:
            worksheet.update('A2', data, value_input_option='USER_ENTERED')
        
        # Format header
        if headers:
            end_col_letter = chr(65 + min(len(headers), 26) - 1) if len(headers) <= 26 else 'Z'
            worksheet.format(f'A2:{end_col_letter}2', {"textFormat": {"bold": True}})
        
        return True, timestamp
    except Exception as e:
        raise e

def format_rupiah(value):
    try:
        return f"Rp {float(value):,.0f}"
    except:
        return "Rp 0"

# -------------------- MAIN APP --------------------
if 'compiled_df' not in st.session_state:
    st.session_state.compiled_df = None
if 'last_update' not in st.session_state:
    st.session_state.last_update = None

st.sidebar.header("⚙️ Kontrol")
action = st.sidebar.radio(
    "📌 Pilih Aksi",
    ["📥 Load & Compile Data", "📊 Lihat Dashboard", "💾 Simpan ke Google Sheets"]
)

try:
    service = get_drive_service()
    gsheet_client = get_gsheet_client()
except Exception as e:
    st.error(f"❌ Gagal autentikasi: {str(e)}")
    st.stop()

# -------------------- LOAD & COMPILE --------------------
if action == "📥 Load & Compile Data":
    st.header("📥 Load & Compile Data dari Google Drive")
    
    col1, col2 = st.columns([2, 1])
    with col1:
        force_refresh = st.checkbox("🔄 Force Refresh (abaikan cache)", value=False)
    with col2:
        if st.session_state.compiled_df is not None:
            st.metric("Data di Memory", f"{len(st.session_state.compiled_df):,} baris")
    
    if st.button("🚀 Mulai Compile Semua Report", type="primary", use_container_width=True):
        with st.spinner("🔄 Membaca dan memproses semua file Excel..."):
            compiled_df, cache_time = compile_all_reports(service, gsheet_client, force_refresh=force_refresh)
            
            if not compiled_df.empty:
                st.session_state.compiled_df = compiled_df
                st.session_state.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                st.success(f"✅ Berhasil compile {len(compiled_df):,} baris data!")
                st.subheader("📋 Preview Data Hasil Compile")
                st.dataframe(compiled_df.head(10), use_container_width=True)
                
                st.subheader("📊 Ringkasan Dataset")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Baris", f"{len(compiled_df):,}")
                with col2:
                    st.metric("Total Store", compiled_df['Store'].nunique())
                with col3:
                    st.metric("Total File", compiled_df['Source File'].nunique())
                with col4:
                    if 'Periode' in compiled_df.columns:
                        st.metric("Periode", ", ".join(compiled_df['Periode'].unique()))
                
                st.subheader("🏪 Data per Store")
                store_summary = compiled_df.groupby('Store').agg({
                    'Total setelah Pajak': 'sum',
                    'Settlement Amount': 'sum',
                    'Source File': 'nunique'
                }).reset_index()
                store_summary.columns = ['Store', 'Total Setelah Pajak', 'Settlement Amount', 'Jumlah File']
                
                store_summary['Total Setelah Pajak'] = store_summary['Total Setelah Pajak'].apply(format_rupiah)
                store_summary['Settlement Amount'] = store_summary['Settlement Amount'].apply(format_rupiah)
                
                st.dataframe(store_summary, use_container_width=True)
            else:
                st.warning("⚠️ Tidak ada data yang berhasil di-compile.")

# -------------------- DASHBOARD --------------------
elif action == "📊 Lihat Dashboard":
    st.header("📊 Dashboard Charging Report")
    
    if st.session_state.compiled_df is None:
        with st.spinner("📦 Mencoba load data dari Google Sheets..."):
            compiled_df, cache_time = compile_all_reports(service, gsheet_client, force_refresh=False)
            if not compiled_df.empty:
                st.session_state.compiled_df = compiled_df
                st.info(f"📦 Data dimuat dari Google Sheets (Last Update: {cache_time})")
            else:
                st.warning("⚠️ Tidak ada data. Silakan Load & Compile terlebih dahulu.")
                st.stop()
    
    df = st.session_state.compiled_df.copy()
    
    st.sidebar.subheader("🔍 Filter Data")
    
    stores = st.sidebar.multiselect(
        "Pilih Store",
        options=sorted(df['Store'].unique()),
        default=sorted(df['Store'].unique())
    )
    
    if 'Periode' in df.columns:
        periods = st.sidebar.multiselect(
            "Pilih Periode",
            options=sorted(df['Periode'].unique()),
            default=sorted(df['Periode'].unique())
        )
        df_filtered = df[df['Store'].isin(stores) & df['Periode'].isin(periods)]
    else:
        df_filtered = df[df['Store'].isin(stores)]
    
    if df_filtered.empty:
        st.warning("⚠️ Tidak ada data dengan filter yang dipilih.")
        st.stop()
    
    st.caption(f"Menampilkan {len(df_filtered):,} baris data")
    
    st.subheader("💰 Ringkasan Keuangan")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_sales = df_filtered['Total setelah Pajak'].sum() if 'Total setelah Pajak' in df_filtered.columns else 0
        st.metric("Total Setelah Pajak", format_rupiah(total_sales))
    
    with col2:
        total_commission = df_filtered['Commission Fees (Confirmed)'].sum() if 'Commission Fees (Confirmed)' in df_filtered.columns else 0
        st.metric("Commission Fees", format_rupiah(total_commission))
    
    with col3:
        total_settlement = df_filtered['Settlement Amount'].sum() if 'Settlement Amount' in df_filtered.columns else 0
        st.metric("Settlement Amount", format_rupiah(total_settlement))
    
    with col4:
        total_orders = df_filtered['Total Order Sold Qty'].sum() if 'Total Order Sold Qty' in df_filtered.columns else 0
        st.metric("Total Order", f"{total_orders:,.0f}")
    
    st.subheader("📈 Analisis per Store")
    col1, col2 = st.columns(2)
    
    with col1:
        if 'Total setelah Pajak' in df_filtered.columns:
            store_total = df_filtered.groupby('Store')['Total setelah Pajak'].sum().reset_index()
            fig1 = px.bar(store_total, x='Store', y='Total setelah Pajak', title="Total Setelah Pajak per Store", color='Store', text_auto='.2s')
            fig1.update_layout(showlegend=False)
            st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        if 'Settlement Amount' in df_filtered.columns:
            store_settlement = df_filtered.groupby('Store')['Settlement Amount'].sum().reset_index()
            fig2 = px.bar(store_settlement, x='Store', y='Settlement Amount', title="Settlement Amount per Store", color='Store', text_auto='.2s')
            fig2.update_layout(showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)
    
    st.markdown(f"📊 [Buka Data Lengkap di Google Sheets](https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit)")
    
    csv = df_filtered.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="📥 Download Data Filtered (CSV)",
        data=csv,
        file_name=f"charging_report_filtered_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

# -------------------- SAVE TO GOOGLE SHEETS --------------------
elif action == "💾 Simpan ke Google Sheets":
    st.header("💾 Simpan Hasil Compile ke Google Sheets")
    
    if st.session_state.compiled_df is None:
        st.warning("⚠️ Tidak ada data untuk disimpan. Silakan Load & Compile terlebih dahulu.")
    else:
        df = st.session_state.compiled_df
        st.info(f"📊 Data yang akan disimpan: **{len(df):,} baris**, **{df['Store'].nunique()} store**")
        st.text(f"📁 Google Sheet ID: `{GOOGLE_SHEET_ID}`")
        st.text(f"📄 Sheet Name: `{SHEET_NAME}`")
        
        st.markdown(f"[📊 Buka Google Sheets](https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit)")
        
        if st.button("📤 Simpan ke Google Sheets", type="primary", use_container_width=True):
            with st.spinner("🔄 Menyimpan data ke Google Sheets..."):
                try:
                    success, timestamp = save_to_gsheet(gsheet_client, df)
                    
                    if success:
                        st.success(f"✅ Data berhasil disimpan ke Google Sheets!")
                        st.info(f"🕒 Last Updated: {timestamp}")
                        st.markdown(f"[📊 Lihat Hasil di Google Sheets](https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit)")
                        st.session_state.last_update = timestamp
                        
                except Exception as e:
                    st.error(f"❌ Gagal menyimpan ke Google Sheets: {str(e)}")
                    
                    with st.expander("Detail Error"):
                        st.code(str(e))
                    
                    st.warning("Silakan download manual sebagai alternatif:")
                    csv_download = df.to_csv(index=False).encode('utf-8-sig')
                    st.download_button(
                        label="📥 Download CSV (Manual)",
                        data=csv_download,
                        file_name="Master_Charging_Report.csv",
                        mime="text/csv"
                    )

# Footer
st.sidebar.divider()
if st.session_state.last_update:
    st.sidebar.caption(f"🕒 Data terakhir di-load: {st.session_state.last_update}")
if st.session_state.compiled_df is not None:
    st.sidebar.caption(f"📊 {len(st.session_state.compiled_df):,} baris di memory")

st.sidebar.divider()
st.sidebar.caption("📌 Sheet yang dibaca: Charging Report Summary")
st.sidebar.caption(f"📁 Total folder store: {len(FOLDER_IDS)}")
st.sidebar.caption(f"📊 Output: Google Sheets")
