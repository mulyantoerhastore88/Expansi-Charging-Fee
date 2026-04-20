import streamlit as st
import pandas as pd
import plotly.express as px
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from datetime import datetime

# -------------------- CONFIG --------------------
st.set_page_config(page_title="Shopee Charging Report Dashboard", layout="wide")
st.title("📊 Shopee Charging Report Dashboard")

# Google Drive Setup
SCOPES = ['https://www.googleapis.com/auth/drive']

# Folder IDs untuk masing-masing store (sumber data)
FOLDER_IDS = {
    "Bali": "1QyrDV3Hp3DDM_hGadpvlyjiDf9qFFj12",
    "Medan": "1rlaw2zcHmPWxXsNezT0qBrOy4lwJOUla",
    "Makassar": "1es6yRaVvXGt0Fs06jsx4-pj_UVjWSg-P",
    "Surabaya": "1WXRqjLiXk5P-BNozr_qgkRM09oRTQR1W",
    "Semarang": "13T9Wtw9qXaKTj52rsh9kdX-N9JIHCzzC"
}

# Folder output (folder utama yang sama dengan folder sumber)
OUTPUT_FOLDER_ID = "1FpQqUnBznK5OaNm6KQmBOhta7PKQu6Zt"
MASTER_FILENAME = "Master_Charging_Report.csv"

# -------------------- AUTHENTICATION --------------------
@st.cache_resource
def get_drive_service():
    """Authenticate and return Google Drive service using service account."""
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=SCOPES
        )
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        st.error(f"❌ Gagal autentikasi Google Drive: {str(e)}")
        st.stop()

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

def load_master_csv(service):
    """Cek apakah Master CSV sudah ada di Drive, jika ada load sebagai DataFrame."""
    query = f"name='{MASTER_FILENAME}' and '{OUTPUT_FOLDER_ID}' in parents and trashed=false"
    results = service.files().list(
        q=query,
        fields="files(id, name, modifiedTime)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    files = results.get('files', [])
    
    if files:
        file_id = files[0]['id']
        file_bytes = download_file(service, file_id)
        modified_time = files[0]['modifiedTime']
        
        encodings_to_try = ['utf-8', 'utf-8-sig', 'latin1', 'iso-8859-1', 'cp1252']
        
        for encoding in encodings_to_try:
            try:
                file_bytes.seek(0)
                df = pd.read_csv(file_bytes, encoding=encoding)
                return df, modified_time
            except UnicodeDecodeError:
                continue
            except Exception:
                continue
        
        st.error("❌ Gagal membaca file CSV.")
        return None, None
    
    return None, None

def compile_all_reports(service, force_refresh=False):
    """Baca semua file dari semua store, compile menjadi satu DataFrame."""
    if not force_refresh:
        cached_df, modified_time = load_master_csv(service)
        if cached_df is not None:
            st.info(f"📦 Menggunakan data cache dari {modified_time}")
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

def save_master_csv(service, df):
    """
    Simpan DataFrame sebagai CSV ke folder yang sudah di-share.
    File akan menggunakan quota storage pemilik folder (Anda).
    """
    # Konversi DataFrame ke CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = io.BytesIO(csv_buffer.getvalue().encode('utf-8-sig'))
    
    # Cek apakah file sudah ada di folder tujuan
    query = f"name='{MASTER_FILENAME}' and '{OUTPUT_FOLDER_ID}' in parents and trashed=false"
    results = service.files().list(
        q=query,
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    existing_files = results.get('files', [])
    is_update = len(existing_files) > 0
    
    if existing_files:
        # Update file existing
        file_id = existing_files[0]['id']
        
        media = MediaIoBaseUpload(
            csv_bytes,
            mimetype='text/csv',
            resumable=True
        )
        
        service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        
        return file_id, is_update
    else:
        # Buat file baru langsung di folder tujuan
        file_metadata = {
            'name': MASTER_FILENAME,
            'parents': [OUTPUT_FOLDER_ID],
            'mimeType': 'text/csv'
        }
        
        media = MediaIoBaseUpload(
            csv_bytes,
            mimetype='text/csv',
            resumable=True
        )
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True
        ).execute()
        
        return file.get('id'), is_update

def format_rupiah(value):
    """Format angka ke format Rupiah."""
    try:
        return f"Rp {float(value):,.0f}"
    except:
        return "Rp 0"

# -------------------- MAIN APP --------------------
if 'compiled_df' not in st.session_state:
    st.session_state.compiled_df = None
if 'last_update' not in st.session_state:
    st.session_state.last_update = None

# Sidebar
st.sidebar.header("⚙️ Kontrol")
action = st.sidebar.radio(
    "📌 Pilih Aksi",
    ["📥 Load & Compile Data", "📊 Lihat Dashboard", "💾 Simpan ke Drive (CSV)"]
)

# Service account authentication
service = get_drive_service()

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
            compiled_df, cache_time = compile_all_reports(service, force_refresh=force_refresh)
            
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
        with st.spinner("📦 Mencoba load data dari cache..."):
            compiled_df, cache_time = compile_all_reports(service, force_refresh=False)
            if not compiled_df.empty:
                st.session_state.compiled_df = compiled_df
                st.info(f"📦 Data dimuat dari cache ({cache_time})")
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
    
    # Metrics
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
    
    # Charts
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
    
    # Download button
    csv = df_filtered.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="📥 Download Data Filtered (CSV)",
        data=csv,
        file_name=f"charging_report_filtered_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

# -------------------- SAVE TO DRIVE --------------------
elif action == "💾 Simpan ke Drive (CSV)":
    st.header("💾 Simpan Hasil Compile ke Google Drive")
    
    if st.session_state.compiled_df is None:
        st.warning("⚠️ Tidak ada data untuk disimpan. Silakan Load & Compile terlebih dahulu.")
    else:
        df = st.session_state.compiled_df
        st.info(f"📊 Data yang akan disimpan: **{len(df):,} baris**, **{df['Store'].nunique()} store**")
        st.text(f"📁 Lokasi: Folder ID `{OUTPUT_FOLDER_ID}`")
        st.text(f"📄 Nama file: `{MASTER_FILENAME}`")
        
        if st.button("📤 Simpan ke Google Drive", type="primary", use_container_width=True):
            with st.spinner("🔄 Menyimpan file ke Google Drive..."):
                try:
                    file_id, is_update = save_master_csv(service, df)
                    
                    if is_update:
                        st.success(f"✅ File berhasil di-update!")
                    else:
                        st.success(f"✅ File baru berhasil dibuat!")
                    
                    st.markdown(f"[📁 Buka di Google Drive](https://drive.google.com/file/d/{file_id}/view)")
                    st.markdown(f"[📂 Buka Folder Utama](https://drive.google.com/drive/folders/{OUTPUT_FOLDER_ID})")
                    
                    st.session_state.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                except Exception as e:
                    error_msg = str(e)
                    st.error(f"❌ Gagal menyimpan file: {error_msg}")
                    
                    # Cek apakah error karena quota
                    if "storageQuotaExceeded" in error_msg or "storage quota" in error_msg.lower():
                        st.warning("""
                        **Service Account tidak memiliki storage quota.**
                        
                        Solusi:
                        1. Pastikan folder tujuan sudah di-**share ke Service Account** dengan akses **Editor**
                        2. Atau download manual menggunakan tombol di bawah
                        """)
                    
                    # Fallback download
                    csv_download = df.to_csv(index=False).encode('utf-8-sig')
                    st.download_button(
                        label="📥 Download CSV (Manual)",
                        data=csv_download,
                        file_name=MASTER_FILENAME,
                        mime="text/csv"
                    )

# Footer
st.sidebar.divider()
if st.session_state.last_update:
    st.sidebar.caption(f"🕒 Data terakhir di-load: {st.session_state.last_update}")
if st.session_state.compiled_df is not None:
    st.sidebar.caption(f"📊 {len(st.session_state.compiled_df):,} baris di memory")
