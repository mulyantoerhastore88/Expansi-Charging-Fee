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

# Folder IDs
FOLDER_IDS = {
    "Bali": "1QyrDV3Hp3DDM_hGadpvlyjiDf9qFFj12",
    "Medan": "1rlaw2zcHmPWxXsNezT0qBrOy4lwJOUla",
    "Makassar": "1es6yRaVvXGt0Fs06jsx4-pj_UVjWSg-P",
    "Surabaya": "1WXRqjLiXk5P-BNozr_qgkRM09oRTQR1W",
    "Semarang": "13T9Wtw9qXaKTj52rsh9kdX-N9JIHCzzC"
}

OUTPUT_FOLDER_ID = "1FpQqUnBznK5OaNm6KQmBOhta7PKQu6Zt"
MASTER_FILENAME = "Master_Charging_Report.csv"

# -------------------- AUTHENTICATION --------------------
@st.cache_resource
def get_drive_service():
    """Authenticate and return Google Drive service using service account."""
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES
    )
    return build('drive', 'v3', credentials=credentials)

# -------------------- HELPER FUNCTIONS --------------------
def list_excel_files_in_folder(service, folder_id):
    """List semua file Excel (.xlsx) di dalam folder."""
    query = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    results = service.files().list(q=query, fields="files(id, name)", pageSize=1000).execute()
    return results.get('files', [])

def download_excel(service, file_id):
    """Download Excel file from Google Drive and return as BytesIO."""
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
        
        # Filter hanya data row (bukan header kosong) - cek CRT ID tidak null
        df = df[df['CRT ID'].notna()]
        
        if df.empty:
            return pd.DataFrame()
        
        # Tambah kolom informasi
        df['Store'] = store_name
        df['Source File'] = file_name
        
        # Parse periode dari Waktu Periode Dimulai
        if 'Waktu Periode Dimulai' in df.columns:
            df['Periode'] = pd.to_datetime(df['Waktu Periode Dimulai']).dt.to_period('M').astype(str)
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Error processing {store_name}/{file_name}: {str(e)}")
        return pd.DataFrame()

def load_master_csv(service):
    """Cek apakah Master CSV sudah ada di Drive, jika ada load sebagai DataFrame."""
    query = f"name='{MASTER_FILENAME}' and '{OUTPUT_FOLDER_ID}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
    files = results.get('files', [])
    
    if files:
        file_id = files[0]['id']
        file_bytes = download_excel(service, file_id)  # Meskipun CSV, download_excel bisa handle
        df = pd.read_csv(file_bytes)
        modified_time = files[0]['modifiedTime']
        return df, modified_time
    return None, None

def compile_all_reports(service, force_refresh=False):
    """
    Baca semua file dari semua store, compile menjadi satu DataFrame.
    Jika force_refresh=False dan master CSV sudah ada, gunakan cache.
    """
    if not force_refresh:
        cached_df, modified_time = load_master_csv(service)
        if cached_df is not None:
            st.info(f"📦 Menggunakan data cache dari {modified_time}")
            return cached_df, modified_time
    
    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    total_stores = len(FOLDER_IDS)
    total_files = 0
    processed_files = 0
    
    # Hitung total file dulu untuk progress
    for store_name, folder_id in FOLDER_IDS.items():
        files = list_excel_files_in_folder(service, folder_id)
        total_files += len(files)
    
    for idx, (store_name, folder_id) in enumerate(FOLDER_IDS.items()):
        status_text.text(f"📂 Memproses store: {store_name}...")
        
        files = list_excel_files_in_folder(service, folder_id)
        
        for file in files:
            file_bytes = download_excel(service, file['id'])
            df = process_excel(file_bytes, store_name, file['name'])
            if not df.empty:
                all_data.append(df)
            
            processed_files += 1
            progress_bar.progress(processed_files / total_files)
    
    status_text.text(f"✅ Selesai memproses {total_files} file dari {total_stores} store!")
    progress_bar.empty()
    
    if all_data:
        compiled_df = pd.concat(all_data, ignore_index=True)
        
        # Konversi kolom numerik ke tipe yang sesuai
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
    """Simpan DataFrame sebagai CSV ke Google Drive (overwrite jika sudah ada)."""
    # Cek file existing
    query = f"name='{MASTER_FILENAME}' and '{OUTPUT_FOLDER_ID}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    existing_files = results.get('files', [])
    
    # Konversi DataFrame ke CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
    
    if existing_files:
        # Update file existing
        file_id = existing_files[0]['id']
        media = MediaIoBaseUpload(
            csv_bytes,
            mimetype='text/csv',
            resumable=True
        )
        service.files().update(fileId=file_id, media_body=media).execute()
        return file_id, True
    else:
        # Buat file baru
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
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id'), False

# -------------------- MAIN APP --------------------
# Inisialisasi session state
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
try:
    service = get_drive_service()
except Exception as e:
    st.error(f"❌ Gagal autentikasi Google Drive. Periksa secrets.\nError: {e}")
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
            compiled_df, cache_time = compile_all_reports(service, force_refresh=force_refresh)
            
            if not compiled_df.empty:
                st.session_state.compiled_df = compiled_df
                st.session_state.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                st.success(f"✅ Berhasil compile {len(compiled_df):,} baris data!")
                
                # Info ringkasan
                st.subheader("📋 Preview Data Hasil Compile")
                st.dataframe(compiled_df.head(10), use_container_width=True)
                
                # Metrics
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
                
                # Tampilkan sample per store
                st.subheader("🏪 Data per Store")
                store_summary = compiled_df.groupby('Store').agg({
                    'Total setelah Pajak': 'sum',
                    'Settlement Amount': 'sum',
                    'Source File': 'nunique'
                }).reset_index()
                store_summary.columns = ['Store', 'Total Setelah Pajak', 'Settlement Amount', 'Jumlah File']
                st.dataframe(store_summary, use_container_width=True)
                
            else:
                st.warning("⚠️ Tidak ada data yang berhasil di-compile.")

# -------------------- DASHBOARD --------------------
elif action == "📊 Lihat Dashboard":
    st.header("📊 Dashboard Charging Report")
    
    # Cek apakah ada data di session state, jika tidak coba load dari cache
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
    
    # Sidebar Filters
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
    
    # Metrics Cards
    st.subheader("💰 Ringkasan Keuangan")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_sales = df_filtered['Total setelah Pajak'].sum()
        st.metric("Total Setelah Pajak", f"Rp {total_sales:,.0f}")
    
    with col2:
        total_commission = df_filtered['Commission Fees (Confirmed)'].sum()
        st.metric("Commission Fees", f"Rp {total_commission:,.0f}")
    
    with col3:
        total_settlement = df_filtered['Settlement Amount'].sum()
        st.metric("Settlement Amount", f"Rp {total_settlement:,.0f}")
    
    with col4:
        total_orders = df_filtered['Total Order Sold Qty'].sum()
        st.metric("Total Order", f"{total_orders:,.0f}")
    
    # Charts Row 1
    st.subheader("📈 Analisis per Store")
    col1, col2 = st.columns(2)
    
    with col1:
        # Total per Store
        store_total = df_filtered.groupby('Store')['Total setelah Pajak'].sum().reset_index()
        fig1 = px.bar(
            store_total,
            x='Store',
            y='Total setelah Pajak',
            title="Total Setelah Pajak per Store",
            color='Store',
            text_auto='.2s'
        )
        fig1.update_layout(showlegend=False)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Settlement per Store
        store_settlement = df_filtered.groupby('Store')['Settlement Amount'].sum().reset_index()
        fig2 = px.bar(
            store_settlement,
            x='Store',
            y='Settlement Amount',
            title="Settlement Amount per Store",
            color='Store',
            text_auto='.2s'
        )
        fig2.update_layout(showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)
    
    # Charts Row 2
    st.subheader("🥧 Komposisi & Proporsi")
    col1, col2 = st.columns(2)
    
    with col1:
        # Pie Chart: Komposisi Biaya
        fee_data = pd.DataFrame({
            'Jenis Biaya': ['Commission', 'Storage', 'Logistics', 'Warehouse', 'Inbound Penalty', 'Other'],
            'Nilai': [
                df_filtered['Commission Fees (Confirmed)'].sum(),
                df_filtered['Storage Fees (Confirmed)'].sum(),
                df_filtered['Logistics Fees (Confirmed)'].sum(),
                df_filtered['Warehouse Handling Fees (Confirmed)'].sum(),
                df_filtered['Inbound Penalty Fees (Confirmed)'].sum(),
                df_filtered['Other Fees (Confirmed)'].sum()
            ]
        })
        fee_data = fee_data[fee_data['Nilai'] > 0]
        
        if not fee_data.empty:
            fig3 = px.pie(
                fee_data,
                values='Nilai',
                names='Jenis Biaya',
                title="Komposisi Biaya (Confirmed)"
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Tidak ada data biaya")
    
    with col2:
        # Pie Chart: Order per Store
        order_per_store = df_filtered.groupby('Store')['Total Order Sold Qty'].sum().reset_index()
        fig4 = px.pie(
            order_per_store,
            values='Total Order Sold Qty',
            names='Store',
            title="Proporsi Order per Store"
        )
        st.plotly_chart(fig4, use_container_width=True)
    
    # Tabel Detail
    st.subheader("📋 Data Detail")
    
    # Pilih kolom yang akan ditampilkan
    display_columns = [
        'Store', 'ID Toko', 'Waktu Periode Dimulai', 'Waktu Periode Berakhir',
        'Total Order Sold Qty', 'Total setelah Pajak', 'Commission Fees (Confirmed)',
        'Storage Fees (Confirmed)', 'Settlement Amount', 'Source File'
    ]
    available_columns = [col for col in display_columns if col in df_filtered.columns]
    
    st.dataframe(
        df_filtered[available_columns].sort_values(['Store', 'Waktu Periode Dimulai']),
        use_container_width=True,
        hide_index=True
    )
    
    # Download button
    csv = df_filtered.to_csv(index=False).encode('utf-8')
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
                    # Konversi DataFrame ke CSV
                    csv_buffer = io.StringIO()
                    df.to_csv(csv_buffer, index=False)
                    csv_bytes = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
                    
                    # Step 1: Upload ke root Drive Service Account
                    file_metadata = {
                        'name': MASTER_FILENAME,
                        'mimeType': 'text/csv'
                    }
                    
                    media = MediaIoBaseUpload(
                        csv_bytes,
                        mimetype='text/csv',
                        resumable=True
                    )
                    
                    new_file = service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id, name'
                    ).execute()
                    
                    new_file_id = new_file['id']
                    
                    # Step 2: Cek file lama di folder tujuan
                    query = f"name='{MASTER_FILENAME}' and '{OUTPUT_FOLDER_ID}' in parents and trashed=false"
                    results = service.files().list(q=query, fields="files(id)").execute()
                    existing_files = results.get('files', [])
                    
                    is_update = len(existing_files) > 0
                    
                    # Step 3: Pindahkan file baru ke folder tujuan
                    service.files().update(
                        fileId=new_file_id,
                        addParents=OUTPUT_FOLDER_ID,
                        removeParents='root',
                        fields='id, parents'
                    ).execute()
                    
                    # Step 4: Hapus file lama
                    for old_file in existing_files:
                        try:
                            service.files().delete(fileId=old_file['id']).execute()
                        except:
                            pass
                    
                    if is_update:
                        st.success(f"✅ File berhasil di-update!")
                    else:
                        st.success(f"✅ File baru berhasil dibuat!")
                    
                    st.markdown(f"[📁 Buka di Google Drive](https://drive.google.com/file/d/{new_file_id}/view)")
                    st.markdown(f"[📂 Buka Folder Utama](https://drive.google.com/drive/folders/{OUTPUT_FOLDER_ID})")
                    
                    st.session_state.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                except Exception as e:
                    st.error(f"❌ Gagal menyimpan file: {str(e)}")
                    st.info("""
                    **Solusi alternatif jika error berlanjut:**
                    1. Gunakan Shared Drive (Google Drive bersama)
                    2. Atau download CSV secara manual dan upload ke Drive
                    """)
                    
                    # Fallback: Tawarkan download manual
                    csv_download = df.to_csv(index=False).encode('utf-8')
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
