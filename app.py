import streamlit as st
import pandas as pd
import plotly.express as px
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# ==========================================
# KONFIGURASI HALAMAN
# ==========================================
st.set_page_config(page_title="Shopee Charging Report", layout="wide", page_icon="📊")

# ==========================================
# KONFIGURASI ID FOLDER GDRIVE
# ==========================================
FOLDER_IDS = {
    "Bali": "1QyrDV3Hp3DDM_hGadpvlyjiDf9qFFj12",
    "Medan": "1rlaw2zcHmPWxXsNezT0qBrOy4lwJOUla",
    "Makassar": "1es6yRaVvXGt0Fs06jsx4-pj_UVjWSg-P",
    "Surabaya": "1WXRqjLiXk5P-BNozr_qgkRM09oRTQR1W",
    "Semarang": "13T9Wtw9qXaKTj52rsh9kdX-N9JIHCzzC"
}
OUTPUT_FOLDER_ID = "1FpQqUnBznK5OaNm6KQmBOhta7PKQu6Zt" 

# OPTIMASI: Menggunakan format CSV agar proses baca/tulis jauh lebih cepat
MASTER_FILENAME = "Master_Charging_Report.csv"

# ==========================================
# FUNGSI AUTENTIKASI GDRIVE
# ==========================================
@st.cache_resource
def get_gdrive_service():
    """Mengambil credentials dari Streamlit Secrets untuk akses GDrive"""
    creds_dict = st.secrets["gcp_service_account"]
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds)

# ==========================================
# FUNGSI ETL (EXTRACT, TRANSFORM, LOAD)
# ==========================================
def run_etl_process():
    drive_service = get_gdrive_service()
    all_data = []
    
    progress_text = "Memulai pengambilan data dari GDrive..."
    my_bar = st.progress(0, text=progress_text)
    
    total_folders = len(FOLDER_IDS)
    current_step = 0

    # 1. Extract & Transform
    for store_name, folder_id in FOLDER_IDS.items():
        current_step += 1
        my_bar.progress(current_step / (total_folders + 1), text=f"Mengambil data Cabang {store_name}...")
        
        # Cari file laporan mentah (format xlsx)
        query = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])
        
        for item in items:
            file_id = item['id']
            # Download file mentah
            request = drive_service.files().get_media(fileId=file_id)
            downloaded = io.BytesIO()
            
            # OPTIMASI: Chunksize 10MB untuk mempercepat download
            downloader = MediaIoBaseDownload(downloaded, request, chunksize=1024*1024*10)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            # Baca Excel file mentah
            downloaded.seek(0)
            try:
                # Hanya ambil sheet Summary
                df = pd.read_excel(downloaded, sheet_name='Charging Report Summary', engine='openpyxl')
                # Transformasi: Tambah Nama Store dan Periode
                df['Store'] = store_name
                df['Waktu Periode Dimulai'] = pd.to_datetime(df['Waktu Periode Dimulai'], errors='coerce')
                df['Periode Charging'] = df['Waktu Periode Dimulai'].dt.strftime('%B %Y')
                all_data.append(df)
            except Exception as e:
                # Abaikan jika ada error pembacaan sheet (misal file bukan format yang diharapkan)
                pass

    if not all_data:
        st.error("Tidak ada data yang berhasil diekstrak!")
        my_bar.empty()
        return False

    my_bar.progress(0.9, text="Menggabungkan dan menyimpan Master Data (CSV) ke GDrive...")
    
    # Gabungkan semua data
    master_df = pd.concat(all_data, ignore_index=True)
    
    # 2. Load (Simpan ke Gdrive sebagai CSV)
    output_bytes = io.BytesIO()
    master_df.to_csv(output_bytes, index=False)
    output_bytes.seek(0)
    
    # Cek apakah file master csv sudah ada sebelumnya di folder output
    query = f"'{OUTPUT_FOLDER_ID}' in parents and name='{MASTER_FILENAME}' and trashed=false"
    res = drive_service.files().list(q=query, fields="files(id)").execute()
    existing_files = res.get('files', [])
    
    # Pastikan mimetype di set text/csv
    media = MediaIoBaseUpload(output_bytes, mimetype='text/csv', resumable=True)
    
    if existing_files:
        # Update file existing
        file_id = existing_files[0]['id']
        drive_service.files().update(fileId=file_id, media_body=media).execute()
    else:
        # Buat file baru
        file_metadata = {'name': MASTER_FILENAME, 'parents': [OUTPUT_FOLDER_ID]}
        drive_service.files().create(body=file_metadata, media_body=media).execute()

    my_bar.progress(1.0, text="Proses Selesai!")
    my_bar.empty()
    return True

# ==========================================
# FUNGSI MEMBACA MASTER DATA (CSV) UNTUK DASHBOARD
# ==========================================
@st.cache_data(ttl=3600) # Cache 1 Jam
def load_master_data():
    drive_service = get_gdrive_service()
    query = f"'{OUTPUT_FOLDER_ID}' in parents and name='{MASTER_FILENAME}' and trashed=false"
    res = drive_service.files().list(q=query, fields="files(id)").execute()
    items = res.get('files', [])
    
    if not items:
        return pd.DataFrame() # Return empty dataframe jika belum ada file master
    
    file_id = items[0]['id']
    request = drive_service.files().get_media(fileId=file_id)
    downloaded = io.BytesIO()
    
    # OPTIMASI: Chunksize 10MB
    downloader = MediaIoBaseDownload(downloaded, request, chunksize=1024*1024*10)
    done = False
    while done is False:
        _, done = downloader.next_chunk()
        
    downloaded.seek(0)
    # OPTIMASI: Baca sebagai CSV
    df = pd.read_csv(downloaded)
    return df

# ==========================================
# UI DASHBOARD STREAMLIT
# ==========================================
st.title("📊 Dashboard Charging Report Shopee")

# Tombol untuk trigger ETL manual
col_title, col_btn = st.columns([3, 1])
with col_btn:
    st.write("") # Spasi
    if st.button("🔄 Tarik & Update Data GDrive", use_container_width=True):
        with st.spinner("Menjalankan proses sinkronisasi... Jangan tutup halaman."):
            success = run_etl_process()
            if success:
                st.success("Data berhasil diperbarui! Memuat ulang dashboard...")
                st.cache_data.clear() # Bersihkan cache agar data terbaru tampil
                st.rerun()

st.markdown("---")

# Load Data
df = load_master_data()

if df.empty:
    st.info("⚠️ Data Master belum tersedia. Silakan klik tombol 'Tarik & Update Data GDrive' di pojok kanan atas untuk menarik data pertama kali.")
else:
    # SIDEBAR FILTER
    st.sidebar.header("🔍 Filter Data")
    
    # Handle list filter (hilangkan NaN jika ada)
    periode_list = [p for p in df['Periode Charging'].dropna().unique()]
    store_list = [s for s in df['Store'].dropna().unique()]

    selected_periode = st.sidebar.multiselect("Pilih Periode", options=periode_list, default=periode_list)
    selected_store = st.sidebar.multiselect("Pilih Store", options=store_list, default=store_list)
    
    # Menerapkan Filter
    filtered_df = df[
        (df['Periode Charging'].isin(selected_periode)) & 
        (df['Store'].isin(selected_store))
    ]

    # KPI METRICS
    st.subheader("💡 Key Performance Indicators")
    col1, col2, col3, col4 = st.columns(4)
    
    total_sold = filtered_df['Total Order Sold Qty'].sum()
    total_before_tax = filtered_df['Total sebelum Pajak'].sum()
    total_after_tax = filtered_df['Total setelah Pajak'].sum()
    total_commission = filtered_df['Commission Fees'].sum()

    col1.metric("Total Order Sold Qty", f"{total_sold:,.0f}")
    col2.metric("Total Sebelum Pajak", f"Rp {total_before_tax:,.0f}")
    col3.metric("Total Setelah Pajak", f"Rp {total_after_tax:,.0f}")
    col4.metric("Total Commission Fees", f"Rp {total_commission:,.0f}")

    st.markdown("---")

    # CHARTS
    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        st.write("**Total Setelah Pajak per Store**")
        df_store_sales = filtered_df.groupby('Store')['Total setelah Pajak'].sum().reset_index()
        fig1 = px.bar(df_store_sales, x='Store', y='Total setelah Pajak', color='Store', text_auto='.2s')
        st.plotly_chart(fig1, use_container_width=True)

    with col_chart2:
        st.write("**Perbandingan Biaya (Commission vs Storage) per Store**")
        df_fees = filtered_df.groupby('Store')[['Commission Fees', 'Storage Fees']].sum().reset_index()
        fig2 = px.bar(df_fees, x='Store', y=['Commission Fees', 'Storage Fees'], barmode='group')
        st.plotly_chart(fig2, use_container_width=True)

    # RAW DATA
    st.markdown("---")
    st.subheader("📋 Rincian Data")
    st.dataframe(filtered_df, use_container_width=True)
