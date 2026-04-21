import streamlit as st
import pandas as pd
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

FOLDER_IDS = {
    "Shopee Bali": "1QyrDV3Hp3DDM_hGadpvlyjiDf9qFFj12",
    "Shopee Medan": "1rlaw2zcHmPWxXsNezT0qBrOy4lwJOUla",
    "Shopee Makassar": "1es6yRaVvXGt0Fs06jsx4-pj_UVjWSg-P",
    "Shopee Surabaya": "1WXRqjLiXk5P-BNozr_qgkRM09oRTQR1W",
    "Shopee Semarang": "13T9Wtw9qXaKTj52rsh9kdX-N9JIHCzzC"
}

GOOGLE_SHEET_ID = "1KfSLfk9lkTzJhpkEpo98SBGvsi3G0R0GcM_-aWgjSh8"
SHEET_MASTER = "Master_Charging_Report"
SHEET_GMV = "Order GMV"
SHEET_QTY = "Order Qty"

MONTH_ORDER = ["Jan 26", "Feb 26", "Mar 26", "Apr 26", "May 26", "Jun 26"]

# -------------------- AUTHENTICATION --------------------
@st.cache_resource
def get_credentials():
    return service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES
    )

@st.cache_resource
def get_drive_service():
    return build('drive', 'v3', credentials=get_credentials())

@st.cache_resource
def get_gsheet_client():
    return gspread.authorize(get_credentials())

# -------------------- HELPER FUNCTIONS --------------------
def list_excel_files_in_folder(service, folder_id):
    query = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false"
    results = service.files().list(
        q=query, fields="files(id, name)", pageSize=1000,
        supportsAllDrives=True, includeItemsFromAllDrives=True
    ).execute()
    return results.get('files', [])

def download_file(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh

def process_excel(file_bytes, store_name, file_name):
    try:
        df = pd.read_excel(file_bytes, sheet_name='Charging Report Summary', header=0)
        df = df[df['CRT ID'].notna()]
        if df.empty:
            return pd.DataFrame()
        df['Store'] = store_name
        df['Source File'] = file_name
        
        if 'Waktu Periode Dimulai' in df.columns:
            df['Periode'] = pd.to_datetime(df['Waktu Periode Dimulai']).dt.strftime('%b %y')
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Error processing {store_name}/{file_name}: {str(e)}")
        return pd.DataFrame()

def load_sheet_data_with_timestamp(client, sheet_name):
    """Load data dari Google Sheet dengan header di baris ke-2."""
    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sheet.worksheet(sheet_name)
        all_data = worksheet.get_all_values()
        
        if not all_data or len(all_data) < 3:
            return pd.DataFrame()
        
        headers = all_data[1]
        data_rows = all_data[2:]
        
        clean_headers = []
        seen = {}
        for i, h in enumerate(headers):
            if h is None or h.strip() == '':
                h = f'Column_{i+1}'
            else:
                h = h.strip()
            if h in seen:
                seen[h] += 1
                h = f"{h}_{seen[h]}"
            else:
                seen[h] = 0
            clean_headers.append(h)
        
        df = pd.DataFrame(data_rows, columns=clean_headers)
        df = df.replace('', pd.NA)
        df.columns = [str(col).strip().replace(' ', '_') for col in df.columns]
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Gagal load sheet {sheet_name}: {str(e)}")
        return pd.DataFrame()

def load_sheet_data_simple(client, sheet_name):
    """Load data dari Google Sheet dengan header di baris pertama."""
    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        if data:
            df = pd.DataFrame(data)
            return df
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"⚠️ Gagal load sheet {sheet_name}: {str(e)}")
        return pd.DataFrame()

def compile_charging_data(service, client, force_refresh=False):
    """Compile data charging dari file Excel di Drive."""
    if not force_refresh:
        cached_df = load_sheet_data_with_timestamp(client, SHEET_MASTER)
        if not cached_df.empty:
            return cached_df

    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_files = sum(len(list_excel_files_in_folder(service, fid)) for fid in FOLDER_IDS.values())
    processed = 0

    if total_files == 0:
        status_text.text("⚠️ Tidak ada file Excel ditemukan.")
        progress_bar.empty()
        return pd.DataFrame()

    for store_name, folder_id in FOLDER_IDS.items():
        status_text.text(f"📂 Memproses store: {store_name}...")
        for file in list_excel_files_in_folder(service, folder_id):
            try:
                file_bytes = download_file(service, file['id'])
                df = process_excel(file_bytes, store_name, file['name'])
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                st.warning(f"⚠️ Gagal download {store_name}/{file['name']}: {str(e)}")
            processed += 1
            progress_bar.progress(processed / total_files)

    status_text.text(f"✅ Selesai memproses {total_files} file!")
    progress_bar.empty()

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

def save_charging_to_gsheet(client, df):
    """Simpan hasil compile charging ke Google Sheets."""
    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        try:
            worksheet = sheet.worksheet(SHEET_MASTER)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=SHEET_MASTER, rows=max(1000, len(df)+10), cols=max(30, len(df.columns)+5))
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        worksheet.update('A1', [[f'Last Updated: {timestamp}']], value_input_option='USER_ENTERED')
        
        df_clean = df.fillna('')
        headers = df_clean.columns.tolist()
        data = [headers]
        for _, row in df_clean.iterrows():
            row_list = [str(val) if str(val) != 'nan' else '' for val in row.tolist()]
            data.append(row_list)
        
        if data:
            worksheet.update('A2', data, value_input_option='USER_ENTERED')
        
        if headers:
            end_col_letter = chr(65 + min(len(headers), 26) - 1) if len(headers) <= 26 else 'Z'
            worksheet.format(f'A2:{end_col_letter}2', {"textFormat": {"bold": True}})
        
        return True, timestamp
    except Exception as e:
        raise e

def convert_periode(p):
    """Konversi periode ke format 'Jan 26'."""
    try:
        if isinstance(p, str):
            if '-' in p and p[0].isdigit():
                dt = pd.to_datetime(p)
                return dt.strftime('%b %y')
            return p
        return str(p)
    except:
        return str(p)

def build_summary_table(charging_df, gmv_df, qty_df):
    """Bangun tabel ringkasan per Store per Bulan."""
    if charging_df.empty:
        return pd.DataFrame()
    
    # 1. Agregasi Charging
    store_col = next((col for col in charging_df.columns if 'store' in col.lower()), 'Store')
    periode_col = next((col for col in charging_df.columns if 'periode' in col.lower()), 'Periode')
    amount_col = next((col for col in charging_df.columns if 'amount_after_tax' in col.lower() or 'total_setelah_pajak' in col.lower()), None)
    
    if amount_col is None:
        st.error("❌ Kolom Amount tidak ditemukan")
        return pd.DataFrame()
    
    charging_df[amount_col] = pd.to_numeric(charging_df[amount_col], errors='coerce')
    charging_df['Periode_Clean'] = charging_df[periode_col].apply(convert_periode)
    
    charging_agg = charging_df.groupby([store_col, 'Periode_Clean'])[amount_col].sum().reset_index()
    charging_agg.columns = ['Store', 'Periode', 'Charging']
    
    # 2. Transform GMV (wide to long)
    gmv_long = pd.DataFrame()
    if not gmv_df.empty:
        id_col = 'Store' if 'Store' in gmv_df.columns else gmv_df.columns[0]
        month_cols = [col for col in gmv_df.columns if col in MONTH_ORDER]
        if month_cols:
            gmv_long = gmv_df.melt(id_vars=[id_col], value_vars=month_cols, 
                                   var_name='Periode', value_name='GMV')
            gmv_long.rename(columns={id_col: 'Store'}, inplace=True)
            gmv_long['GMV'] = pd.to_numeric(gmv_long['GMV'], errors='coerce')
    
    # 3. Transform Order Qty (wide to long)
    qty_long = pd.DataFrame()
    if not qty_df.empty:
        id_col = 'Store' if 'Store' in qty_df.columns else qty_df.columns[0]
        month_cols = [col for col in qty_df.columns if col in MONTH_ORDER]
        if month_cols:
            qty_long = qty_df.melt(id_vars=[id_col], value_vars=month_cols,
                                   var_name='Periode', value_name='Order_Qty')
            qty_long.rename(columns={id_col: 'Store'}, inplace=True)
            qty_long['Order_Qty'] = pd.to_numeric(qty_long['Order_Qty'], errors='coerce')
    
    # 4. Gabungkan semua
    summary = charging_agg.copy()
    if not gmv_long.empty:
        summary = summary.merge(gmv_long, on=['Store', 'Periode'], how='left')
    if not qty_long.empty:
        summary = summary.merge(qty_long, on=['Store', 'Periode'], how='left')
    
    # 5. Hitung metrik
    summary['AOV'] = summary['GMV'] / summary['Order_Qty']
    summary['Cost_Ratio_%'] = (summary['Charging'] / summary['GMV']) * 100
    summary['Cost_per_Order'] = summary['Charging'] / summary['Order_Qty']
    
    return summary

def format_rupiah(value):
    try:
        return f"Rp {float(value):,.0f}"
    except:
        return "-"

def format_percent(value):
    try:
        return f"{float(value):.2f}%"
    except:
        return "-"

# -------------------- MAIN APP --------------------
if 'charging_df' not in st.session_state:
    st.session_state.charging_df = None
if 'last_update' not in st.session_state:
    st.session_state.last_update = None

st.sidebar.header("⚙️ Kontrol")
action = st.sidebar.radio(
    "📌 Pilih Aksi",
    ["📥 Load & Compile Data", "📊 Dashboard Ringkasan", "💾 Simpan ke Google Sheets"]
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
    
    force_refresh = st.checkbox("🔄 Force Refresh (abaikan cache)", value=True)
    
    if st.button("🚀 Mulai Compile Semua Report", type="primary", use_container_width=True):
        with st.spinner("🔄 Membaca dan memproses semua file Excel..."):
            charging_df = compile_charging_data(service, gsheet_client, force_refresh=force_refresh)
            
            if not charging_df.empty:
                st.session_state.charging_df = charging_df
                st.success(f"✅ Berhasil compile {len(charging_df):,} baris data charging!")
                
                if st.button("💾 Simpan ke Google Sheets Sekarang", type="secondary"):
                    try:
                        success, timestamp = save_charging_to_gsheet(gsheet_client, charging_df)
                        if success:
                            st.success(f"✅ Data berhasil disimpan!")
                            st.session_state.last_update = timestamp
                    except Exception as e:
                        st.error(f"❌ Gagal menyimpan: {str(e)}")
            else:
                st.warning("⚠️ Tidak ada data charging yang berhasil di-compile.")

# -------------------- DASHBOARD RINGKASAN --------------------
elif action == "📊 Dashboard Ringkasan":
    st.header("📊 Dashboard Ringkasan per Store per Bulan")
    
    with st.spinner("📦 Memuat data dari Google Sheets..."):
        charging_df = load_sheet_data_with_timestamp(gsheet_client, SHEET_MASTER)
        gmv_df = load_sheet_data_simple(gsheet_client, SHEET_GMV)
        qty_df = load_sheet_data_simple(gsheet_client, SHEET_QTY)
    
    if charging_df.empty:
        st.warning("⚠️ Data charging belum tersedia. Silakan Load & Compile terlebih dahulu.")
        st.stop()
    
    # Build summary table
    summary_df = build_summary_table(charging_df, gmv_df, qty_df)
    
    if summary_df.empty:
        st.warning("⚠️ Tidak dapat membuat tabel ringkasan.")
        st.stop()
    
    # Filter
    st.sidebar.subheader("🔍 Filter")
    stores = st.sidebar.multiselect(
        "Pilih Store",
        options=sorted(summary_df['Store'].unique()),
        default=sorted(summary_df['Store'].unique())
    )
    
    df_filtered = summary_df[summary_df['Store'].isin(stores)]
    
    # Pivot Table untuk tampilan ringkas
    st.subheader("📋 Tabel Ringkasan")
    
    # Pilih metrik yang ingin ditampilkan
    metric_choice = st.selectbox(
        "Pilih Metrik",
        ["GMV", "Order Qty", "Charging", "AOV", "Cost Ratio (%)", "Cost per Order"]
    )
    
    metric_map = {
        "GMV": "GMV",
        "Order Qty": "Order_Qty",
        "Charging": "Charging",
        "AOV": "AOV",
        "Cost Ratio (%)": "Cost_Ratio_%",
        "Cost per Order": "Cost_per_Order"
    }
    
    metric_col = metric_map[metric_choice]
    
    # Buat pivot table: Store sebagai baris, Periode sebagai kolom
    pivot_df = df_filtered.pivot_table(
        index='Store',
        columns='Periode',
        values=metric_col,
        aggfunc='sum' if metric_choice in ["GMV", "Order Qty", "Charging"] else 'mean'
    )
    
    # Urutkan kolom sesuai MONTH_ORDER
    available_months = [m for m in MONTH_ORDER if m in pivot_df.columns]
    pivot_df = pivot_df[available_months]
    
    # Format tampilan
    if metric_choice in ["GMV", "Charging", "AOV", "Cost per Order"]:
        # Format Rupiah
        st.dataframe(pivot_df.style.format(lambda x: format_rupiah(x) if pd.notna(x) else "-"))
    elif metric_choice == "Cost Ratio (%)":
        # Format Persen
        st.dataframe(pivot_df.style.format(lambda x: format_percent(x) if pd.notna(x) else "-"))
    else:
        # Format Number
        st.dataframe(pivot_df.style.format("{:,.0f}"))
    
    # Tampilkan semua metrik dalam satu tabel detail
    st.subheader("📊 Tabel Detail Semua Metrik")
    
    # Pilih store untuk detail
    selected_store = st.selectbox("Pilih Store untuk Detail", sorted(df_filtered['Store'].unique()))
    
    store_detail = df_filtered[df_filtered['Store'] == selected_store].copy()
    store_detail = store_detail.sort_values('Periode')
    
    # Format kolom
    store_detail['GMV_Fmt'] = store_detail['GMV'].apply(format_rupiah)
    store_detail['Order_Qty_Fmt'] = store_detail['Order_Qty'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "-")
    store_detail['Charging_Fmt'] = store_detail['Charging'].apply(format_rupiah)
    store_detail['AOV_Fmt'] = store_detail['AOV'].apply(format_rupiah)
    store_detail['Cost_Ratio_Fmt'] = store_detail['Cost_Ratio_%'].apply(format_percent)
    store_detail['Cost_per_Order_Fmt'] = store_detail['Cost_per_Order'].apply(format_rupiah)
    
    st.dataframe(
        store_detail[['Periode', 'GMV_Fmt', 'Order_Qty_Fmt', 'Charging_Fmt', 
                      'AOV_Fmt', 'Cost_Ratio_Fmt', 'Cost_per_Order_Fmt']],
        column_config={
            'Periode': 'Periode',
            'GMV_Fmt': 'GMV',
            'Order_Qty_Fmt': 'Order Qty',
            'Charging_Fmt': 'Charging',
            'AOV_Fmt': 'AOV',
            'Cost_Ratio_Fmt': 'Cost Ratio',
            'Cost_per_Order_Fmt': 'Cost per Order'
        },
        use_container_width=True,
        hide_index=True
    )
    
    # Link ke Google Sheets
    st.markdown(f"📊 [Buka di Google Sheets](https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit)")

# -------------------- SAVE TO GOOGLE SHEETS --------------------
elif action == "💾 Simpan ke Google Sheets":
    st.header("💾 Simpan Hasil Compile ke Google Sheets")
    
    if st.session_state.charging_df is None:
        st.warning("⚠️ Tidak ada data untuk disimpan. Silakan Load & Compile terlebih dahulu.")
    else:
        df = st.session_state.charging_df
        st.info(f"📊 Data yang akan disimpan: **{len(df):,} baris**")
        
        if st.button("📤 Simpan ke Google Sheets", type="primary", use_container_width=True):
            with st.spinner("🔄 Menyimpan data..."):
                try:
                    success, timestamp = save_charging_to_gsheet(gsheet_client, df)
                    if success:
                        st.success(f"✅ Data berhasil disimpan! Last Updated: {timestamp}")
                        st.markdown(f"[📊 Lihat di Google Sheets](https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit)")
                        st.session_state.last_update = timestamp
                except Exception as e:
                    st.error(f"❌ Gagal menyimpan: {str(e)}")

# Footer
st.sidebar.divider()
if st.session_state.last_update:
    st.sidebar.caption(f"🕒 Last update: {st.session_state.last_update}")
