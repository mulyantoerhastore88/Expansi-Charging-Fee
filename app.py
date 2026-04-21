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

MONTH_ORDER = ["Jan 26", "Feb 26", "Mar 26", "Apr 26", "May 26", "Jun 26",
               "Jul 26", "Aug 26", "Sep 26", "Oct 26", "Nov 26", "Dec 26"]

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
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Gagal load sheet {sheet_name}: {str(e)}")
        return pd.DataFrame()

def load_sheet_data_simple(client, sheet_name):
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

def wide_to_long(df, value_name):
    """Ubah format wide ke long."""
    if df.empty:
        return pd.DataFrame()
    
    store_col = df.columns[0]
    month_cols = [col for col in df.columns[1:] if col in MONTH_ORDER]
    
    if not month_cols:
        return pd.DataFrame()
    
    df_long = df.melt(
        id_vars=[store_col],
        value_vars=month_cols,
        var_name='Periode',
        value_name=value_name
    )
    
    df_long.rename(columns={store_col: 'Store'}, inplace=True)
    df_long[value_name] = pd.to_numeric(df_long[value_name], errors='coerce')
    df_long = df_long.dropna(subset=[value_name])
    
    return df_long[['Store', 'Periode', value_name]]

def convert_periode(p):
    """Konversi periode ke format 'Jan 26'."""
    try:
        if isinstance(p, str) and '-' in p and p[0].isdigit():
            dt = pd.to_datetime(p)
            return dt.strftime('%b %y')
        return str(p).strip()
    except:
        return str(p)

def build_summary_table(charging_df, gmv_df, qty_df):
    """Gabungkan Charging, GMV, dan Qty."""
    if charging_df.empty:
        return pd.DataFrame()
    
    # 1. Agregasi Charging
    if 'Periode' in charging_df.columns:
        charging_df['Periode'] = charging_df['Periode'].apply(convert_periode)
    
    # Cari kolom amount
    amount_col = None
    for col in charging_df.columns:
        if 'amount' in col.lower() or 'total_setelah' in col.lower():
            amount_col = col
            break
    
    if amount_col is None:
        st.error("❌ Kolom Amount tidak ditemukan")
        st.write("Kolom tersedia:", charging_df.columns.tolist())
        return pd.DataFrame()
    
    charging_df[amount_col] = pd.to_numeric(charging_df[amount_col], errors='coerce')
    
    charging_agg = charging_df.groupby(['Store', 'Periode'])[amount_col].sum().reset_index()
    charging_agg.columns = ['Store', 'Periode', 'Charging']
    
    # 2. Transform GMV dan Qty
    gmv_long = wide_to_long(gmv_df, 'GMV')
    qty_long = wide_to_long(qty_df, 'Order_Qty')
    
    # 3. Gabungkan
    summary = charging_agg.copy()
    
    if not gmv_long.empty:
        summary = summary.merge(gmv_long, on=['Store', 'Periode'], how='left')
    else:
        summary['GMV'] = 0
    
    if not qty_long.empty:
        summary = summary.merge(qty_long, on=['Store', 'Periode'], how='left')
    else:
        summary['Order_Qty'] = 0
    
    # 4. Isi NaN dengan 0
    summary['GMV'] = summary['GMV'].fillna(0)
    summary['Order_Qty'] = summary['Order_Qty'].fillna(0)
    summary['Charging'] = summary['Charging'].fillna(0)
    
    # 5. Hitung metrik
    summary['AOV'] = summary.apply(lambda r: r['GMV'] / r['Order_Qty'] if r['Order_Qty'] > 0 else 0, axis=1)
    summary['Cost_Ratio_%'] = summary.apply(lambda r: (r['Charging'] / r['GMV']) * 100 if r['GMV'] > 0 else 0, axis=1)
    summary['Cost_per_Order'] = summary.apply(lambda r: r['Charging'] / r['Order_Qty'] if r['Order_Qty'] > 0 else 0, axis=1)
    
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

def format_number(value):
    try:
        return f"{float(value):,.0f}"
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
                st.success(f"✅ Berhasil compile {len(charging_df):,} baris data!")
                
                if st.button("💾 Simpan ke Google Sheets Sekarang", type="secondary"):
                    try:
                        success, timestamp = save_charging_to_gsheet(gsheet_client, charging_df)
                        if success:
                            st.success(f"✅ Data berhasil disimpan!")
                            st.session_state.last_update = timestamp
                    except Exception as e:
                        st.error(f"❌ Gagal menyimpan: {str(e)}")
            else:
                st.warning("⚠️ Tidak ada data yang berhasil di-compile.")

# -------------------- DASHBOARD --------------------
elif action == "📊 Dashboard Ringkasan":
    st.header("📊 Dashboard Ringkasan per Store per Bulan")
    
    with st.spinner("📦 Memuat data dari Google Sheets..."):
        charging_df = load_sheet_data_with_timestamp(gsheet_client, SHEET_MASTER)
        gmv_df = load_sheet_data_simple(gsheet_client, SHEET_GMV)
        qty_df = load_sheet_data_simple(gsheet_client, SHEET_QTY)
    
    if charging_df.empty:
        st.warning("⚠️ Data charging belum tersedia.")
        st.stop()
    
    # ========== DEBUG DETAIL ==========
    with st.expander("🔍 DEBUG - Periksa Data", expanded=True):
        st.subheader("1️⃣ Charging Data")
        st.write(f"Shape: {charging_df.shape}")
        st.write("Store unik:", sorted(charging_df['Store'].unique()))
        st.write("Periode unik:", sorted(charging_df['Periode'].unique()))
        st.write("Sample data:")
        st.dataframe(charging_df[['Store', 'Periode', 'Amount after tax (Confirmed)']].head(10) if 'Amount after tax (Confirmed)' in charging_df.columns else charging_df.head(10))
        
        st.subheader("2️⃣ GMV Data (sebelum transform)")
        st.write(f"Shape: {gmv_df.shape}")
        st.write("Kolom:", gmv_df.columns.tolist())
        if not gmv_df.empty:
            st.write("Store unik:", sorted(gmv_df[gmv_df.columns[0]].unique()))
            st.dataframe(gmv_df.head(10))
        
        st.subheader("3️⃣ Qty Data (sebelum transform)")
        st.write(f"Shape: {qty_df.shape}")
        st.write("Kolom:", qty_df.columns.tolist())
        if not qty_df.empty:
            st.write("Store unik:", sorted(qty_df[qty_df.columns[0]].unique()))
            st.dataframe(qty_df.head(10))
    
    # Build summary
    summary_df = build_summary_table(charging_df, gmv_df, qty_df)
    
    with st.expander("🔍 DEBUG - Hasil Transform & Merge", expanded=True):
        st.subheader("GMV Long")
        gmv_long = wide_to_long(gmv_df, 'GMV')
        st.write(f"Shape: {gmv_long.shape}")
        st.dataframe(gmv_long.head(10))
        
        st.subheader("Qty Long")
        qty_long = wide_to_long(qty_df, 'Order_Qty')
        st.write(f"Shape: {qty_long.shape}")
        st.dataframe(qty_long.head(10))
        
        st.subheader("Charging Agg")
        st.dataframe(summary_df[['Store', 'Periode', 'Charging', 'GMV', 'Order_Qty']].head(20))
    
    if summary_df.empty:
        st.warning("⚠️ Tidak dapat membuat tabel ringkasan.")
        st.stop()
    
    # Filter Store
    stores = st.sidebar.multiselect(
        "Pilih Store",
        options=sorted(summary_df['Store'].unique()),
        default=sorted(summary_df['Store'].unique())
    )
    
    df_filtered = summary_df[summary_df['Store'].isin(stores)]
    
    # Pivot Table
    st.subheader("📋 Tabel Ringkasan")
    
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
    
    pivot_df = df_filtered.pivot_table(
        index='Store',
        columns='Periode',
        values=metric_col,
        aggfunc='sum' if metric_choice in ["GMV", "Order Qty", "Charging"] else 'mean'
    )
    
    available_months = [m for m in MONTH_ORDER if m in pivot_df.columns]
    if available_months:
        pivot_df = pivot_df[available_months]
    
    if metric_choice in ["GMV", "Charging", "AOV", "Cost per Order"]:
        st.dataframe(pivot_df.style.format(lambda x: format_rupiah(x) if pd.notna(x) and x != 0 else "-"))
    elif metric_choice == "Cost Ratio (%)":
        st.dataframe(pivot_df.style.format(lambda x: format_percent(x) if pd.notna(x) and x != 0 else "-"))
    else:
        st.dataframe(pivot_df.style.format(lambda x: format_number(x) if pd.notna(x) and x != 0 else "-"))
    
    # Detail per Store
    st.subheader("📊 Detail per Store")
    selected_store = st.selectbox("Pilih Store", sorted(df_filtered['Store'].unique()))
    
    store_detail = df_filtered[df_filtered['Store'] == selected_store].sort_values('Periode')
    
    display_df = pd.DataFrame({
        'Periode': store_detail['Periode'],
        'GMV': store_detail['GMV'].apply(format_rupiah),
        'Order Qty': store_detail['Order_Qty'].apply(format_number),
        'Charging': store_detail['Charging'].apply(format_rupiah),
        'AOV': store_detail['AOV'].apply(format_rupiah),
        'Cost Ratio': store_detail['Cost_Ratio_%'].apply(format_percent),
        'Cost per Order': store_detail['Cost_per_Order'].apply(format_rupiah)
    })
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    st.markdown(f"📊 [Buka di Google Sheets](https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit)")

# -------------------- SAVE --------------------
elif action == "💾 Simpan ke Google Sheets":
    st.header("💾 Simpan Hasil Compile ke Google Sheets")
    
    if st.session_state.charging_df is None:
        st.warning("⚠️ Tidak ada data untuk disimpan.")
    else:
        df = st.session_state.charging_df
        st.info(f"📊 Data: **{len(df):,} baris**")
        
        if st.button("📤 Simpan ke Google Sheets", type="primary"):
            with st.spinner("🔄 Menyimpan..."):
                try:
                    success, timestamp = save_charging_to_gsheet(gsheet_client, df)
                    if success:
                        st.success(f"✅ Berhasil! Last Update: {timestamp}")
                        st.markdown(f"[📊 Lihat](https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit)")
                except Exception as e:
                    st.error(f"❌ Gagal: {str(e)}")

# Footer
st.sidebar.divider()
if st.session_state.last_update:
    st.sidebar.caption(f"🕒 Last update: {st.session_state.last_update}")
