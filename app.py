import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
SHEET_PCA = "Charging PCA"

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
            df['Periode'] = pd.to_datetime(df['Waktu Periode Dimulai']).dt.to_period('M').astype(str)
        return df
    except Exception as e:
        st.warning(f"⚠️ Error processing {store_name}/{file_name}: {str(e)}")
        return pd.DataFrame()

def load_sheet_data(client, sheet_name):
    """Load data dari Google Sheet dengan penanganan header duplikat."""
    try:
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sheet.worksheet(sheet_name)
        all_data = worksheet.get_all_values()
        
        if not all_data or len(all_data) < 2:
            return pd.DataFrame()
        
        headers = all_data[0]
        data_rows = all_data[1:]
        
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

def compile_charging_data(service, client, force_refresh=False):
    """Compile data charging dari file Excel di Drive."""
    if not force_refresh:
        cached_df = load_sheet_data(client, SHEET_MASTER)
        if not cached_df.empty:
            numeric_columns = [
                'Total Order Sold Qty', 'Total MTSKU Sold Qty', 'Total sebelum Pajak', 'Pajak',
                'Total setelah Pajak', 'Amount after tax (Confirmed)', 'Commission Fees',
                'Commission Fees (Confirmed)', 'Storage Fees', 'Storage Fees (Confirmed)',
                'Warehouse Handling Fees', 'Warehouse Handling Fees (Confirmed)', 'Logistics Fees',
                'Logistics Fees (Confirmed)', 'Inbound Penalty Fees', 'Inbound Penalty Fees (Confirmed)',
                'Other Fees', 'Other Fees (Confirmed)', 'Settlement Amount'
            ]
            for col in numeric_columns:
                if col in cached_df.columns:
                    cached_df[col] = pd.to_numeric(cached_df[col], errors='coerce')
            return cached_df

    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_files = sum(len(list_excel_files_in_folder(service, fid)) for fid in FOLDER_IDS.values())
    processed = 0

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

def transform_monthly_sheet(df, value_name):
    if df.empty:
        return pd.DataFrame()
    month_cols = [col for col in df.columns if col not in ['Store', 'Description']]
    df_melted = df.melt(id_vars=['Store'], value_vars=month_cols, var_name='Month', value_name=value_name)
    month_map = {
        'Jan 26': '2026-01', 'Feb 26': '2026-02', 'Mar 26': '2026-03',
        'Apr 26': '2026-04', 'May 26': '2026-05', 'Jun 26': '2026-06',
        'Jul 26': '2026-07', 'Aug 26': '2026-08', 'Sep 26': '2026-09',
        'Oct 26': '2026-10', 'Nov 26': '2026-11', 'Dec 26': '2026-12'
    }
    df_melted['Periode'] = df_melted['Month'].map(month_map)
    df_melted = df_melted.dropna(subset=['Periode'])
    return df_melted[['Store', 'Periode', value_name]]

def transform_pca_charging(df_pca):
    if df_pca.empty:
        return pd.DataFrame()
    pca_row = df_pca[df_pca['Description'] == 'Charging PCA']
    if pca_row.empty:
        return pd.DataFrame()
    month_cols = [col for col in df_pca.columns if col != 'Description']
    df_melted = pca_row.melt(var_name='Month', value_name='Charging')
    month_map = {
        'Jan 26': '2026-01', 'Feb 26': '2026-02', 'Mar 26': '2026-03',
        'Apr 26': '2026-04', 'May 26': '2026-05', 'Jun 26': '2026-06',
        'Jul 26': '2026-07', 'Aug 26': '2026-08', 'Sep 26': '2026-09',
        'Oct 26': '2026-10', 'Nov 26': '2026-11', 'Dec 26': '2026-12'
    }
    df_melted['Periode'] = df_melted['Month'].map(month_map)
    df_melted['Store'] = 'PCA'
    df_melted = df_melted.dropna(subset=['Periode'])
    return df_melted[['Store', 'Periode', 'Charging']]

def build_combined_dataset(charging_df, gmv_df, qty_df, pca_charging_df):
    if charging_df.empty:
        return pd.DataFrame()
    
    charging_agg = charging_df.groupby(['Store', 'Periode']).agg({
        'Amount after tax (Confirmed)': 'sum'
    }).reset_index()
    charging_agg.rename(columns={'Amount after tax (Confirmed)': 'Charging'}, inplace=True)
    charging_agg = charging_agg[charging_agg['Periode'].str.startswith('2026', na=False)]
    
    gmv_long = transform_monthly_sheet(gmv_df, 'GMV')
    qty_long = transform_monthly_sheet(qty_df, 'Order_Qty')
    
    combined = charging_agg.merge(gmv_long, on=['Store', 'Periode'], how='left')
    combined = combined.merge(qty_long, on=['Store', 'Periode'], how='left')
    
    if not pca_charging_df.empty:
        combined = pd.concat([combined, pca_charging_df], ignore_index=True)
    
    combined['Cost_Ratio_%'] = (combined['Charging'] / combined['GMV']) * 100
    combined['AOV'] = combined['GMV'] / combined['Order_Qty']
    combined['Charging_per_Order'] = combined['Charging'] / combined['Order_Qty']
    
    return combined

def format_rupiah(value):
    try:
        return f"Rp {float(value):,.0f}"
    except:
        return "Rp 0"

def format_percent(value):
    try:
        return f"{float(value):.2f}%"
    except:
        return "N/A"

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

# -------------------- MAIN APP --------------------
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
    
    force_refresh = st.checkbox("🔄 Force Refresh (abaikan cache)", value=True)
    
    if st.button("🚀 Mulai Compile Semua Report", type="primary", use_container_width=True):
        with st.spinner("🔄 Membaca dan memproses semua file Excel..."):
            charging_df = compile_charging_data(service, gsheet_client, force_refresh=force_refresh)
            
            if not charging_df.empty:
                st.session_state.charging_df = charging_df
                st.success(f"✅ Berhasil compile {len(charging_df):,} baris data charging!")
                st.subheader("📋 Preview Data")
                st.dataframe(charging_df.head(10), use_container_width=True)
                
                # Opsi langsung simpan
                if st.button("💾 Simpan ke Google Sheets Sekarang", type="secondary"):
                    try:
                        success, timestamp = save_charging_to_gsheet(gsheet_client, charging_df)
                        if success:
                            st.success(f"✅ Data berhasil disimpan ke Google Sheets!")
                    except Exception as e:
                        st.error(f"❌ Gagal menyimpan: {str(e)}")
            else:
                st.warning("⚠️ Tidak ada data charging yang berhasil di-compile.")

# -------------------- DASHBOARD --------------------
elif action == "📊 Lihat Dashboard":
    st.header("📊 Dashboard Charging Report")
    
    with st.spinner("📦 Memuat data dari Google Sheets..."):
        charging_df = load_sheet_data(gsheet_client, SHEET_MASTER)
        gmv_df = load_sheet_data(gsheet_client, SHEET_GMV)
        qty_df = load_sheet_data(gsheet_client, SHEET_QTY)
        pca_df = load_sheet_data(gsheet_client, SHEET_PCA)
    
    if charging_df.empty:
        st.warning("⚠️ Data charging belum tersedia. Silakan Load & Compile terlebih dahulu.")
        st.stop()
    
    # Konversi kolom numerik untuk charging_df
    numeric_columns = [
        'Amount after tax (Confirmed)', 'Total setelah Pajak', 'Commission Fees (Confirmed)',
        'Storage Fees (Confirmed)', 'Settlement Amount'
    ]
    for col in numeric_columns:
        if col in charging_df.columns:
            charging_df[col] = pd.to_numeric(charging_df[col], errors='coerce')
    
    pca_charging = transform_pca_charging(pca_df)
    combined_df = build_combined_dataset(charging_df, gmv_df, qty_df, pca_charging)
    
    if combined_df.empty:
        st.warning("⚠️ Data gabungan kosong. Periksa sheet GMV dan Qty.")
        st.stop()
    
    # Filter sidebar
    st.sidebar.subheader("🔍 Filter Data")
    stores = st.sidebar.multiselect(
        "Pilih Store",
        options=sorted(combined_df['Store'].unique()),
        default=sorted(combined_df['Store'].unique())
    )
    periods = st.sidebar.multiselect(
        "Pilih Periode",
        options=sorted(combined_df['Periode'].unique()),
        default=sorted(combined_df['Periode'].unique())
    )
    
    df_filtered = combined_df[
        combined_df['Store'].isin(stores) & 
        combined_df['Periode'].isin(periods)
    ]
    
    if df_filtered.empty:
        st.warning("⚠️ Tidak ada data dengan filter yang dipilih.")
        st.stop()
    
    # KPI Cards
    st.subheader("💰 Key Metrics")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_charging = df_filtered['Charging'].sum()
        st.metric("Total Charging", format_rupiah(total_charging))
    with col2:
        total_gmv = df_filtered['GMV'].sum()
        st.metric("Total GMV", format_rupiah(total_gmv))
    with col3:
        avg_cost_ratio = df_filtered['Cost_Ratio_%'].mean()
        st.metric("Avg Cost Ratio", format_percent(avg_cost_ratio))
    with col4:
        total_orders = df_filtered['Order_Qty'].sum()
        st.metric("Total Orders", f"{total_orders:,.0f}")
    with col5:
        avg_aov = df_filtered['AOV'].mean()
        st.metric("Avg AOV", format_rupiah(avg_aov))
    
    # Charts
    st.subheader("📈 Charging vs GMV per Store")
    store_summary = df_filtered.groupby('Store').agg({
        'Charging': 'sum', 'GMV': 'sum', 'Order_Qty': 'sum', 'Cost_Ratio_%': 'mean'
    }).reset_index()
    
    col1, col2 = st.columns(2)
    with col1:
        fig1 = px.bar(store_summary, x='Store', y=['Charging', 'GMV'],
                      title="Charging vs GMV per Store", barmode='group', text_auto='.2s')
        st.plotly_chart(fig1, use_container_width=True)
    with col2:
        fig2 = px.bar(store_summary, x='Store', y='Cost_Ratio_%',
                      title="Cost Ratio (%) per Store", color='Store', text_auto='.2f')
        fig2.update_layout(showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)
    
    st.subheader("📉 Tren Bulanan")
    col1, col2 = st.columns(2)
    with col1:
        fig3 = px.line(df_filtered, x='Periode', y='Cost_Ratio_%', color='Store',
                       markers=True, title="Tren Cost Ratio (%) per Store")
        st.plotly_chart(fig3, use_container_width=True)
    with col2:
        monthly_trend = df_filtered.groupby('Periode').agg({'Charging': 'sum', 'GMV': 'sum'}).reset_index()
        fig4 = go.Figure()
        fig4.add_trace(go.Bar(x=monthly_trend['Periode'], y=monthly_trend['GMV'], name='GMV', yaxis='y2'))
        fig4.add_trace(go.Scatter(x=monthly_trend['Periode'], y=monthly_trend['Charging'],
                                  name='Charging', mode='lines+markers', line=dict(color='red', width=3)))
        fig4.update_layout(title="Tren Bulanan: Charging vs GMV",
                           yaxis=dict(title="Charging (Rp)"),
                           yaxis2=dict(title="GMV (Rp)", overlaying='y', side='right'))
        st.plotly_chart(fig4, use_container_width=True)
    
    st.subheader("🔵 Korelasi GMV vs Charging")
    fig5 = px.scatter(df_filtered, x='GMV', y='Charging', color='Store', size='Order_Qty',
                      hover_data=['Periode', 'Cost_Ratio_%'], title="GMV vs Charging")
    st.plotly_chart(fig5, use_container_width=True)
    
    st.subheader("📋 Tabel Insight per Store")
    insight_df = store_summary.copy()
    insight_df['Charging'] = insight_df['Charging'].apply(format_rupiah)
    insight_df['GMV'] = insight_df['GMV'].apply(format_rupiah)
    insight_df['Cost_Ratio_%'] = insight_df['Cost_Ratio_%'].apply(format_percent)
    insight_df['AOV'] = (insight_df['GMV'].str.replace('Rp ', '').str.replace(',', '').astype(float) / insight_df['Order_Qty']).apply(format_rupiah)
    st.dataframe(insight_df[['Store', 'Charging', 'GMV', 'Order_Qty', 'Cost_Ratio_%', 'AOV']],
                 use_container_width=True, hide_index=True)
    
    st.markdown(f"📊 [Buka Data Lengkap di Google Sheets](https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit)")
    csv = df_filtered.to_csv(index=False).encode('utf-8-sig')
    st.download_button(label="📥 Download Data Filtered (CSV)", data=csv,
                       file_name=f"charging_analysis_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")

# -------------------- SAVE TO GOOGLE SHEETS --------------------
elif action == "💾 Simpan ke Google Sheets":
    st.header("💾 Simpan Hasil Compile ke Google Sheets")
    
    if 'charging_df' not in st.session_state or st.session_state.charging_df is None:
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
                except Exception as e:
                    st.error(f"❌ Gagal menyimpan: {str(e)}")

# Footer
st.sidebar.divider()
st.sidebar.caption("📌 Data source: Google Sheets")
st.sidebar.caption(f"📊 Total store: {len(FOLDER_IDS) + 1} (termasuk PCA)")
