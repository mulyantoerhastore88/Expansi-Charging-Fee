import streamlit as st
import pandas as pd
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime
import gspread
from streamlit_echarts import st_echarts
import plotly.express as px

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
def clean_column_names(df):
    df.columns = [str(col).strip() for col in df.columns]
    return df

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
        df = clean_column_names(df)
        
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
            df = clean_column_names(df)
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
    try:
        if isinstance(p, str):
            p = p.strip()
            if '-' in p and p[0].isdigit():
                dt = pd.to_datetime(p)
                return dt.strftime('%b %y')
            return p
        return str(p).strip()
    except:
        return str(p).strip()

def build_summary_table(charging_df, gmv_df, qty_df):
    if charging_df.empty:
        return pd.DataFrame()
    
    if 'Periode' in charging_df.columns:
        charging_df['Periode'] = charging_df['Periode'].astype(str).str.strip()
        charging_df['Periode'] = charging_df['Periode'].apply(convert_periode)
    
    amount_col = None
    for name in ['Amount after tax (Confirmed)', 'Amount_after_tax_(Confirmed)', 'Total setelah Pajak', 'Total_setelah_Pajak']:
        if name in charging_df.columns:
            amount_col = name
            break
    
    if amount_col is None:
        for col in charging_df.columns:
            if 'amount' in col.lower() or 'total_setelah' in col.lower():
                amount_col = col
                break
    
    if amount_col is None:
        return pd.DataFrame()
    
    charging_df[amount_col] = charging_df[amount_col].astype(str).str.replace('Rp', '').str.replace(',', '').str.strip()
    charging_df[amount_col] = pd.to_numeric(charging_df[amount_col], errors='coerce')
    
    charging_agg = charging_df.groupby(['Store', 'Periode'])[amount_col].sum().reset_index()
    charging_agg.columns = ['Store', 'Periode', 'Charging']
    
    gmv_long = wide_to_long(gmv_df, 'GMV')
    qty_long = wide_to_long(qty_df, 'Order_Qty')
    
    summary = charging_agg.copy()
    
    if not gmv_long.empty:
        summary = summary.merge(gmv_long, on=['Store', 'Periode'], how='left')
    else:
        summary['GMV'] = 0
    
    if not qty_long.empty:
        summary = summary.merge(qty_long, on=['Store', 'Periode'], how='left')
    else:
        summary['Order_Qty'] = 0
    
    summary['GMV'] = summary['GMV'].fillna(0)
    summary['Order_Qty'] = summary['Order_Qty'].fillna(0)
    summary['Charging'] = summary['Charging'].fillna(0)
    
    summary['AOV'] = summary.apply(lambda r: r['GMV'] / r['Order_Qty'] if r['Order_Qty'] > 0 else 0, axis=1)
    summary['Cost_Ratio_%'] = summary.apply(lambda r: (r['Charging'] / r['GMV']) * 100 if r['GMV'] > 0 else 0, axis=1)
    summary['Cost_per_Order'] = summary.apply(lambda r: r['Charging'] / r['Order_Qty'] if r['Order_Qty'] > 0 else 0, axis=1)
    
    return summary

def format_rupiah(value):
    try:
        return f"Rp {float(value):,.0f}"
    except:
        return "-"

def format_rupiah_short(value):
    try:
        val = float(value)
        if val >= 1e9:
            return f"Rp {val/1e9:.1f}B"
        elif val >= 1e6:
            return f"Rp {val/1e6:.1f}M"
        else:
            return f"Rp {val/1e3:.0f}K"
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

# -------------------- ECHARTS FUNCTIONS --------------------
def create_bar_chart(data, x_col, y_col, title, color='#5470c6'):
    """Buat bar chart dengan ECharts."""
    options = {
        "title": {"text": title, "left": "center"},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "grid": {"left": "10%", "right": "5%", "bottom": "15%", "top": "15%", "containLabel": True},
        "xAxis": {
            "type": "category",
            "data": data[x_col].tolist(),
            "axisLabel": {"rotate": 45, "interval": 0}
        },
        "yAxis": {"type": "value"},
        "series": [{
            "name": y_col,
            "type": "bar",
            "data": data[y_col].tolist(),
            "itemStyle": {"color": color},
            "label": {"show": True, "position": "top"}
        }]
    }
    return options

def create_line_chart(data, x_col, y_cols, title, colors=None):
    """Buat line chart dengan ECharts."""
    if colors is None:
        colors = ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de']
    
    series = []
    for i, col in enumerate(y_cols):
        series.append({
            "name": col,
            "type": "line",
            "data": data[col].tolist(),
            "smooth": True,
            "color": colors[i % len(colors)],
            "label": {"show": True, "position": "top"}
        })
    
    options = {
        "title": {"text": title, "left": "center"},
        "tooltip": {"trigger": "axis"},
        "legend": {"data": y_cols, "bottom": 0},
        "grid": {"left": "10%", "right": "5%", "bottom": "20%", "top": "15%", "containLabel": True},
        "xAxis": {
            "type": "category",
            "data": data[x_col].tolist(),
            "axisLabel": {"rotate": 45}
        },
        "yAxis": {"type": "value"},
        "series": series
    }
    return options

def create_pie_chart(data, labels_col, values_col, title):
    """Buat pie chart dengan ECharts."""
    pie_data = [{"name": row[labels_col], "value": row[values_col]} for _, row in data.iterrows()]
    
    options = {
        "title": {"text": title, "left": "center"},
        "tooltip": {"trigger": "item"},
        "legend": {"orient": "vertical", "left": "left"},
        "series": [{
            "name": values_col,
            "type": "pie",
            "radius": "60%",
            "data": pie_data,
            "emphasis": {"itemStyle": {"shadowBlur": 10, "shadowOffsetX": 0, "shadowColor": "rgba(0, 0, 0, 0.5)"}},
            "label": {"show": True, "formatter": "{b}: {d}%"}
        }]
    }
    return options

def create_gauge_chart(value, title, min_val=0, max_val=100):
    """Buat gauge chart untuk metrik tunggal."""
    options = {
        "title": {"text": title, "left": "center"},
        "series": [{
            "type": "gauge",
            "center": ["50%", "60%"],
            "radius": "80%",
            "startAngle": 210,
            "endAngle": -30,
            "min": min_val,
            "max": max_val,
            "progress": {"show": True, "width": 20},
            "axisLine": {"lineStyle": {"width": 20}},
            "axisTick": {"show": False},
            "splitLine": {"show": False},
            "axisLabel": {"show": False},
            "pointer": {"show": True, "length": "70%", "width": 8},
            "detail": {
                "offsetCenter": [0, 0],
                "valueAnimation": True,
                "fontSize": 24,
                "formatter": "{value}%"
            },
            "data": [{"value": value, "name": title}]
        }]
    }
    return options

# -------------------- MAIN APP --------------------
if 'charging_df' not in st.session_state:
    st.session_state.charging_df = None
if 'last_update' not in st.session_state:
    st.session_state.last_update = None

st.sidebar.header("⚙️ Kontrol")
action = st.sidebar.radio(
    "📌 Pilih Aksi",
    ["📥 Load & Compile Data", "📊 Dashboard ECharts", "📈 Dashboard Plotly", "💾 Simpan ke Google Sheets"]
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

# -------------------- DASHBOARD ECHARTS --------------------
elif action == "📊 Dashboard ECharts":
    st.header("📊 Dashboard ECharts - Shopee Charging Report")
    
    with st.spinner("📦 Memuat data dari Google Sheets..."):
        charging_df = load_sheet_data_with_timestamp(gsheet_client, SHEET_MASTER)
        gmv_df = load_sheet_data_simple(gsheet_client, SHEET_GMV)
        qty_df = load_sheet_data_simple(gsheet_client, SHEET_QTY)
    
    if charging_df.empty:
        st.warning("⚠️ Data charging belum tersedia.")
        st.stop()
    
    summary_df = build_summary_table(charging_df, gmv_df, qty_df)
    
    if summary_df.empty:
        st.warning("⚠️ Tidak dapat membuat ringkasan.")
        st.stop()
    
    # Filter Store Shopee
    shopee_stores = ["Shopee Bali", "Shopee Makassar", "Shopee Medan", "Shopee Semarang", "Shopee Surabaya"]
    summary_df = summary_df[summary_df['Store'].isin(shopee_stores)]
    
    # Filter periode 2026
    periods_2026 = [p for p in summary_df['Periode'].unique() if '26' in str(p)]
    summary_df = summary_df[summary_df['Periode'].isin(periods_2026)]
    
    if summary_df.empty:
        st.warning("⚠️ Tidak ada data untuk Store Shopee di tahun 2026.")
        st.stop()
    
    # Sidebar filters
    st.sidebar.subheader("🔍 Filter")
    selected_stores = st.sidebar.multiselect(
        "Pilih Store",
        options=sorted(summary_df['Store'].unique()),
        default=sorted(summary_df['Store'].unique())
    )
    
    df_filtered = summary_df[summary_df['Store'].isin(selected_stores)]
    
    # ========== KPI METRICS ==========
    st.subheader("📊 Key Performance Indicators")
    
    total_gmv = df_filtered['GMV'].sum()
    total_charging = df_filtered['Charging'].sum()
    total_orders = df_filtered['Order_Qty'].sum()
    avg_cost_ratio = df_filtered['Cost_Ratio_%'].mean()
    avg_aov = df_filtered['AOV'].mean()
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("💰 Total GMV", format_rupiah_short(total_gmv))
    with col2:
        st.metric("📦 Total Charging", format_rupiah_short(total_charging))
    with col3:
        st.metric("🛒 Total Orders", format_number(total_orders))
    with col4:
        st.metric("📊 Avg Cost Ratio", format_percent(avg_cost_ratio))
    with col5:
        st.metric("💵 Avg AOV", format_rupiah_short(avg_aov))
    
    # ========== ROW 1: Bar & Pie Charts ==========
    st.subheader("📈 Analisis per Store")
    col1, col2 = st.columns(2)
    
    with col1:
        # Bar Chart: GMV per Store
        store_summary = df_filtered.groupby('Store').agg({
            'GMV': 'sum', 'Charging': 'sum', 'Order_Qty': 'sum', 'Cost_Ratio_%': 'mean'
        }).reset_index()
        
        bar_options = create_bar_chart(
            store_summary, 'Store', 'GMV',
            'Total GMV per Store', '#5470c6'
        )
        st_echarts(options=bar_options, height="400px")
    
    with col2:
        # Pie Chart: Charging Distribution
        pie_options = create_pie_chart(
            store_summary, 'Store', 'Charging',
            'Distribusi Charging per Store'
        )
        st_echarts(options=pie_options, height="400px")
    
    # ========== ROW 2: Line Charts ==========
    st.subheader("📉 Tren Bulanan")
    col1, col2 = st.columns(2)
    
    with col1:
        # Line Chart: GMV & Charging Trend
        monthly_trend = df_filtered.groupby('Periode').agg({
            'GMV': 'sum', 'Charging': 'sum'
        }).reset_index()
        
        # Urutkan
        monthly_trend['Periode'] = pd.Categorical(monthly_trend['Periode'], categories=MONTH_ORDER, ordered=True)
        monthly_trend = monthly_trend.sort_values('Periode').dropna(subset=['Periode'])
        
        line_options = create_line_chart(
            monthly_trend, 'Periode', ['GMV', 'Charging'],
            'Tren GMV vs Charging', ['#5470c6', '#ee6666']
        )
        st_echarts(options=line_options, height="400px")
    
    with col2:
        # Line Chart: Cost Ratio per Store
        cost_trend = df_filtered.pivot_table(
            index='Periode', columns='Store', values='Cost_Ratio_%', aggfunc='mean'
        ).reset_index()
        cost_trend['Periode'] = pd.Categorical(cost_trend['Periode'], categories=MONTH_ORDER, ordered=True)
        cost_trend = cost_trend.sort_values('Periode').dropna(subset=['Periode'])
        
        stores_list = [s for s in selected_stores if s in cost_trend.columns]
        if stores_list:
            line_options2 = create_line_chart(
                cost_trend, 'Periode', stores_list,
                'Tren Cost Ratio (%) per Store'
            )
            st_echarts(options=line_options2, height="400px")
        else:
            st.info("Pilih store untuk melihat tren.")
    
    # ========== ROW 3: Gauge & Scatter ==========
    st.subheader("🎯 Metrik Performa")
    col1, col2 = st.columns(2)
    
    with col1:
        # Gauge Chart: Avg Cost Ratio
        gauge_value = min(avg_cost_ratio, 100) if pd.notna(avg_cost_ratio) else 0
        gauge_options = create_gauge_chart(gauge_value, "Rata-rata Cost Ratio", 0, 20)
        st_echarts(options=gauge_options, height="300px")
        
        # Insight text
        if avg_cost_ratio < 3:
            st.success("✅ Cost Ratio sangat baik (< 3%)")
        elif avg_cost_ratio < 5:
            st.info("ℹ️ Cost Ratio normal (3-5%)")
        else:
            st.warning("⚠️ Cost Ratio tinggi (> 5%)")
    
    with col2:
        # Top/Bottom Insights
        st.subheader("📋 Insight Performa Store")
        
        best_store = store_summary.loc[store_summary['Cost_Ratio_%'].idxmin()]
        worst_store = store_summary.loc[store_summary['Cost_Ratio_%'].idxmax()]
        
        col_a, col_b = st.columns(2)
        with col_a:
            st.metric(
                "🏆 Store Terbaik (Cost Ratio Terendah)",
                best_store['Store'],
                delta=format_percent(best_store['Cost_Ratio_%']),
                delta_color="inverse"
            )
        with col_b:
            st.metric(
                "⚠️ Store Perlu Perhatian",
                worst_store['Store'],
                delta=format_percent(worst_store['Cost_Ratio_%']),
                delta_color="inverse"
            )
        
        st.divider()
        
        # Tabel ringkasan singkat
        st.write("**Ringkasan per Store:**")
        summary_display = store_summary[['Store', 'GMV', 'Charging', 'Order_Qty', 'Cost_Ratio_%']].copy()
        summary_display['GMV'] = summary_display['GMV'].apply(format_rupiah_short)
        summary_display['Charging'] = summary_display['Charging'].apply(format_rupiah_short)
        summary_display['Cost_Ratio_%'] = summary_display['Cost_Ratio_%'].apply(format_percent)
        summary_display.columns = ['Store', 'GMV', 'Charging', 'Orders', 'Cost Ratio']
        st.dataframe(summary_display, use_container_width=True, hide_index=True)
    
    # ========== ROW 4: Detail Table ==========
    st.subheader("📊 Data Detail per Periode")
    
    selected_store_detail = st.selectbox("Pilih Store untuk Detail", sorted(df_filtered['Store'].unique()))
    store_detail = df_filtered[df_filtered['Store'] == selected_store_detail].sort_values('Periode')
    
    display_df = pd.DataFrame({
        'Periode': store_detail['Periode'],
        'GMV': store_detail['GMV'].apply(format_rupiah),
        'Order Qty': store_detail['Order_Qty'].apply(format_number),
        'Charging': store_detail['Charging'].apply(format_rupiah),
        'AOV': store_detail['AOV'].apply(format_rupiah),
        'Cost Ratio': store_detail['Cost_Ratio_%'].apply(format_percent),
        'Cost/Order': store_detail['Cost_per_Order'].apply(format_rupiah)
    })
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # Download button
    csv = df_filtered.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="📥 Download Data (CSV)",
        data=csv,
        file_name=f"shopee_charging_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )
    
    st.markdown(f"📊 [Buka di Google Sheets](https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit)")

# -------------------- DASHBOARD PLOTLY --------------------
elif action == "📈 Dashboard Plotly":
    st.header("📈 Dashboard Plotly - Shopee Charging Report")
    
    with st.spinner("📦 Memuat data dari Google Sheets..."):
        charging_df = load_sheet_data_with_timestamp(gsheet_client, SHEET_MASTER)
        gmv_df = load_sheet_data_simple(gsheet_client, SHEET_GMV)
        qty_df = load_sheet_data_simple(gsheet_client, SHEET_QTY)
    
    if charging_df.empty:
        st.warning("⚠️ Data charging belum tersedia.")
        st.stop()
    
    summary_df = build_summary_table(charging_df, gmv_df, qty_df)
    
    if summary_df.empty:
        st.warning("⚠️ Tidak dapat membuat ringkasan.")
        st.stop()
    
    shopee_stores = ["Shopee Bali", "Shopee Makassar", "Shopee Medan", "Shopee Semarang", "Shopee Surabaya"]
    summary_df = summary_df[summary_df['Store'].isin(shopee_stores)]
    
    periods_2026 = [p for p in summary_df['Periode'].unique() if '26' in str(p)]
    summary_df = summary_df[summary_df['Periode'].isin(periods_2026)]
    
    if summary_df.empty:
        st.warning("⚠️ Tidak ada data untuk Store Shopee di tahun 2026.")
        st.stop()
    
    st.sidebar.subheader("🔍 Filter")
    selected_stores = st.sidebar.multiselect(
        "Pilih Store",
        options=sorted(summary_df['Store'].unique()),
        default=sorted(summary_df['Store'].unique())
    )
    
    df_filtered = summary_df[summary_df['Store'].isin(selected_stores)]
    
    # KPI Metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("💰 Total GMV", format_rupiah_short(df_filtered['GMV'].sum()))
    with col2:
        st.metric("📦 Total Charging", format_rupiah_short(df_filtered['Charging'].sum()))
    with col3:
        st.metric("🛒 Total Orders", format_number(df_filtered['Order_Qty'].sum()))
    with col4:
        st.metric("📊 Avg Cost Ratio", format_percent(df_filtered['Cost_Ratio_%'].mean()))
    with col5:
        st.metric("💵 Avg AOV", format_rupiah_short(df_filtered['AOV'].mean()))
    
    # Charts
    st.subheader("📈 Visualisasi Plotly")
    col1, col2 = st.columns(2)
    
    with col1:
        store_summary = df_filtered.groupby('Store').agg({
            'GMV': 'sum', 'Charging': 'sum'
        }).reset_index()
        
        fig1 = px.bar(
            store_summary, x='Store', y=['GMV', 'Charging'],
            title="GMV vs Charging per Store", barmode='group',
            color_discrete_sequence=['#5470c6', '#ee6666']
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        fig2 = px.pie(
            store_summary, values='Charging', names='Store',
            title="Distribusi Charging per Store"
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        monthly_trend = df_filtered.groupby('Periode').agg({
            'GMV': 'sum', 'Charging': 'sum'
        }).reset_index()
        monthly_trend['Periode'] = pd.Categorical(monthly_trend['Periode'], categories=MONTH_ORDER, ordered=True)
        monthly_trend = monthly_trend.sort_values('Periode').dropna()
        
        fig3 = px.line(
            monthly_trend, x='Periode', y=['GMV', 'Charging'],
            title="Tren GMV vs Charging", markers=True,
            color_discrete_sequence=['#5470c6', '#ee6666']
        )
        st.plotly_chart(fig3, use_container_width=True)
    
    with col2:
        store_cost = df_filtered.groupby('Store')['Cost_Ratio_%'].mean().reset_index()
        fig4 = px.bar(
            store_cost, x='Store', y='Cost_Ratio_%',
            title="Rata-rata Cost Ratio per Store",
            color='Cost_Ratio_%', color_continuous_scale='RdYlGn_r'
        )
        st.plotly_chart(fig4, use_container_width=True)
    
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
