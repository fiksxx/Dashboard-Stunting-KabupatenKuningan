import streamlit as st
import pandas as pd
import numpy as np
import re
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Konfigurasi halaman
st.set_page_config(
    page_title="Analisis Data Stunting Kabupaten Kuningan",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #2c3e50;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #7f8c8d;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 4rem;
        padding: 0 2rem;
        font-size: 1.1rem;
        font-weight: 600;
    }
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: bold;
    }
    .info-box {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #3498db;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Data koordinat kecamatan
KOORDINAT_KECAMATAN = {
    'CIAWIGEBANG': {'lat': -6.94139, 'lon': 108.58000},
    'CIBEUREUM': {'lat': -7.0542423389, 'lon': 108.7335485111},
    'CIBINGBIN': {'lat': -7.0620274806, 'lon': 108.7574305306},
    'CIDAHU': {'lat': -6.9807440111, 'lon': 108.6430066694},
    'CIGANDAMEKAR': {'lat': -6.8816537806, 'lon': 108.5276942889},
    'CIGUGUR': {'lat': -6.96667, 'lon': 108.43306},
    'CILEBAK': {'lat': -7.1364884306, 'lon': 108.5866935611},
    'CILIMUS': {'lat': -6.8671659, 'lon': 108.5023500111},
    'CIMAHI': {'lat': -6.98692555, 'lon': 108.6930708306},
    'CINIRU': {'lat': -7.0426375806, 'lon': 108.4998609806},
    'CIPICUNG': {'lat': -6.9423001194, 'lon': 108.5367464806},
    'CIWARU': {'lat': -7.09250, 'lon': 108.65167},
    'DARMA': {'lat': -7.02667, 'lon': 108.40444},
    'GARAWANGI': {'lat': -6.99527306, 'lon': 108.55040389},
    'HANTARA': {'lat': -7.0586109889, 'lon': 108.4594966806},
    'JALAKSANA': {'lat': -6.90333, 'lon': 108.48417},
    'JAPARA': {'lat': -6.8962436111, 'lon': 108.519557},
    'KADUGEDE': {'lat': -6.99968035, 'lon': 108.4568345306},
    'KALIMANGGIS': {'lat': -6.9614633389, 'lon': 108.6121274},
    'KARANGKANCANA': {'lat': -7.0957940806, 'lon': 108.6601490194},
    'KRAMATMULYA': {'lat': -6.94250, 'lon': 108.49389},
    'KUNINGAN': {'lat': -6.9766048, 'lon': 108.4849021},
    'LEBAKWANGI': {'lat': -7.04083, 'lon': 108.57361},
    'LURAGUNG': {'lat': -7.0186099306, 'lon': 108.6376317611},
    'MALEBER': {'lat': -7.0286144194, 'lon': 108.5728650306},
    'MANDIRANCAN': {'lat': -6.8094092889, 'lon': 108.4686848694},
    'NUSAHERANG': {'lat': -7.0053324806, 'lon': 108.4415909889},
    'PANCALANG': {'lat': -6.82113125, 'lon': 108.4878855611},
    'PASAWAHAN': {'lat': -6.80722, 'lon': 108.42917},
    'SELAJAMBE': {'lat': -7.10417, 'lon': 108.47111},
    'SINDANGAGUNG': {'lat': -6.9782077611, 'lon': 108.5416392389},
    'SUBANG': {'lat': -7.13139, 'lon': 108.55917}
}

# Fungsi ETL
def clean_puskesmas_name(puskesmas_str):
    if isinstance(puskesmas_str, str):
        cleaned = re.sub(r'^\d+\.\s*', '', puskesmas_str)
        return cleaned.strip().upper()
    return puskesmas_str

def safe_to_numeric(series):
    return pd.to_numeric(series, errors='coerce').fillna(0)

def clean_db_column_name(col_name):
    name = str(col_name).lower()
    name = name.replace('/', '_per_')
    name = name.replace('%', 'persen')
    name = re.sub(r'[\s\(\)-]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    return name

def proses_etl(uploaded_file):
    try:
        df_title_row = pd.read_excel(uploaded_file, sheet_name="STATUS GIZI", skiprows=1, nrows=1, header=None)
        title_string = str(df_title_row.iloc[0, 0])
        
        match = re.search(r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})', title_string)
        
        if match:
            tahun_report = int(match.group(1))
            bulan_int = int(match.group(2))
            tanggal_report = int(match.group(3))
            jam_report = int(match.group(4))
            menit_report = int(match.group(5))
            month_map = {
                1: 'JANUARI', 2: 'FEBRUARI', 3: 'MARET', 4: 'APRIL', 5: 'MEI', 6: 'JUNI',
                7: 'JULI', 8: 'AGUSTUS', 9: 'SEPTEMBER', 10: 'OKTOBER', 11: 'NOVEMBER', 12: 'DESEMBER'
            }
            bulan_report = month_map.get(bulan_int, 'TIDAK DIKETAHUI')
        else:
            tahun_report, bulan_report, tanggal_report = 2025, 'TIDAK DIKETAHUI', 1
            jam_report, menit_report = 0, 0
        
        df_gizi_raw = pd.read_excel(uploaded_file, sheet_name="STATUS GIZI", skiprows=5, header=None, skipfooter=1)
        
        gizi_column_names = [
            'No', 'Puskesmas', 'KECMATAN',
            'BB/U Sangat Kurang', 'BB/U Kurang', 'BB/U Normal', 'BB/U Risiko Lebih', 'BB/U Outlier',
            'TB/U Sangat Pendek', 'TB/U Pendek', 'TB/U Normal', 'TB/U Tinggi', 'TB/U Outlier',
            'BB/TB Gizi Buruk', 'BB/TB Gizi Kurang', 'BB/TB Normal', 'BB/TB Risiko Gizi Lebih',
            'BB/TB Gizi Lebih', 'BB/TB Obesitas', 'BB/TB Outlier'
        ]
        
        if len(df_gizi_raw.columns) > len(gizi_column_names):
            df_gizi_raw = df_gizi_raw.iloc[:, :len(gizi_column_names)]
        elif len(df_gizi_raw.columns) < len(gizi_column_names):
            missing_cols = len(gizi_column_names) - len(df_gizi_raw.columns)
            for i in range(missing_cols):
                df_gizi_raw[f'missing_{i}'] = np.nan
        
        df_gizi_raw.columns = gizi_column_names
        
        df_waktu = pd.DataFrame({
            'id_waktu': [1],
            'tahun': [tahun_report],
            'bulan': [bulan_report],
            'tanggal': [tanggal_report],
            'jam': [jam_report],
            'menit': [menit_report]
        })
        
        df_gizi_raw['Puskesmas_clean'] = df_gizi_raw['Puskesmas'].apply(clean_puskesmas_name)
        df_wilayah = df_gizi_raw[['Puskesmas_clean', 'KECMATAN']].drop_duplicates().reset_index(drop=True)
        df_wilayah = df_wilayah.rename(columns={'Puskesmas_clean': 'nama_puskesmas', 'KECMATAN': 'nama_kecamatan'})
        df_wilayah.insert(0, 'id_wilayah', range(1, 1 + len(df_wilayah)))
        
        df_fact = df_gizi_raw.copy()
        raw_gizi_cols = gizi_column_names[3:]
        
        for col in raw_gizi_cols:
            df_fact[col] = safe_to_numeric(df_fact[col])
        
        cols_ditimbang = ['BB/U Sangat Kurang', 'BB/U Kurang', 'BB/U Normal', 'BB/U Risiko Lebih', 'BB/U Outlier']
        df_fact['jumlah_balita_ditimbang'] = df_fact[cols_ditimbang].sum(axis=1)
        
        cols_kurang_gizi = ['BB/U Sangat Kurang', 'BB/U Kurang']
        df_fact['jumlah_balita_kurang_gizi'] = df_fact[cols_kurang_gizi].sum(axis=1)
        
        cols_stunting = ['TB/U Sangat Pendek', 'TB/U Pendek']
        df_fact['jumlah_balita_stunting'] = df_fact[cols_stunting].sum(axis=1)
        
        cols_wasting = ['BB/TB Gizi Buruk', 'BB/TB Gizi Kurang']
        df_fact['jumlah_balita_wasting'] = df_fact[cols_wasting].sum(axis=1)
        
        pembagi_d = df_fact['jumlah_balita_ditimbang']
        df_fact['persentase_kurang_gizi'] = (df_fact['jumlah_balita_kurang_gizi'] / pembagi_d * 100).replace([np.inf, -np.inf], 0).fillna(0)
        df_fact['persentase_stunting'] = (df_fact['jumlah_balita_stunting'] / pembagi_d * 100).replace([np.inf, -np.inf], 0).fillna(0)
        df_fact['persentase_wasting'] = (df_fact['jumlah_balita_wasting'] / pembagi_d * 100).replace([np.inf, -np.inf], 0).fillna(0)
        
        df_fact = pd.merge(df_fact, df_wilayah, left_on=['Puskesmas_clean', 'KECMATAN'], 
                          right_on=['nama_puskesmas', 'nama_kecamatan'])
        df_fact['id_waktu'] = 1
        
        key_cols = ['nama_kecamatan', 'id_waktu']
        calc_cols = ['jumlah_balita_ditimbang', 'jumlah_balita_kurang_gizi', 'persentase_kurang_gizi',
                     'jumlah_balita_stunting', 'persentase_stunting', 'jumlah_balita_wasting', 'persentase_wasting']
        
        final_fact_columns = key_cols + calc_cols + raw_gizi_cols
        df_fact_final = df_fact[final_fact_columns]
        df_fact_final.columns = [clean_db_column_name(col) for col in df_fact_final.columns]
        
        return df_fact_final, df_wilayah, df_waktu, True, "Proses ETL berhasil!"
    
    except Exception as e:
        return None, None, None, False, f"Error: {str(e)}"

# Header
st.markdown('<p class="main-header">📊 Sistem Analisis Data Stunting</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Dinas Kesehatan Kabupaten Kuningan</p>', unsafe_allow_html=True)
st.markdown("---")

# Sidebar
with st.sidebar:
    st.markdown("### 🏥 DINKES Kuningan")
    st.markdown("---")
    st.markdown("### 📤 Upload Data")
    uploaded_file = st.file_uploader("Upload file Excel (raw_status_gizi.xlsx)", type=['xlsx'])
    
    if uploaded_file:
        st.success("✅ File berhasil diupload!")
    
    st.markdown("---")
    st.markdown("### 📖 Panduan")
    with st.expander("Cara Menggunakan"):
        st.markdown("""
        1. **Upload File**: Klik tombol upload di atas
        2. **Tunggu Proses**: Sistem akan memproses data otomatis
        3. **Lihat Hasil**: Eksplorasi visualisasi di tab-tab yang tersedia
        4. **Download**: Unduh hasil analisis jika diperlukan
        """)
    
    with st.expander("Tentang Indikator"):
        st.markdown("""
        - **Stunting**: Tinggi badan pendek untuk usia
        - **Kurang Gizi**: Berat badan kurang untuk usia
        - **Wasting**: Berat badan kurang untuk tinggi badan
        """)

# Main content
if uploaded_file is None:
    st.info("👈 Silakan upload file data stunting di menu sebelah kiri untuk memulai analisis.")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Kecamatan", "32", help="Jumlah kecamatan di Kab. Kuningan")
    with col2:
        st.metric("Status Sistem", "Siap", "✓", help="Sistem siap memproses data")
    with col3:
        st.metric("Mode", "ETL + Visualisasi", help="Extract, Transform, Load + Visualisasi")
    with col4:
        st.metric("Menunggu", "Upload Data", help="Upload file untuk memulai")
    
    st.markdown("---")
    st.markdown("### 🎯 Fitur Sistem")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        #### 🗺️ Peta Interaktif
        - Sebaran stunting per kecamatan
        - Visualisasi ukuran dan warna
        - Hover untuk detail data
        """)
    
    with col2:
        st.markdown("""
        #### 📊 Grafik Lengkap
        - Top kecamatan tertinggi/terendah
        - Perbandingan antar indikator
        - Distribusi kategori gizi
        """)
    
    with col3:
        st.markdown("""
        #### 💾 Export Data
        - Download hasil ETL
        - Format CSV siap pakai
        - Data agregat per kecamatan
        """)

else:
    with st.spinner("🔄 Memproses data... Mohon tunggu..."):
        df_fact, df_wilayah, df_waktu, success, message = proses_etl(uploaded_file)
    
    if success:
        st.success(message)
        
        # Agregasi data
        df_agg = df_fact.groupby('nama_kecamatan').agg({
            'jumlah_balita_ditimbang': 'sum',
            'jumlah_balita_stunting': 'sum',
            'jumlah_balita_kurang_gizi': 'sum',
            'jumlah_balita_wasting': 'sum'
        }).reset_index()
        
        df_agg['persentase_stunting'] = (df_agg['jumlah_balita_stunting'] / df_agg['jumlah_balita_ditimbang'] * 100).fillna(0)
        df_agg['persentase_kurang_gizi'] = (df_agg['jumlah_balita_kurang_gizi'] / df_agg['jumlah_balita_ditimbang'] * 100).fillna(0)
        df_agg['persentase_wasting'] = (df_agg['jumlah_balita_wasting'] / df_agg['jumlah_balita_ditimbang'] * 100).fillna(0)
        
        # Tambahkan koordinat
        df_agg['lat'] = df_agg['nama_kecamatan'].map(lambda x: KOORDINAT_KECAMATAN.get(x, {}).get('lat', 0))
        df_agg['lon'] = df_agg['nama_kecamatan'].map(lambda x: KOORDINAT_KECAMATAN.get(x, {}).get('lon', 0))
        
        # Ringkasan statistik dengan styling lebih baik
        st.markdown("### 📈 Ringkasan Data")
        col1, col2, col3, col4, col5 = st.columns(5)
        
        total_ditimbang = int(df_agg['jumlah_balita_ditimbang'].sum())
        total_stunting = int(df_agg['jumlah_balita_stunting'].sum())
        total_kurang_gizi = int(df_agg['jumlah_balita_kurang_gizi'].sum())
        total_wasting = int(df_agg['jumlah_balita_wasting'].sum())
        avg_stunting = df_agg['persentase_stunting'].mean()
        
        with col1:
            st.metric("Total Balita Ditimbang", f"{total_ditimbang:,}", help="Jumlah total balita yang ditimbang")
        with col2:
            st.metric("Total Stunting", f"{total_stunting:,}", f"{avg_stunting:.1f}%", help="Jumlah dan persentase rata-rata stunting")
        with col3:
            st.metric("Total Kurang Gizi", f"{total_kurang_gizi:,}", help="Jumlah balita kurang gizi")
        with col4:
            st.metric("Total Wasting", f"{total_wasting:,}", help="Jumlah balita wasting")
        with col5:
            st.metric("Jumlah Kecamatan", f"{len(df_agg)}", help="Total kecamatan yang dianalisis")
        
        st.markdown("---")
        
        # Tab untuk visualisasi
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["🗺️ Peta Sebaran", "📊 Perbandingan Kecamatan", "🎯 Distribusi & Kategori", "📋 Tabel Data", "💾 Download"])
        
        with tab1:
            st.markdown("### 🗺️ Peta Sebaran Stunting per Kecamatan")
            
            # Pilihan jenis peta
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown("#### Pilih Jenis Visualisasi Peta")
            with col2:
                jenis_peta = st.radio("Tipe Peta:", ["Scatter Map", "Heatmap"], horizontal=True)
            
            if jenis_peta == "Scatter Map":
                # Peta Scatter (yang sudah ada)
                fig_map = px.scatter_mapbox(
                    df_agg,
                    lat='lat',
                    lon='lon',
                    size='jumlah_balita_stunting',
                    color='persentase_stunting',
                    hover_name='nama_kecamatan',
                    hover_data={
                        'jumlah_balita_ditimbang': ':,',
                        'jumlah_balita_stunting': ':,',
                        'persentase_stunting': ':.2f',
                        'lat': False,
                        'lon': False
                    },
                    color_continuous_scale='Reds',
                    size_max=35,
                    zoom=9.5,
                    mapbox_style='open-street-map',
                    title='Sebaran Kasus Stunting (Scatter Map)'
                )
                
                fig_map.update_layout(
                    height=650,
                    margin={"r":0,"t":50,"l":0,"b":0},
                    font=dict(size=12),
                    title_font_size=18
                )
                st.plotly_chart(fig_map, use_container_width=True)
                
                st.markdown("""
                <div class="info-box">
                    <b>💡 Cara membaca Scatter Map:</b><br>
                    • <b>Ukuran lingkaran</b> = Jumlah kasus stunting (lingkaran lebih besar = kasus lebih banyak)<br>
                    • <b>Warna merah</b> = Persentase stunting (merah lebih gelap = persentase lebih tinggi)<br>
                    • <b>Klik lingkaran</b> untuk melihat detail informasi kecamatan
                </div>
                """, unsafe_allow_html=True)
            
            else:
                # Heatmap
                fig_heatmap = px.density_mapbox(
                    df_agg,
                    lat='lat',
                    lon='lon',
                    z='persentase_stunting',
                    radius=25,
                    center=dict(lat=-6.98, lon=108.48),
                    zoom=9.5,
                    mapbox_style='open-street-map',
                    color_continuous_scale='Reds',
                    range_color=[0, df_agg['persentase_stunting'].max()],
                    title='Peta Panas (Heatmap) Intensitas Stunting',
                    hover_name='nama_kecamatan',
                    hover_data={
                        'persentase_stunting': ':.2f',
                        'jumlah_balita_stunting': ':,',
                        'lat': False,
                        'lon': False
                    }
                )
                
                fig_heatmap.update_layout(
                    height=650,
                    margin={"r":0,"t":50,"l":0,"b":0},
                    font=dict(size=12),
                    title_font_size=18,
                    coloraxis_colorbar=dict(
                        title="Persentase<br>Stunting (%)",
                        ticksuffix="%"
                    )
                )
                st.plotly_chart(fig_heatmap, use_container_width=True)
                
                st.markdown("""
                <div class="info-box">
                    <b>💡 Cara membaca Heatmap:</b><br>
                    • <b>Warna intensitas</b> = Tingkat persentase stunting di area tersebut<br>
                    • <b>Merah lebih gelap/terang</b> = Konsentrasi stunting lebih tinggi<br>
                    • <b>Area yang menyala</b> menunjukkan zona dengan masalah stunting yang perlu perhatian khusus<br>
                    • Hover pada peta untuk melihat detail per kecamatan
                </div>
                """, unsafe_allow_html=True)
                
                # Tambahan: Slider untuk mengatur radius heatmap
                st.markdown("#### Pengaturan Heatmap")
                col1, col2 = st.columns(2)
                with col1:
                    radius_heat = st.slider("Radius Intensitas Panas:", 10, 50, 25, 5,
                                           help="Semakin besar radius, semakin luas area yang terpengaruh")
                with col2:
                    opacity_heat = st.slider("Tingkat Transparansi:", 0.3, 1.0, 0.8, 0.1,
                                            help="Mengatur tingkat transparansi heatmap")
                
                if radius_heat != 25 or opacity_heat != 0.8:
                    fig_heatmap_custom = px.density_mapbox(
                        df_agg,
                        lat='lat',
                        lon='lon',
                        z='persentase_stunting',
                        radius=radius_heat,
                        center=dict(lat=-6.98, lon=108.48),
                        zoom=9.5,
                        mapbox_style='open-street-map',
                        color_continuous_scale='Reds',
                        range_color=[0, df_agg['persentase_stunting'].max()],
                        title='Peta Panas (Heatmap) - Custom Settings',
                        hover_name='nama_kecamatan',
                        hover_data={
                            'persentase_stunting': ':.2f',
                            'jumlah_balita_stunting': ':,',
                            'lat': False,
                            'lon': False
                        }
                    )
                    
                    fig_heatmap_custom.update_layout(
                        height=650,
                        margin={"r":0,"t":50,"l":0,"b":0},
                        font=dict(size=12),
                        title_font_size=18,
                        coloraxis_colorbar=dict(
                            title="Persentase<br>Stunting (%)",
                            ticksuffix="%"
                        )
                    )
                    
                    # Update opacity
                    fig_heatmap_custom.update_traces(opacity=opacity_heat)
                    
                    st.plotly_chart(fig_heatmap_custom, use_container_width=True)
            
            # Tambahan: Highlight kecamatan dengan perhatian khusus
            st.markdown("### ⚠️ Kecamatan Prioritas")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 🔴 Persentase Tertinggi (Top 5)")
                top5_persen = df_agg.nlargest(5, 'persentase_stunting')[['nama_kecamatan', 'persentase_stunting', 'jumlah_balita_stunting']]
                for idx, row in top5_persen.iterrows():
                    st.markdown(f"**{row['nama_kecamatan']}**: {row['persentase_stunting']:.2f}% ({int(row['jumlah_balita_stunting'])} balita)")
            
            with col2:
                st.markdown("#### 🔢 Jumlah Kasus Tertinggi (Top 5)")
                top5_jumlah = df_agg.nlargest(5, 'jumlah_balita_stunting')[['nama_kecamatan', 'jumlah_balita_stunting', 'persentase_stunting']]
                for idx, row in top5_jumlah.iterrows():
                    st.markdown(f"**{row['nama_kecamatan']}**: {int(row['jumlah_balita_stunting'])} balita ({row['persentase_stunting']:.2f}%)")
        
        with tab2:
            st.markdown("### 📊 Perbandingan Antar Kecamatan")
            
            # Pilihan filter
            col1, col2 = st.columns([2, 1])
            with col1:
                jumlah_kecamatan = st.slider("Jumlah kecamatan yang ditampilkan:", 5, 32, 15)
            with col2:
                urutan = st.radio("Urutkan berdasarkan:", ["Tertinggi", "Terendah"])
            
            # Bar chart dengan jumlah dan persentase
            st.markdown("#### Top Kecamatan dengan Stunting " + urutan)
            
            if urutan == "Tertinggi":
                df_display = df_agg.nlargest(jumlah_kecamatan, 'persentase_stunting')
            else:
                df_display = df_agg.nsmallest(jumlah_kecamatan, 'persentase_stunting')
            
            # Buat bar chart dengan angka yang lebih jelas
            fig_bar = go.Figure()
            
            fig_bar.add_trace(go.Bar(
                y=df_display['nama_kecamatan'],
                x=df_display['persentase_stunting'],
                orientation='h',
                text=[f"{persen:.1f}% ({int(jml)} balita)" 
                      for persen, jml in zip(df_display['persentase_stunting'], df_display['jumlah_balita_stunting'])],
                textposition='outside',
                marker=dict(
                    color=df_display['persentase_stunting'],
                    colorscale='Reds',
                    showscale=True,
                    colorbar=dict(title="Persentase (%)")
                ),
                hovertemplate='<b>%{y}</b><br>Persentase: %{x:.2f}%<br><extra></extra>'
            ))
            
            fig_bar.update_layout(
                height=max(400, jumlah_kecamatan * 35),
                xaxis_title='Persentase Stunting (%)',
                yaxis_title='',
                yaxis={'categoryorder':'total ascending' if urutan == "Tertinggi" else 'total descending'},
                font=dict(size=11),
                margin=dict(l=150, r=150, t=30, b=50)
            )
            st.plotly_chart(fig_bar, use_container_width=True)
            
            st.markdown("---")
            
            # Perbandingan 3 indikator
            st.markdown("#### Perbandingan Tiga Indikator Gizi")
            
            df_compare = df_agg.sort_values('persentase_stunting', ascending=False).head(15)
            
            fig_compare = go.Figure()
            
            fig_compare.add_trace(go.Bar(
                name='Stunting',
                x=df_compare['nama_kecamatan'],
                y=df_compare['persentase_stunting'],
                text=[f"{val:.1f}%<br>({int(jml)})" for val, jml in zip(df_compare['persentase_stunting'], df_compare['jumlah_balita_stunting'])],
                textposition='outside',
                marker_color='#e74c3c',
                hovertemplate='<b>%{x}</b><br>Stunting: %{y:.2f}%<extra></extra>'
            ))
            fig_compare.add_trace(go.Bar(
                name='Kurang Gizi',
                x=df_compare['nama_kecamatan'],
                y=df_compare['persentase_kurang_gizi'],
                text=[f"{val:.1f}%<br>({int(jml)})" for val, jml in zip(df_compare['persentase_kurang_gizi'], df_compare['jumlah_balita_kurang_gizi'])],
                textposition='outside',
                marker_color='#f39c12',
                hovertemplate='<b>%{x}</b><br>Kurang Gizi: %{y:.2f}%<extra></extra>'
            ))
            fig_compare.add_trace(go.Bar(
                name='Wasting',
                x=df_compare['nama_kecamatan'],
                y=df_compare['persentase_wasting'],
                text=[f"{val:.1f}%<br>({int(jml)})" for val, jml in zip(df_compare['persentase_wasting'], df_compare['jumlah_balita_wasting'])],
                textposition='outside',
                marker_color='#9b59b6',
                hovertemplate='<b>%{x}</b><br>Wasting: %{y:.2f}%<extra></extra>'
            ))
            
            fig_compare.update_layout(
                barmode='group',
                height=550,
                xaxis_tickangle=-45,
                yaxis_title='Persentase (%)',
                xaxis_title='Kecamatan',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                font=dict(size=10),
                margin=dict(t=80, b=120)
            )
            st.plotly_chart(fig_compare, use_container_width=True)
            
            st.markdown("""
            <div class="info-box">
                <b>📌 Catatan:</b> Angka di dalam kurung menunjukkan jumlah balita absolut untuk setiap indikator.
            </div>
            """, unsafe_allow_html=True)
        
        with tab3:
            st.markdown("### 🎯 Distribusi dan Kategori Status Gizi")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### Distribusi Status Gizi Balita")
                
                total_normal = total_ditimbang - total_stunting - total_kurang_gizi - total_wasting
                
                labels = ['Stunting', 'Kurang Gizi', 'Wasting', 'Normal/Lainnya']
                values = [total_stunting, total_kurang_gizi, total_wasting, total_normal]
                colors = ['#ff6b6b', '#feca57', '#ee5a6f', '#48dbfb']
                
                # Pie chart dengan label yang jelas
                fig_pie = go.Figure(data=[go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.45,
                    marker_colors=colors,
                    textinfo='label+percent+value',
                    texttemplate='<b>%{label}</b><br>%{value:,} balita<br>(%{percent})',
                    textposition='outside',
                    textfont=dict(size=12),
                    hovertemplate='<b>%{label}</b><br>Jumlah: %{value:,} balita<br>Persentase: %{percent}<extra></extra>'
                )])
                
                fig_pie.update_layout(
                    height=500,
                    title_text="Proporsi Masalah Gizi",
                    font=dict(size=11),
                    showlegend=False,
                    margin=dict(t=80, b=20, l=20, r=20)
                )
                st.plotly_chart(fig_pie, use_container_width=True)
                
                # Info box dengan detail
                st.markdown(f"""
                <div class="info-box">
                    <b>📊 Detail Distribusi:</b><br>
                    • <b style="color: #ff6b6b;">Stunting:</b> {total_stunting:,} balita ({total_stunting/total_ditimbang*100:.2f}%)<br>
                    • <b style="color: #feca57;">Kurang Gizi:</b> {total_kurang_gizi:,} balita ({total_kurang_gizi/total_ditimbang*100:.2f}%)<br>
                    • <b style="color: #ee5a6f;">Wasting:</b> {total_wasting:,} balita ({total_wasting/total_ditimbang*100:.2f}%)<br>
                    • <b style="color: #48dbfb;">Normal/Lainnya:</b> {total_normal:,} balita ({total_normal/total_ditimbang*100:.2f}%)
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown("#### Kategori Berdasarkan Tingkat Keparahan")
                
                # Kategorisasi kecamatan berdasarkan persentase stunting
                df_agg['kategori'] = pd.cut(
                    df_agg['persentase_stunting'],
                    bins=[0, 5, 10, 20, 100],
                    labels=['Rendah (<5%)', 'Sedang (5-10%)', 'Tinggi (10-20%)', 'Sangat Tinggi (>20%)']
                )
                
                kategori_count = df_agg['kategori'].value_counts().sort_index()
                kategori_colors = ['#2ecc71', '#f39c12', '#e67e22', '#e74c3c']
                
                fig_kategori = go.Figure(data=[go.Pie(
                    labels=kategori_count.index,
                    values=kategori_count.values,
                    hole=0.45,
                    marker_colors=kategori_colors,
                    textinfo='label+percent+value',
                    texttemplate='<b>%{label}</b><br>%{value} kecamatan<br>(%{percent})',
                    textposition='outside',
                    textfont=dict(size=11),
                    hovertemplate='<b>%{label}</b><br>Jumlah: %{value} kecamatan<br>Persentase: %{percent}<extra></extra>'
                )])
                
                fig_kategori.update_layout(
                    height=500,
                    title_text="Kategori Kecamatan Berdasarkan Stunting",
                    font=dict(size=11),
                    showlegend=False,
                    margin=dict(t=80, b=20, l=20, r=20)
                )
                st.plotly_chart(fig_kategori, use_container_width=True)
                
                # Detail per kategori
                st.markdown("**📍 Daftar Kecamatan per Kategori:**")
                for kategori in ['Rendah (<5%)', 'Sedang (5-10%)', 'Tinggi (10-20%)', 'Sangat Tinggi (>20%)']:
                    kec_list = df_agg[df_agg['kategori'] == kategori]['nama_kecamatan'].tolist()
                    if kec_list:
                        emoji = '🟢' if 'Rendah' in kategori else '🟡' if 'Sedang' in kategori else '🟠' if 'Tinggi' in kategori else '🔴'
                        st.markdown(f"{emoji} **{kategori}**: {', '.join(kec_list)}")
            
            st.markdown("---")
            
            # Distribusi detail per indikator BB/U, TB/U, BB/TB
            st.markdown("#### Distribusi Detail Kategori Gizi (BB/U, TB/U, BB/TB)")
            
            # Agregasi untuk kategori detail
            kategori_detail = {
                'BB/U': ['bb_per_u_sangat_kurang', 'bb_per_u_kurang', 'bb_per_u_normal', 'bb_per_u_risiko_lebih'],
                'TB/U': ['tb_per_u_sangat_pendek', 'tb_per_u_pendek', 'tb_per_u_normal', 'tb_per_u_tinggi'],
                'BB/TB': ['bb_per_tb_gizi_buruk', 'bb_per_tb_gizi_kurang', 'bb_per_tb_normal', 
                         'bb_per_tb_risiko_gizi_lebih', 'bb_per_tb_gizi_lebih', 'bb_per_tb_obesitas']
            }
            
            col1, col2, col3 = st.columns(3)
            
            for idx, (col, (indikator, kolom_list)) in enumerate(zip([col1, col2, col3], kategori_detail.items())):
                with col:
                    st.markdown(f"**{indikator}**")
                    
                    # Hitung total per kategori
                    kategori_values = []
                    kategori_labels = []
                    for kolom in kolom_list:
                        if kolom in df_fact.columns:
                            nilai = df_fact[kolom].sum()
                            kategori_values.append(nilai)
                            # Buat label yang lebih ringkas
                            label_map = {
                                'bb_per_u_sangat_kurang': 'Sangat Kurang',
                                'bb_per_u_kurang': 'Kurang',
                                'bb_per_u_normal': 'Normal',
                                'bb_per_u_risiko_lebih': 'Risiko Lebih',
                                'tb_per_u_sangat_pendek': 'Sangat Pendek',
                                'tb_per_u_pendek': 'Pendek',
                                'tb_per_u_normal': 'Normal',
                                'tb_per_u_tinggi': 'Tinggi',
                                'bb_per_tb_gizi_buruk': 'Gizi Buruk',
                                'bb_per_tb_gizi_kurang': 'Gizi Kurang',
                                'bb_per_tb_normal': 'Normal',
                                'bb_per_tb_risiko_gizi_lebih': 'Risiko Lebih',
                                'bb_per_tb_gizi_lebih': 'Gizi Lebih',
                                'bb_per_tb_obesitas': 'Obesitas'
                            }
                            kategori_labels.append(label_map.get(kolom, kolom))
                    
                    fig_detail = go.Figure(data=[go.Bar(
                        x=kategori_labels,
                        y=kategori_values,
                        text=[f"{int(v):,}" for v in kategori_values],
                        textposition='outside',
                        marker_color=['#e74c3c' if 'kurang' in l.lower() or 'pendek' in l.lower() or 'buruk' in l.lower()
                                     else '#f39c12' if 'risiko' in l.lower() or 'lebih' in l.lower() or 'obesitas' in l.lower()
                                     else '#2ecc71' for l in kategori_labels],
                        hovertemplate='<b>%{x}</b><br>Jumlah: %{y:,} balita<extra></extra>'
                    )])
                    
                    fig_detail.update_layout(
                        height=350,
                        xaxis_tickangle=-45,
                        yaxis_title='Jumlah Balita',
                        margin=dict(t=20, b=80, l=40, r=20),
                        font=dict(size=9)
                    )
                    st.plotly_chart(fig_detail, use_container_width=True)
            
            st.markdown("""
            <div class="info-box">
                <b>📚 Penjelasan Indikator:</b><br>
                • <b>BB/U (Berat Badan per Usia):</b> Mengukur kecukupan berat badan anak sesuai usianya<br>
                • <b>TB/U (Tinggi Badan per Usia):</b> Mengukur stunting atau kekurangan gizi kronis<br>
                • <b>BB/TB (Berat Badan per Tinggi Badan):</b> Mengukur wasting atau kekurangan gizi akut
            </div>
            """, unsafe_allow_html=True)
        
        with tab4:
            st.markdown("### 📋 Data Detail per Kecamatan")
            
            # Filter dan pencarian
            col1, col2 = st.columns([3, 1])
            with col1:
                search_term = st.text_input("🔍 Cari kecamatan:", placeholder="Ketik nama kecamatan...")
            with col2:
                sort_by = st.selectbox("Urutkan berdasarkan:", 
                                      ["Nama Kecamatan", "% Stunting", "Jml Stunting", "Jml Ditimbang"])
            
            # Filter data
            df_display = df_agg.copy()
            if search_term:
                df_display = df_display[df_display['nama_kecamatan'].str.contains(search_term.upper())]
            
            # Sorting
            if sort_by == "Nama Kecamatan":
                df_display = df_display.sort_values('nama_kecamatan')
            elif sort_by == "% Stunting":
                df_display = df_display.sort_values('persentase_stunting', ascending=False)
            elif sort_by == "Jml Stunting":
                df_display = df_display.sort_values('jumlah_balita_stunting', ascending=False)
            else:
                df_display = df_display.sort_values('jumlah_balita_ditimbang', ascending=False)
            
            # Format tabel
            df_table = df_display[['nama_kecamatan', 'jumlah_balita_ditimbang', 'jumlah_balita_stunting', 
                                   'persentase_stunting', 'jumlah_balita_kurang_gizi', 
                                   'persentase_kurang_gizi', 'jumlah_balita_wasting', 
                                   'persentase_wasting', 'kategori']].copy()
            
            df_table.columns = ['Kecamatan', 'Jml Ditimbang', 'Jml Stunting', '% Stunting', 
                               'Jml Kurang Gizi', '% Kurang Gizi', 'Jml Wasting', '% Wasting', 'Kategori']
            
            # Format angka
            df_table['Jml Ditimbang'] = df_table['Jml Ditimbang'].apply(lambda x: f"{int(x):,}")
            df_table['Jml Stunting'] = df_table['Jml Stunting'].apply(lambda x: f"{int(x):,}")
            df_table['% Stunting'] = df_table['% Stunting'].apply(lambda x: f"{x:.2f}%")
            df_table['Jml Kurang Gizi'] = df_table['Jml Kurang Gizi'].apply(lambda x: f"{int(x):,}")
            df_table['% Kurang Gizi'] = df_table['% Kurang Gizi'].apply(lambda x: f"{x:.2f}%")
            df_table['Jml Wasting'] = df_table['Jml Wasting'].apply(lambda x: f"{int(x):,}")
            df_table['% Wasting'] = df_table['% Wasting'].apply(lambda x: f"{x:.2f}%")
            
            # Tambahkan warna untuk kategori
            def highlight_kategori(row):
                if 'Sangat Tinggi' in str(row['Kategori']):
                    return ['background-color: #ffcccc'] * len(row)
                elif 'Tinggi' in str(row['Kategori']):
                    return ['background-color: #ffe6cc'] * len(row)
                elif 'Sedang' in str(row['Kategori']):
                    return ['background-color: #fff4cc'] * len(row)
                else:
                    return ['background-color: #ccffcc'] * len(row)
            
            df_styled = df_table.style.apply(highlight_kategori, axis=1)
            
            st.dataframe(df_styled, use_container_width=True, height=500)
            
            st.markdown(f"**Menampilkan {len(df_display)} dari {len(df_agg)} kecamatan**")
        
        with tab5:
            st.markdown("### 💾 Download Hasil ETL dan Analisis")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 📊 Hasil ETL (Star Schema)")
                st.markdown("Download file hasil proses ETL dalam format CSV:")
                
                # Download Fact Table
                csv_fact = df_fact.to_csv(index=False)
                st.download_button(
                    label="📥 Download Fact Gizi Balita",
                    data=csv_fact,
                    file_name="fact_gizi_balita.csv",
                    mime="text/csv",
                    use_container_width=True
                )
                
                # Download Dim Wilayah
                csv_wilayah = df_wilayah.to_csv(index=False)
                st.download_button(
                    label="📥 Download Dimensi Wilayah",
                    data=csv_wilayah,
                    file_name="dim_wilayah.csv",
                    mime="text/csv",
                    use_container_width=True
                )
                
                # Download Dim Waktu
                csv_waktu = df_waktu.to_csv(index=False)
                st.download_button(
                    label="📥 Download Dimensi Waktu",
                    data=csv_waktu,
                    file_name="dim_waktu.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with col2:
                st.markdown("#### 📈 Data Analisis")
                st.markdown("Download data agregat dan analisis per kecamatan:")
                
                # Download Data Agregat
                csv_agg = df_agg.to_csv(index=False)
                st.download_button(
                    label="📥 Download Data Agregat Kecamatan",
                    data=csv_agg,
                    file_name="data_agregat_kecamatan.csv",
                    mime="text/csv",
                    use_container_width=True
                )
                
                # Download Summary Report
                summary_data = {
                    'Indikator': ['Total Balita Ditimbang', 'Total Stunting', 'Persentase Stunting Rata-rata',
                                 'Total Kurang Gizi', 'Total Wasting', 'Jumlah Kecamatan'],
                    'Nilai': [total_ditimbang, total_stunting, f"{avg_stunting:.2f}%",
                             total_kurang_gizi, total_wasting, len(df_agg)]
                }
                df_summary = pd.DataFrame(summary_data)
                csv_summary = df_summary.to_csv(index=False)
                
                st.download_button(
                    label="📥 Download Ringkasan Statistik",
                    data=csv_summary,
                    file_name="ringkasan_statistik.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            st.markdown("---")
            
            # Informasi file
            st.markdown("#### ℹ️ Informasi File")
            st.markdown("""
            - **Fact Gizi Balita**: Tabel fakta berisi semua data gizi per puskesmas/kecamatan
            - **Dimensi Wilayah**: Daftar puskesmas dan kecamatan
            - **Dimensi Waktu**: Informasi waktu pengambilan data
            - **Data Agregat**: Ringkasan data per kecamatan (sudah diagregasi)
            - **Ringkasan Statistik**: Statistik umum untuk laporan
            """)
            
            st.success("✅ Semua file dalam format CSV, mudah dibuka di Excel atau aplikasi lainnya!")
        
        # Footer
        st.markdown("---")
        waktu_info = f"{df_waktu['tanggal'].iloc[0]} {df_waktu['bulan'].iloc[0]} {df_waktu['tahun'].iloc[0]}, Pukul {df_waktu['jam'].iloc[0]:02d}:{df_waktu['menit'].iloc[0]:02d}"
        
        st.markdown(f"""
        <div style='text-align: center; padding: 2rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    border-radius: 10px; color: white; margin-top: 2rem;'>
            <h3 style='margin: 0;'>📅 Data Terakhir Diperbarui</h3>
            <p style='font-size: 1.3rem; margin: 0.5rem 0;'><b>{waktu_info}</b></p>
            <p style='margin: 0.5rem 0;'>Dinas Kesehatan Kabupaten Kuningan</p>
            <p style='margin: 0; font-size: 0.9rem;'>Sistem Informasi Analisis Data Stunting</p>
        </div>
        """, unsafe_allow_html=True)
    
    else:
        st.error(f"❌ {message}")
        st.info("Pastikan file Excel memiliki format yang benar dan sheet 'STATUS GIZI' tersedia.")
        st.markdown("""
        ### 🔧 Tips Troubleshooting:
        - Pastikan nama sheet adalah "STATUS GIZI"
        - Periksa format tanggal di sel A2
        - Pastikan data dimulai dari baris ke-6
        - Cek apakah semua kolom tersedia
        """)