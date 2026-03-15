import streamlit as st
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from data_provider import load_stock_list, load_multi_interval_data_threadsafe
from components import render_chart
from session_store import init_session_state

st.set_page_config(layout="wide", page_title="各セクター代表銘柄 チャート", page_icon="📈")

init_session_state()

st.title("国内プライム 33業種 代表銘柄チャート")
st.markdown("各セクター（33業種）から代表的な銘柄（時価総額ベース・最大5銘柄）を抽出し、月足・週足・日足チャートを並べて表示しています。")

@st.cache_data(ttl=3600)
def get_sector_representatives():
    df = load_stock_list()
    prime_df = df[df['market'].str.contains('プライム', na=False)]
    valid_sectors = prime_df.dropna(subset=['sector33']).copy()
    
    # 時価総額の代理としてTOPIXの規模(scale)を利用して順位付け
    scale_map = {
        'TOPIX Core30': 1,
        'TOPIX Large70': 2,
        'TOPIX Mid400': 3,
        'TOPIX Small 1': 4,
        'TOPIX Small 2': 5,
        '-': 6
    }
    valid_sectors['scale_rank'] = valid_sectors['scale'].map(scale_map).fillna(6)
    
    # 業種ごと、規模順(scale_rank) -> コード順にソートし、各業種5件取得
    reps = valid_sectors.sort_values(['sector33', 'scale_rank', 'code'])
    reps = reps.groupby('sector33').head(5).reset_index()
    return reps

sector_reps_df = get_sector_representatives()
stocks = sector_reps_df[['code', 'name', 'sector33']].to_dict('records')

if not stocks:
    st.warning("対象となる銘柄が見つかりませんでした。")
    st.stop()

days_daily = 365
days_weekly = 365 * 3
days_monthly = 365 * 6
intervals = [
    ("MONTHLY", days_monthly),
    ("WEEKLY", days_weekly),
    ("DAILY", days_daily)
]

st.info(f"{len(stocks)}セクターのデータを取得しています...")

@st.cache_data(ttl=3600*12, show_spinner=False)
def fetch_all_chart_data(stocks_list, intervals_list):
    res_dict = {}
    with ThreadPoolExecutor(max_workers=min(30, len(stocks_list))) as executor:
        futures = {
            executor.submit(load_multi_interval_data_threadsafe, s['code'], intervals_list): s
            for s in stocks_list
        }
        for future in futures:
            s = futures[future]
            try:
                res = future.result()
                res_dict[s['code']] = res
            except Exception as e:
                print(f"Error fetching data for {s['code']}: {e}")
                res_dict[s['code']] = None
    return res_dict

with st.spinner("データ取得中（初回は時間がかかります）..."):
    results_dict = fetch_all_chart_data(stocks, intervals)

st.subheader("チャート一覧")

# セクターごとにグループ化して描画
current_sector = None

for s in stocks:
    code = s['code']
    name = s['name']
    sector = s['sector33']
    
    # セクターが変わったら大見出しを表示
    if sector != current_sector:
        if current_sector is not None:
            st.divider() # 前のセクターとの区切り
        st.markdown(f"## ■ {sector}")
        current_sector = sector
    
    st.markdown(f"#### {code}: {name}")
    
    data = results_dict.get(code)
    if data:
        cols = st.columns(3)
        
        with cols[0]:
            df_monthly = data.get("MONTHLY")
            if df_monthly is not None and not df_monthly.empty:
                render_chart(df_monthly, "月足", height=300)
            else:
                st.write("月足データがありません")
                
        with cols[1]:
            df_weekly = data.get("WEEKLY")
            if df_weekly is not None and not df_weekly.empty:
                render_chart(df_weekly, "週足", height=300)
            else:
                st.write("週足データがありません")
                
        with cols[2]:
            df_daily = data.get("DAILY")
            if df_daily is not None and not df_daily.empty:
                render_chart(df_daily, "日足", height=300)
            else:
                st.write("日足データがありません")
    else:
        st.warning(f"{name} のデータは取得できませんでした。")
    st.write("") # 銘柄間の余白

# 最後に一番下の区切り線
st.divider()
