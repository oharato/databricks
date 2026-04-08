import streamlit as st
import pandas as pd
from data_provider import load_stock_list, load_bulk_multi_interval_data
from components import render_chart_compact
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

# 監視用に表示期間を短縮（SPEC: MONTHLY=3000d, WEEKLY=600d, DAILY=180d）
INTERVALS: tuple = (
    ("MONTHLY", 365 * 3),   # 約3年（月足）
    ("WEEKLY",  365),       # 約1年（週足）
    ("DAILY",   180),       # 約6か月（日足）
)

# バルク取得: 全銘柄を 1 クエリで取得
codes_tuple = tuple(sorted(s['code'] for s in stocks))

with st.spinner(f"{len(codes_tuple)} 銘柄のデータを取得中…（初回は時間がかかります）"):
    all_data: dict = load_bulk_multi_interval_data(codes_tuple, INTERVALS)

st.caption(f"取得銘柄数: {len(codes_tuple)}")
st.subheader("チャート一覧")


@st.fragment
def render_sectors(stocks_list: list, data_map: dict) -> None:
    """セクターごとにチャートを描画する。st.fragment でスコープを分離し全体リランを防ぐ。"""
    current_sector = None

    for s in stocks_list:
        code = s['code']
        name = s['name']
        sector = s['sector33']

        if sector != current_sector:
            if current_sector is not None:
                st.divider()
            st.markdown(f"## ■ {sector}")
            current_sector = sector

        st.markdown(f"#### {code}: {name}")

        data = data_map.get(str(code))
        if data:
            cols = st.columns(3)
            labels = {"MONTHLY": "月足", "WEEKLY": "週足", "DAILY": "日足"}
            for i, (interval, _) in enumerate(INTERVALS):
                with cols[i]:
                    df_chart = data.get(interval)
                    if df_chart is not None and not df_chart.empty:
                        render_chart_compact(df_chart, labels[interval], height=250)
                    else:
                        st.caption(f"{labels[interval]}データなし")
        else:
            st.warning(f"{name} のデータは取得できませんでした。")
        st.write("")


render_sectors(stocks, all_data)
st.divider()
