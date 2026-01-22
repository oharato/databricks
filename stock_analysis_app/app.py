import concurrent.futures
import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

from utils import IS_SQL_MODE, TICKER_DELIMITER
from session_store import init_session_state
from data_provider import load_stock_list, load_and_process_data
from components import render_chart

# ページ設定
st.set_page_config(layout="wide")
title = "Stock Price Analysis App " + ("(Local SQL Mode)" if IS_SQL_MODE else "(Databricks Apps)")
st.title(title)

# 初期化
init_session_state()

# 1. マスタデータの読み込み
df_stock_list = load_stock_list()

# クエリパラメータの処理 (初回アクセス時のみ)
if st.session_state.get("first_run", False) and "tickers" in st.query_params:
    tickers_param = st.query_params["tickers"]
    if isinstance(tickers_param, list):
        tickers_str = tickers_param[0]
    else:
        tickers_str = tickers_param

    if tickers_str:
        query_codes = [t.strip() for t in tickers_str.split(TICKER_DELIMITER) if t.strip()]
        
        if query_codes and not df_stock_list.empty:
            available_codes_str = df_stock_list['code'].astype(str)
            valid_mask = available_codes_str.isin(query_codes)
            valid_codes = df_stock_list.loc[valid_mask, 'code'].tolist()

            if valid_codes:
                # カレントリストを上書きしてチャート表示を有効化
                current_list = st.session_state.user_data["current_list"]
                st.session_state.user_data["lists"][current_list] = valid_codes
                st.session_state.data_loaded = True
                st.sidebar.success(f"Loaded from URL: {tickers_str}")
            else:
                st.sidebar.warning(f"No valid stock codes found in: {tickers_str}")

# --- サイドバー UI ---
st.sidebar.header("Configuration")

# 1. お気に入りリスト管理
st.sidebar.subheader("Favorite Lists")
user_data = st.session_state.user_data
current_list_name = st.sidebar.selectbox(
    "Select List",
    options=list(user_data["lists"].keys()),
    index=list(user_data["lists"].keys()).index(user_data.get("current_list", "Default")) if user_data.get("current_list") in user_data["lists"] else 0,
    key="list_selector"
)

# リスト切り替え時の処理
if current_list_name != user_data["current_list"]:
    user_data["current_list"] = current_list_name
    st.rerun()

# リスト操作用UI
with st.sidebar.expander("Manage Lists"):
    new_list_name = st.text_input("New List Name")
    col_add, col_del = st.columns(2)
    if col_add.button("Create List"):
        if new_list_name and new_list_name not in user_data["lists"]:
            user_data["lists"][new_list_name] = []
            user_data["current_list"] = new_list_name
            st.rerun()
    
    if col_del.button("Delete List"):
        if len(user_data["lists"]) > 1:
            del user_data["lists"][current_list_name]
            user_data["current_list"] = list(user_data["lists"].keys())[0]
            st.rerun()
        else:
            st.sidebar.warning("Cannot delete the last list.")

# 2. 銘柄検索と選択
st.sidebar.subheader("Stock Selection")

# フィルタリング機能
if not df_stock_list.empty:
    # フィルタ用コンテナ
    with st.sidebar.expander("Filter Options", expanded=False):
        # 市場フィルタ
        markets = sorted(df_stock_list['market'].dropna().unique())
        selected_markets = st.multiselect("Market", markets)
        
        # 33業種フィルタ
        sectors = sorted(df_stock_list['sector33'].dropna().unique())
        selected_sectors = st.multiselect("Sector (33)", sectors)
        
        # 17業種フィルタ
        sub_sectors = sorted(df_stock_list['sector17'].dropna().unique())
        selected_sub_sectors = st.multiselect("Sector (17)", sub_sectors)

    # DataFrameのフィルタリング
    df_filtered = df_stock_list.copy()
    if selected_markets:
        df_filtered = df_filtered[df_filtered['market'].isin(selected_markets)]
    if selected_sectors:
        df_filtered = df_filtered[df_filtered['sector33'].isin(selected_sectors)]
    if selected_sub_sectors:
        df_filtered = df_filtered[df_filtered['sector17'].isin(selected_sub_sectors)]
    
    # 選択肢の作成
    options_map = dict(zip(df_filtered['label'], df_filtered['code']))
    
    # 現在の選択済みコードを取得
    current_codes = user_data["lists"][current_list_name]
    default_labels = df_stock_list[df_stock_list['code'].isin(current_codes)]['label'].tolist()

    # defaultにoptionsに含まれない値があるとエラーになるためフィルタリング
    valid_options = set(options_map.keys())
    filtered_default_labels = [label for label in default_labels if label in valid_options]

    # マルチセレクト
    selected_labels = st.sidebar.multiselect(
        "Search & Select Stocks",
        options=options_map.keys(),
        default=filtered_default_labels,
        placeholder="Type code or name..."
    )
    
    # 保存ロジック
    visible_selected_codes = [options_map[label] for label in selected_labels if label in options_map]
    
    # 表示されていないが選択されていたコードを保持
    visible_codes = set(df_filtered['code'])
    hidden_selected_codes = [code for code in current_codes if code not in visible_codes]
    
    new_selected_codes = visible_selected_codes + hidden_selected_codes
    
    if set(new_selected_codes) != set(current_codes):
        user_data["lists"][current_list_name] = new_selected_codes
        if "data_loaded" in st.session_state:
            del st.session_state.data_loaded
        st.rerun()

else:
    st.sidebar.warning("Stock list is empty or failed to load.")
    new_selected_codes = user_data["lists"].get(current_list_name, [])

# 表示ボタン
st.sidebar.markdown("---")
if st.sidebar.button("Display Charts", type="primary"):
    st.session_state.data_loaded = True
    st.query_params["tickers"] = TICKER_DELIMITER.join(map(str, new_selected_codes))

show_charts = st.session_state.get("data_loaded", False)

# --- メインコンテンツ描画 ---

with st.spinner('Loading data...'):
    if show_charts and new_selected_codes:
        interval_configs = [("MONTHLY", 3000), ("WEEKLY", 600), ("DAILY", 120)]
        
        # 選択された各銘柄についてループ
        for target_code in new_selected_codes:
            # 銘柄情報の取得
            stock_info = df_stock_list[df_stock_list['code'] == target_code].iloc[0] if not df_stock_list.empty else None
            stock_name = stock_info['name'] if stock_info is not None else target_code
            
            st.markdown(f"### {target_code}: {stock_name}")
            
            # 3つのカラムを作成
            cols = st.columns(3)
            
            # 並列実行のためのExecutor
            # メインスレッドのコンテキストを取得
            ctx = get_script_run_ctx()

            def run_with_context(ctx, func, *args, **kwargs):
                # 実行スレッドにコンテキストを設定
                add_script_run_ctx(ctx=ctx)
                return func(*args, **kwargs)

            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                future_to_index = {}
                for i, (interval, days) in enumerate(interval_configs):
                    # ラッパー関数を経由して実行することでコンテキストを伝播
                    future = executor.submit(run_with_context, ctx, load_and_process_data, target_code, interval, days)
                    future_to_index[future] = i

                for future in concurrent.futures.as_completed(future_to_index):
                    i = future_to_index[future]
                    interval, days = interval_configs[i]
                    
                    try:
                        data = future.result()
                    except Exception as e:
                        data = None
                        print(f"Error processing {target_code} ({interval}): {e}")

                    with cols[i]:
                        if data is not None and not data.empty:
                            render_chart(data, f'{interval}')
                        else:
                            st.info(f"No data ({interval})")
            
            st.markdown("---")
    elif not new_selected_codes:
        st.info("Please select stocks from the sidebar and click 'Display Charts'.")
    else:
        st.info("Click 'Display Charts' to view analysis.")
