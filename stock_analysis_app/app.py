import streamlit as st

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

# クエリパラメータの処理（URL直接指定時の銘柄読み込み）
if "tickers" in st.query_params:
    tickers_param = st.query_params["tickers"]
    if isinstance(tickers_param, list):
        tickers_str = tickers_param[0]
    else:
        tickers_str = tickers_param

    if tickers_str:
        query_codes = [t.strip() for t in tickers_str.split(TICKER_DELIMITER) if t.strip()]
        
        # 前回処理したURLパラメータと異なる場合のみ処理
        last_url_param = st.session_state.get("last_url_tickers", "")
        if tickers_str != last_url_param and query_codes and not df_stock_list.empty:
            # コードの型を揃える（文字列として比較）
            available_codes_str = df_stock_list['code'].astype(str)
            valid_mask = available_codes_str.isin(query_codes)
            # 元の型（おそらく整数）で取得
            valid_codes = df_stock_list.loc[valid_mask, 'code'].tolist()

            if valid_codes:
                # カレントリストを上書きしてチャート表示を有効化
                current_list = st.session_state.user_data["current_list"]
                st.session_state.user_data["lists"][current_list] = valid_codes
                
                # session_stateのstock_{code}キーも更新
                for code in valid_codes:
                    st.session_state[f"stock_{code}"] = True
                
                st.session_state.data_loaded = True
                st.session_state.last_url_tickers = tickers_str
                st.sidebar.success(f"✅ Loaded {len(valid_codes)} stock(s) from URL: {tickers_str}")
            else:
                st.sidebar.warning(f"⚠️ No valid stock codes found in: {tickers_str}")

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
    # ボタン風チェックボックスのスタイル
    st.sidebar.markdown(
        """
        <style>
        /* チェックボックスのアイコンを非表示 */
        div[data-testid="stCheckbox"] input[type="checkbox"] {
            display: none;
        }
        
        /* ラベルをボタン風に */
        div[data-testid="stCheckbox"] label {
            cursor: pointer;
            border: 2px solid #e1e4e8;
            border-radius: 6px;
            padding: 0.35rem 0.75rem;
            margin: 0.25rem 0.25rem 0.25rem 0;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: #f6f8fa;
            color: #24292f;
            font-size: 0.9rem;
            font-weight: 500;
            transition: all 0.2s ease;
            min-width: 60px;
            text-align: center;
        }
        
        /* ホバー時 */
        div[data-testid="stCheckbox"] label:hover {
            background: #e9ecef;
            border-color: #adb5bd;
            transform: translateY(-1px);
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        /* チェック時 */
        div[data-testid="stCheckbox"] label:has(input:checked) {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-color: #667eea;
            color: #ffffff;
            font-weight: 600;
            box-shadow: 0 4px 8px rgba(102, 126, 234, 0.3);
        }
        
        /* チェック時のホバー */
        div[data-testid="stCheckbox"] label:has(input:checked):hover {
            background: linear-gradient(135deg, #5568d3 0%, #653a8a 100%);
            transform: translateY(-1px);
            box-shadow: 0 6px 12px rgba(102, 126, 234, 0.4);
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    def render_checkbox_group(label, options, key_prefix, columns=3):
        st.markdown(f"**{label}**")
        if not options:
            return []
        cols = st.columns(columns)
        selected = []
        for i, option in enumerate(options):
            key = f"{key_prefix}_{option}"
            col = cols[i % columns]
            if col.checkbox(str(option), key=key):
                selected.append(option)
        return selected

    markets = sorted(df_stock_list['market'].dropna().unique())
    sectors = sorted(df_stock_list['sector33'].dropna().unique())
    sub_sectors = sorted(df_stock_list['sector17'].dropna().unique())

    st.session_state.setdefault("stock_search", "")

    def clear_filter_state(markets, sectors, sub_sectors):
        for market in markets:
            st.session_state[f"market_{market}"] = False
        for sector in sectors:
            st.session_state[f"sector33_{sector}"] = False
        for sub_sector in sub_sectors:
            st.session_state[f"sector17_{sub_sector}"] = False
        st.session_state["stock_search"] = ""

    with st.sidebar.form("filter_form", border=False):
        # フィルタ用コンテナ（デフォルトで展開）
        with st.expander("🔍 Filter Options", expanded=True):
            # 市場フィルタ
            selected_markets = render_checkbox_group("Market", markets, "market", columns=2)

            # 33業種フィルタ
            selected_sectors = render_checkbox_group("Sector (33)", sectors, "sector33", columns=2)

            # 17業種フィルタ
            selected_sub_sectors = render_checkbox_group("Sector (17)", sub_sectors, "sector17", columns=2)

        st.markdown("### 🔎 Search & Select Stocks")

        # 検索ボックス
        search_query = st.text_input(
            "Search by code or name",
            key="stock_search",
            placeholder="Type to filter stocks...",
            label_visibility="collapsed"
        )

        form_cols = st.columns(2)
        form_cols[0].form_submit_button("Apply Filters", use_container_width=True)
        form_cols[1].form_submit_button(
            "Clear Filters",
            use_container_width=True,
            on_click=clear_filter_state,
            args=(markets, sectors, sub_sectors)
        )

    # DataFrameのフィルタリング
    df_filtered = df_stock_list.copy()
    if selected_markets:
        df_filtered = df_filtered[df_filtered['market'].isin(selected_markets)]
    if selected_sectors:
        df_filtered = df_filtered[df_filtered['sector33'].isin(selected_sectors)]
    if selected_sub_sectors:
        df_filtered = df_filtered[df_filtered['sector17'].isin(selected_sub_sectors)]
    
    # 選択肢の作成（フィルタ済みのデータから）
    options_map = dict(zip(df_filtered['label'], df_filtered['code']))
    
    # 現在の選択済みコードを取得
    current_codes = user_data["lists"][current_list_name]
    
    # Search & Select Stocks セクション
    st.sidebar.markdown("---")
    
    # フィルタリング状況を明確に表示
    total_stocks = len(df_stock_list)
    filtered_stocks = len(df_filtered)
    
    if selected_markets or selected_sectors or selected_sub_sectors:
        filter_info = []
        if selected_markets:
            filter_info.append(f"Market: {', '.join(selected_markets)}")
        if selected_sectors:
            filter_info.append(f"Sector33: {', '.join(selected_sectors[:2])}{'...' if len(selected_sectors) > 2 else ''}")
        if selected_sub_sectors:
            filter_info.append(f"Sector17: {', '.join(selected_sub_sectors[:2])}{'...' if len(selected_sub_sectors) > 2 else ''}")
        
        st.sidebar.success(f"📊 **Showing {filtered_stocks} stocks** (filtered from {total_stocks})")
        with st.sidebar.expander("Active Filters", expanded=False):
            for info in filter_info:
                st.markdown(f"- {info}")
    else:
        st.sidebar.info(f"📊 **Showing all {total_stocks} stocks**")
    
    # 検索によるフィルタリング
    filtered_options = options_map.items()
    if search_query:
        query_lower = search_query.lower()
        filtered_options = [
            (label, code) for label, code in filtered_options
            if query_lower in str(label).lower() or query_lower in str(code).lower()
        ]
    else:
        filtered_options = list(filtered_options)
    
    # パフォーマンス改善：表示件数を制限
    MAX_DISPLAY = 100
    total_filtered = len(filtered_options)
    if total_filtered > MAX_DISPLAY:
        st.sidebar.warning(f"⚠️ Too many results ({total_filtered}). Showing first {MAX_DISPLAY}. Please refine your search.")
        filtered_options = filtered_options[:MAX_DISPLAY]
    
    # 一括選択/クリアボタン
    col_select, col_clear = st.sidebar.columns(2)
    if col_select.button("✓ Select All", use_container_width=True):
        for label, code in filtered_options:
            st.session_state[f"stock_{code}"] = True
        st.rerun()
    if col_clear.button("✗ Clear All", use_container_width=True):
        for label, code in filtered_options:
            st.session_state[f"stock_{code}"] = False
        st.rerun()
    
    # 選択数の表示
    selected_count = sum(1 for _, code in filtered_options if st.session_state.get(f"stock_{code}", code in current_codes))
    st.sidebar.markdown(f"**Selected:** {selected_count} / {len(filtered_options)} stocks")
    
    # 銘柄リストを折りたたみ可能に（パフォーマンス改善）
    with st.sidebar.expander("📋 Stock List", expanded=False):
        for label, code in filtered_options:
            key = f"stock_{code}"
            # 初期値：既に選択されているか確認
            if key not in st.session_state:
                st.session_state[key] = code in current_codes
            
            # 選択状態に応じたボタンスタイル
            is_selected = st.session_state[key]
            button_label = f"{'✓ ' if is_selected else ''}{label}"
            button_type = "primary" if is_selected else "secondary"
            
            # ボタンをクリックすると選択状態をトグル
            if st.button(button_label, key=f"btn_{code}", use_container_width=True, type=button_type):
                st.session_state[key] = not st.session_state[key]
                # 選択リストの更新のみ行い、不要なrerunを避ける
                if st.session_state[key]:
                    if code not in current_codes:
                        user_data["lists"][current_list_name].append(code)
                else:
                    if code in user_data["lists"][current_list_name]:
                        user_data["lists"][current_list_name].remove(code)
                if "data_loaded" in st.session_state:
                    del st.session_state.data_loaded
    
    # 選択済み銘柄を収集（expanderの外で計算）
    # session_stateから選択状態を確認
    visible_selected_codes = [code for code in df_filtered['code'] if st.session_state.get(f"stock_{code}", False)]
    
    # 保存ロジック（最適化版）
    # 表示されていないが選択されていたコードを保持
    visible_codes = set(df_filtered['code'])
    hidden_selected_codes = [code for code in current_codes if code not in visible_codes]
    
    # visible_selected_codesとhidden_selected_codesを統合
    new_selected_codes = visible_selected_codes + hidden_selected_codes
    
    # リストが変更された場合のみ更新（不要な処理を削減）
    if set(new_selected_codes) != set(current_codes):
        user_data["lists"][current_list_name] = new_selected_codes

else:
    st.sidebar.warning("Stock list is empty or failed to load.")
    # フォールバック: リストにある銘柄を使用
    new_selected_codes = user_data["lists"].get(current_list_name, [])

# 表示ボタン
st.sidebar.markdown("---")

# デバッグ情報を表示
if new_selected_codes:
    st.sidebar.caption(f"📋 {len(new_selected_codes)} stock(s) in current list")

if st.sidebar.button("📈 Display Charts", type="primary", use_container_width=True):
    st.session_state.data_loaded = True
    st.query_params["tickers"] = TICKER_DELIMITER.join(map(str, new_selected_codes))

show_charts = st.session_state.get("data_loaded", False)

# --- メインコンテンツ描画 ---

if show_charts and new_selected_codes:
    # パフォーマンス改善：表示銘柄数を制限
    MAX_STOCKS_DISPLAY = 100
    display_codes = new_selected_codes[:MAX_STOCKS_DISPLAY]
    
    if len(new_selected_codes) > MAX_STOCKS_DISPLAY:
        st.warning(f"⚠️ Displaying first {MAX_STOCKS_DISPLAY} of {len(new_selected_codes)} selected stocks for performance. Please reduce selection for faster loading.")
    
    stock_name_map = {}
    if not df_stock_list.empty:
        stock_name_map = dict(zip(df_stock_list["code"], df_stock_list["name"]))

    label_to_code = {
        f"{code}: {stock_name_map.get(code, code)}": code
        for code in display_codes
    }

    default_labels = list(label_to_code.keys())[:5]
    selected_labels = st.multiselect(
        "Stocks to render",
        options=list(label_to_code.keys()),
        default=default_labels
    )
    selected_codes = [label_to_code[label] for label in selected_labels]

    if not selected_codes:
        st.info("💡 Select at least one stock to load charts.")
        selected_codes = []
    else:
        st.info(f"📊 Loading charts for {len(selected_codes)} stock(s)...")
    
    interval_configs = [("MONTHLY", 3000), ("WEEKLY", 600), ("DAILY", 120)]
    
    # 選択された各銘柄についてループ
    for idx, target_code in enumerate(selected_codes, 1):
        stock_name = stock_name_map.get(target_code, target_code)

        with st.container(border=True):
            st.subheader(f"📈 {idx}/{len(selected_codes)} - {target_code}: {stock_name}")

            # 3つのカラムを作成
            cols = st.columns(3)

            for i, (interval, days) in enumerate(interval_configs):
                try:
                    data = load_and_process_data(target_code, interval, days)
                except Exception as e:
                    data = None

                with cols[i]:
                    if data is not None and not data.empty:
                        render_chart(data, f"{interval}")
                    else:
                        st.caption(f"No data ({interval})")
elif not new_selected_codes:
    st.info("💡 Please select stocks from the sidebar and click 'Display Charts'.")
else:
    st.info("💡 Click 'Display Charts' to view analysis.")
