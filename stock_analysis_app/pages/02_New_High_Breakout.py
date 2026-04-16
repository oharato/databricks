import streamlit as st
import pandas as pd
from data_provider import load_new_high_breakout, load_stock_list
from session_store import init_session_state

st.set_page_config(layout="wide", page_title="新高値ブレイクスキャン", page_icon="🚀")

init_session_state()

st.title("🚀 新高値ブレイクスキャン")
st.markdown(
    "直近の取引日において「過去の上値抵抗線を出来高急増で突破した銘柄」を一覧表示します。"
)

# ---------------------------------------------------------------------------
# サイドバー: パラメータ設定
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("スクリーニング条件")

    resistance_days = st.slider(
        "新高値の基準期間（営業日）",
        min_value=60,
        max_value=500,
        value=250,
        step=10,
        help="この期間の最高値を終値が上回るとブレイクアウトと判定します（約52週 = 250営業日）",
    )

    volume_lookback = st.slider(
        "平均出来高の基準期間（営業日）",
        min_value=10,
        max_value=120,
        value=50,
        step=5,
        help="この期間の平均出来高に対する倍率で出来高急増を判定します",
    )

    volume_ratio = st.slider(
        "出来高急増のしきい値（倍）",
        min_value=1.0,
        max_value=5.0,
        value=1.5,
        step=0.1,
        format="%.1f",
        help="当日出来高が平均のこの倍数以上の場合のみ表示します",
    )

    st.divider()
    st.subheader("絞り込み（オプション）")

    # 市場フィルタ
    market_options = ["すべて", "プライム", "スタンダード", "グロース"]
    selected_market = st.selectbox("市場", market_options, index=0)
    market_filter = None if selected_market == "すべて" else selected_market

    # セクターフィルタ（stock_list から動的取得）
    @st.cache_data(ttl=3600)
    def get_sectors():
        df = load_stock_list()
        if df.empty or "sector33" not in df.columns:
            return []
        return sorted(df["sector33"].dropna().unique().tolist())

    sectors = get_sectors()
    sector_options = ["すべて"] + sectors
    selected_sector = st.selectbox("セクター（東証33業種）", sector_options, index=0)
    sector_filter = None if selected_sector == "すべて" else selected_sector

    st.divider()
    scan_button = st.button("🔍 スキャン実行", use_container_width=True, type="primary")

# ---------------------------------------------------------------------------
# メインエリア: スキャン結果
# ---------------------------------------------------------------------------
if scan_button or "nhb_result" in st.session_state:
    if scan_button:
        with st.spinner("スキャン中…（初回は時間がかかります）"):
            df = load_new_high_breakout(
                resistance_days=resistance_days,
                volume_lookback=volume_lookback,
                volume_ratio=volume_ratio,
                market_filter=market_filter,
                sector_filter=sector_filter,
            )
        st.session_state["nhb_result"] = df
        st.session_state["nhb_params"] = {
            "resistance_days": resistance_days,
            "volume_lookback": volume_lookback,
            "volume_ratio": volume_ratio,
            "market_filter": market_filter,
            "sector_filter": sector_filter,
        }
    else:
        df = st.session_state["nhb_result"]

    if df.empty:
        st.info("条件に合う銘柄が見つかりませんでした。条件を緩めて再スキャンしてください。")
        st.stop()

    # サマリー
    params = st.session_state.get("nhb_params", {})
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("ブレイクアウト銘柄数", f"{len(df)} 銘柄")
    if "breakout_date" in df.columns and not df.empty:
        col2.metric("基準日", df["breakout_date"].iloc[0])
    col3.metric("新高値基準期間", f"{params.get('resistance_days', resistance_days)} 営業日")
    col4.metric("出来高しきい値", f"{params.get('volume_ratio', volume_ratio):.1f} 倍")

    st.divider()

    # 表示用カラム整形
    display_df = df.copy()
    if "breakout_pct" in display_df.columns:
        display_df["breakout_pct"] = display_df["breakout_pct"].map(
            lambda x: f"{x * 100:+.2f}%" if pd.notna(x) else "-"
        )
    if "volume_ratio_actual" in display_df.columns:
        display_df["volume_ratio_actual"] = display_df["volume_ratio_actual"].map(
            lambda x: f"{x:.2f}x" if pd.notna(x) else "-"
        )
    if "breakout_close" in display_df.columns:
        display_df["breakout_close"] = display_df["breakout_close"].map(
            lambda x: f"¥{x:,.0f}" if pd.notna(x) else "-"
        )
    if "resistance_high" in display_df.columns:
        display_df["resistance_high"] = display_df["resistance_high"].map(
            lambda x: f"¥{x:,.0f}" if pd.notna(x) else "-"
        )
    if "breakout_volume" in display_df.columns:
        display_df["breakout_volume"] = display_df["breakout_volume"].map(
            lambda x: f"{int(x):,}" if pd.notna(x) else "-"
        )

    column_labels = {
        "code": "コード",
        "name": "銘柄名",
        "market": "市場",
        "sector33": "セクター",
        "breakout_date": "ブレイク日",
        "breakout_close": "終値",
        "resistance_high": f"過去{params.get('resistance_days', resistance_days)}日高値",
        "breakout_pct": "突破率",
        "breakout_volume": "出来高",
        "volume_ratio_actual": "出来高倍率",
    }

    show_cols = [c for c in column_labels if c in display_df.columns]
    display_df = display_df[show_cols].rename(columns=column_labels)

    st.subheader(f"スキャン結果（{len(df)} 銘柄）")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # CSV ダウンロード
    csv = df.to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        label="📥 CSV ダウンロード",
        data=csv,
        file_name=f"new_high_breakout_{df['breakout_date'].iloc[0] if not df.empty else 'result'}.csv",
        mime="text/csv",
    )

    # メインページでチャートを表示
    st.divider()
    st.subheader("📈 チャートで確認")
    st.caption(f"{len(df)} 銘柄のコードをメインページに渡してチャートを一括表示します。")

    if st.button("📈 メインページでチャートを表示", type="primary", use_container_width=True):
        codes = df["code"].astype(str).tolist()
        # session_state に直接セットして app.py へ遷移（query_params は switch_page でリセットされるため）
        current_list = st.session_state.user_data["current_list"]
        st.session_state.user_data["lists"][current_list] = codes
        for code in codes:
            st.session_state[f"stock_{code}"] = True
        st.session_state.data_loaded = True
        st.session_state.last_url_tickers = ""  # app.py の URL パラメータ処理をスキップさせない
        st.switch_page("app.py")

else:
    st.info("👈 サイドバーでスクリーニング条件を設定し、「スキャン実行」ボタンを押してください。")

    with st.expander("📖 スクリーニング条件の説明"):
        st.markdown("""
        ### 新高値ブレイク投資法とは

        過去の「上値抵抗線（高値）」を突き抜けた銘柄を順張りで狙う手法です。

        #### 判定ロジック
        1. **新高値ブレイク**: 当日の終値 > 過去 N 営業日の最高値
        2. **出来高急増**: 当日の出来高 ≥ 過去 M 日平均出来高 × しきい値

        #### なぜ有効か
        - 新高値圏では「やれやれ売り（含み損からの逃げ売り）」が存在しない
        - 機関投資家などの大口参入を出来高急増で確認できる
        - CAN SLIM（ウィリアム・オニール）などで実績のある順張り戦略

        #### デフォルト設定
        | 条件 | デフォルト |
        |------|-----------|
        | 新高値の基準期間 | 250 営業日（約 52 週） |
        | 出来高基準期間 | 50 営業日 |
        | 出来高急増しきい値 | 1.5 倍 |
        """)
