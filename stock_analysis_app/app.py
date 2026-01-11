import streamlit as st
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import datetime
from lightweight_charts.widgets import StreamlitChart

# ページ設定
st.set_page_config(layout="wide")
st.title("Stock Price Analysis App (Databricks)")

# Sparkの初期化
# Databricks Apps環境では、databricks-connectを使用する場合、認証は自動的に処理されます
def get_spark():
    session = DatabricksSession.builder.serverless().getOrCreate()
    try:
        # INACTIVITY_TIMEOUTを処理するためのセッション検証
        session.sql("SELECT 1").collect()
    except Exception as e:
        if "INACTIVITY_TIMEOUT" in str(e) or "session_id is no longer usable" in str(e):
            # print("セッションがタイムアウトしました。再接続中...")
            session.stop()
            session = DatabricksSession.builder.serverless().getOrCreate()
        else:
             # 別のエラーの場合は、バブルアップさせるか処理する可能性があります
             # 今のところはログを出力してセッションを返すか、例外を発生させます
             st.error(f"Spark接続チェック失敗: {e}")
             raise e
    return session

try:
    spark = get_spark()


except Exception as e:
    st.error(f"Failed to connect to Databricks: {e}")
    st.stop()

# --- 設定 ---
# 実際のシナリオでは、テーブルから一意のコードをクエリする場合があります
TARGET_CODES = ['1301', '3031'] 

# サイドバーコントロール
st.sidebar.header("Configuration")
selected_code = st.sidebar.selectbox("Select Stock Code", TARGET_CODES)
# 複数のチャートビューはすべてのインターバルを表示することを意味するため、単一のインターバルセレクターを削除します
# interval = st.sidebar.selectbox("Interval", ["DAILY", "WEEKLY", "MONTHLY"])
# 日数設定はインターバルタイプごとに固定されました

# 1. データの読み込みと処理
@st.cache_data(ttl=3600)
def load_and_process_data(code, interval, days):
    try:
        # テーブルのロード
        table_name = "main.default.stock_prices"
        df = spark.table(table_name).filter(F.col("code") == code)
        
        # sample.mdからの日付変換ロジック
        if "dateString" in df.columns:
            df = df.withColumn("trade_date", F.to_timestamp(F.col("dateString"), "yyyy-MM-dd"))
        elif "date" in df.columns:
            df = df.withColumn("trade_date", (F.col("date").cast("double") / 1000).cast("timestamp"))
        else:
            # カラムが異なる場合のフォールバックまたはエラー
            pass

        # 集計ロジック
        if interval == 'DAILY':
            df_agg = df.select("code", "trade_date", "open", "high", "low", "close", "volume")
        elif interval == 'WEEKLY':
            df_agg = df.withColumn("year_week", F.date_trunc("week", "trade_date")) \
                .groupBy("code", "year_week").agg(
                    F.first("open").alias("open"), F.max("high").alias("high"), F.min("low").alias("low"), F.last("close").alias("close"),
                    F.sum("volume").alias("volume"), F.min("trade_date").alias("trade_date")
                )
        elif interval == 'MONTHLY':
            df_agg = df.withColumn("year_month", F.date_trunc("month", "trade_date")) \
                .groupBy("code", "year_month").agg(
                    F.first("open").alias("open"), F.max("high").alias("high"), F.min("low").alias("low"), F.last("close").alias("close"),
                    F.sum("volume").alias("volume"), F.min("trade_date").alias("trade_date")
                )
        
        # 移動平均
        w = Window.partitionBy("code").orderBy("trade_date")
        for window_size in [5, 25, 75]:
            df_agg = df_agg.withColumn(f"ma_{window_size}", F.avg("close").over(w.rowsBetween(-window_size + 1, 0)))

        # 日付によるフィルタリング
        # 最初に最大日付を計算する必要があります。
        # 注: Streamlit/Sparkアプリでは、必要なものだけを収集するように最適化してください。
        max_date_row = df_agg.select(F.max("trade_date")).collect()
        if not max_date_row or max_date_row[0][0] is None:
            return None
            
        max_date = max_date_row[0][0]
        cutoff_date = max_date - datetime.timedelta(days=days)
        
        # フィルタリングしてPandasに変換
        pdf = df_agg.filter(F.col("trade_date") >= cutoff_date).orderBy("trade_date").toPandas()
        
        # lightweight-charts用にリネーム
        rename_dict = {'trade_date': 'date'}
        for ma in [5, 25, 75]:
            rename_dict[f'ma_{ma}'] = f'MA{ma}'
        
        pdf = pdf.rename(columns=rename_dict)
        
        # JSONシリアル化のために日付が文字列形式(YYYY-MM-DD)であることを確認します
        if 'date' in pdf.columns:
            pdf['date'] = pdf['date'].dt.strftime('%Y-%m-%d')
            
        # 問題を引き起こす可能性のあるカラム（グループ化タイムスタンプなど）を削除し、チャートに必要なものだけを保持します
        # date, open, high, low, close, volume, およびMAカラムが必要です。
        keep_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        expected_mas = [f'MA{ma}' for ma in [5, 25, 75]]
        for ma_col in expected_mas:
            if ma_col in pdf.columns:
                keep_cols.append(ma_col)
                
        # エラーを避けるために利用可能なカラムと交差させます
        final_cols = [c for c in keep_cols if c in pdf.columns]
        pdf = pdf[final_cols]
            
        return pdf
    except Exception as e:
        st.error(f"Error processing data: {e}")
        return None

with st.spinner('Loading data...'):
    interval_configs = [("MONTHLY", 3000), ("WEEKLY", 600), ("DAILY", 120)]
    
    # 3つのカラムを作成
    cols = st.columns(3)
    
    for i, (interval, days) in enumerate(interval_configs):
        with cols[i]:
            data = load_and_process_data(selected_code, interval, days)

            if data is not None and not data.empty:
                # Lightweight Charts 実装
                # カラムに収まるように幅を調整（ワイドレイアウト3カラムの場合は約500px）
                chart = StreamlitChart(width=500, height=600, toolbox=True)
                chart.legend(True)
                
                # 日付フォーマットの設定 (yyyy-MM-dd)
                chart.run_script(f"""
                    {chart.id}.chart.applyOptions({{
                        localization: {{
                            dateFormat: 'yyyy-MM-dd'
                        }}
                    }});
                """)

                chart.topbar.textbox('title', f'{selected_code} {interval}')
                
                # データ設定
                chart.set(data)
                
                # 移動平均の追加
                ma_colors = {5: 'orange', 25: '#A020F0', 75: '#008000'}
                for ma in [5, 25, 75]:
                    ma_name = f'MA{ma}'
                    if ma_name in data.columns:
                        line = chart.create_line(name=ma_name, color=ma_colors[ma], width=1)
                        line.set(data)
                        
                chart.load()
            else:
                st.info(f"No data ({interval})")
