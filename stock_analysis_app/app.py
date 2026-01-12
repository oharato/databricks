import streamlit as st
import os
import pandas as pd
import datetime
from lightweight_charts.widgets import StreamlitChart
from dotenv import load_dotenv

# ローカル環境変数を読み込む
load_dotenv()

# 接続モードの判定
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
IS_SQL_MODE = HTTP_PATH is not None

# ライブラリのインポート分岐
if IS_SQL_MODE:
    from databricks import sql
else:
    from databricks.connect import DatabricksSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

# ページ設定
st.set_page_config(layout="wide")
title = "Stock Price Analysis App " + ("(Local SQL Mode)" if IS_SQL_MODE else "(Databricks Apps)")
st.title(title)

# Sparkの初期化 (Databricks Apps モードのみ)
def get_spark():
    if IS_SQL_MODE:
        return None
        
    session = DatabricksSession.builder.serverless().getOrCreate()
    try:
        session.sql("SELECT 1").collect()
    except Exception as e:
        if "INACTIVITY_TIMEOUT" in str(e) or "session_id is no longer usable" in str(e):
            session.stop()
            session = DatabricksSession.builder.serverless().getOrCreate()
        else:
             st.error(f"Spark接続チェック失敗: {e}")
             raise e
    return session

try:
    spark = get_spark()
except Exception as e:
    st.error(f"Failed to connect to Databricks: {e}")
    st.stop()

# --- 設定 ---
TARGET_CODES = ['1301', '3031'] 

# サイドバーコントロール
st.sidebar.header("Configuration")
selected_code = st.sidebar.selectbox("Select Stock Code", TARGET_CODES)

# 1. データの読み込みと処理
@st.cache_data(ttl=3600)
def load_and_process_data(code, interval, days):
    try:
        if IS_SQL_MODE:
            # --- SQL Connector (Local) Mode ---
            with sql.connect(
                server_hostname=os.getenv("DATABRICKS_HOST"),
                http_path=HTTP_PATH,
                access_token=os.getenv("DATABRICKS_TOKEN")
            ) as connection:
                # データを取得
                query = f"SELECT * FROM main.default.stock_prices WHERE code = '{code}'"
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    # Pandas DataFrame として取得
                    df = cursor.fetchall_arrow().to_pandas()
            
            # 日付変換ロジック (Pandas)
            if "dateString" in df.columns:
                df["trade_date"] = pd.to_datetime(df["dateString"])
            elif "date" in df.columns:
                df["trade_date"] = pd.to_datetime(df["date"], unit='ms')
            
            # 集計ロジック (Pandas)
            if interval == 'DAILY':
                df_agg = df[["code", "trade_date", "open", "high", "low", "close", "volume"]].copy()
            elif interval == 'WEEKLY':
                # 'W-MON' は月曜始まり
                df_agg = df.resample('W-MON', on='trade_date').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum',
                    'code': 'first'
                }).reset_index()
                # データがない週を除外
                df_agg = df_agg.dropna(subset=['open'])
            elif interval == 'MONTHLY':
                df_agg = df.resample('ME', on='trade_date').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum',
                    'code': 'first'
                }).reset_index()
                df_agg = df_agg.dropna(subset=['open'])
            
            # 移動平均 (Pandas)
            df_agg = df_agg.sort_values('trade_date')
            for window_size in [5, 25, 75]:
                # Spark の Window.rowsBetween(-window_size + 1, 0) と同じ挙動にする
                df_agg[f"ma_{window_size}"] = df_agg['close'].rolling(window=window_size, min_periods=1).mean()
            
            # 日付によるフィルタリング
            max_date = df_agg['trade_date'].max()
            if pd.isna(max_date):
                return None
            
            cutoff_date = max_date - datetime.timedelta(days=days)
            pdf = df_agg[df_agg['trade_date'] >= cutoff_date].copy()
            
        else:
            # --- Databricks Connect (Spark) Mode ---
            table_name = "main.default.stock_prices"
            df = spark.table(table_name).filter(F.col("code") == code)
            
            # sample.mdからの日付変換ロジック
            if "dateString" in df.columns:
                df = df.withColumn("trade_date", F.to_timestamp(F.col("dateString"), "yyyy-MM-dd"))
            elif "date" in df.columns:
                df = df.withColumn("trade_date", (F.col("date").cast("double") / 1000).cast("timestamp"))
            else:
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
            max_date_row = df_agg.select(F.max("trade_date")).collect()
            if not max_date_row or max_date_row[0][0] is None:
                return None
                
            max_date = max_date_row[0][0]
            cutoff_date = max_date - datetime.timedelta(days=days)
            
            pdf = df_agg.filter(F.col("trade_date") >= cutoff_date).orderBy("trade_date").toPandas()
        
        # --- 共通の後処理 ---
        
        # lightweight-charts用にリネーム
        rename_dict = {'trade_date': 'date'}
        for ma in [5, 25, 75]:
            rename_dict[f'ma_{ma}'] = f'MA{ma}'
        
        pdf = pdf.rename(columns=rename_dict)
        
        if 'date' in pdf.columns:
            pdf['date'] = pdf['date'].dt.strftime('%Y-%m-%d')
            
        keep_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        expected_mas = [f'MA{ma}' for ma in [5, 25, 75]]
        for ma_col in expected_mas:
            if ma_col in pdf.columns:
                keep_cols.append(ma_col)
                
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
