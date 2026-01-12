import os
import datetime
import pandas as pd
import streamlit as st
from utils import IS_SQL_MODE, HTTP_PATH, get_spark

# ライブラリのインポート分岐
if IS_SQL_MODE:
    from databricks import sql
else:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

try:
    spark = get_spark()
except Exception as e:
    st.error(f"Failed to connect to Databricks: {e}")
    # 接続失敗時は続行不可だが、importエラーを避けるため後続でガードする
    spark = None

@st.cache_data(ttl=3600)
def load_stock_list():
    try:
        if IS_SQL_MODE:
            with sql.connect(
                server_hostname=os.getenv("DATABRICKS_HOST"),
                http_path=HTTP_PATH,
                access_token=os.getenv("DATABRICKS_TOKEN")
            ) as connection:
                query = "SELECT * FROM main.default.stock_list"
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    df = cursor.fetchall_arrow().to_pandas()
        else:
            if spark is None: return pd.DataFrame()
            df = spark.table("main.default.stock_list").toPandas()
        
        df['code'] = df['code'].astype(str)
        df['label'] = df['code'] + ": " + df['name'] + " (" + df['market'].fillna('-') + ")"
        return df
    except Exception as e:
        st.error(f"Error loading stock list: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_and_process_data(code, interval, days):
    try:
        if IS_SQL_MODE:
            # --- SQLコネクタ (ローカル) モード ---
            with sql.connect(
                server_hostname=os.getenv("DATABRICKS_HOST"),
                http_path=HTTP_PATH,
                access_token=os.getenv("DATABRICKS_TOKEN")
            ) as connection:
                query = f"SELECT * FROM main.default.stock_prices WHERE code = '{code}'"
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    df = cursor.fetchall_arrow().to_pandas()
            
            if "dateString" in df.columns:
                df["trade_date"] = pd.to_datetime(df["dateString"])
            elif "date" in df.columns:
                df["trade_date"] = pd.to_datetime(df["date"], unit='ms')
            
            if interval == 'DAILY':
                df_agg = df[["code", "trade_date", "open", "high", "low", "close", "volume"]].copy()
            elif interval == 'WEEKLY':
                df_agg = df.resample('W-MON', on='trade_date').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum',
                    'code': 'first'
                }).reset_index()
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
            
            df_agg = df_agg.sort_values('trade_date')
            for window_size in [5, 25, 75]:
                df_agg[f"ma_{window_size}"] = df_agg['close'].rolling(window=window_size, min_periods=1).mean()
            
            max_date = df_agg['trade_date'].max()
            if pd.isna(max_date):
                return None
            
            cutoff_date = max_date - datetime.timedelta(days=days)
            pdf = df_agg[df_agg['trade_date'] >= cutoff_date].copy()
            
        else:
            # --- Databricks Connect (Spark) モード ---
            if spark is None: return None
            table_name = "main.default.stock_prices"
            df = spark.table(table_name).filter(F.col("code") == code)
            
            if "dateString" in df.columns:
                df = df.withColumn("trade_date", F.to_timestamp(F.col("dateString"), "yyyy-MM-dd"))
            elif "date" in df.columns:
                df = df.withColumn("trade_date", (F.col("date").cast("double") / 1000).cast("timestamp"))
            
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
            
            w = Window.partitionBy("code").orderBy("trade_date")
            for window_size in [5, 25, 75]:
                df_agg = df_agg.withColumn(f"ma_{window_size}", F.avg("close").over(w.rowsBetween(-window_size + 1, 0)))

            max_date_row = df_agg.select(F.max("trade_date")).collect()
            if not max_date_row or max_date_row[0][0] is None:
                return None
                
            max_date = max_date_row[0][0]
            cutoff_date = max_date - datetime.timedelta(days=days)
            
            pdf = df_agg.filter(F.col("trade_date") >= cutoff_date).orderBy("trade_date").toPandas()
        
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
        print(f"Error in load_and_process_data: {e}") # デバッグ用
        st.error(f"Error processing data: {e}")
        return None
