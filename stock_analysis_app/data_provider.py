import os
import datetime
from functools import lru_cache
import pandas as pd
import streamlit as st
from utils import IS_SQL_MODE, HTTP_PATH, get_spark

# ライブラリのインポート分岐
if IS_SQL_MODE:
    from databricks import sql
else:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

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
            try:
                spark = get_spark()
            except Exception as e:
                st.error(f"Failed to connect to Databricks: {e}")
                return pd.DataFrame()
            if spark is None: return pd.DataFrame()
            df = spark.table("main.default.stock_list").toPandas()
        
        df['code'] = df['code'].astype(str)
        df['label'] = df['code'] + ": " + df['name'] + " (" + df['market'].fillna('-') + ")"
        return df
    except Exception as e:
        st.error(f"Error loading stock list: {e}")
        return pd.DataFrame()

def _load_stock_raw_data_impl(code, report_errors):
    try:
        if IS_SQL_MODE:
            with sql.connect(
                server_hostname=os.getenv("DATABRICKS_HOST"),
                http_path=HTTP_PATH,
                access_token=os.getenv("DATABRICKS_TOKEN")
            ) as connection:
                query = f"SELECT * FROM main.default.stock_prices WHERE code = '{code}'"
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    df = cursor.fetchall_arrow().to_pandas()
        else:
            try:
                spark = get_spark()
            except Exception as e:
                if report_errors:
                    st.error(f"Failed to connect to Databricks: {e}")
                return pd.DataFrame()
            if spark is None:
                return pd.DataFrame()
            table_name = "main.default.stock_prices"
            df = spark.table(table_name).filter(F.col("code") == code)
            if "dateString" in df.columns:
                df = df.withColumn("trade_date", F.to_timestamp(F.col("dateString"), "yyyy-MM-dd"))
            elif "date" in df.columns:
                df = df.withColumn("trade_date", (F.col("date").cast("double") / 1000).cast("timestamp"))
            df = df.select("code", "trade_date", "open", "high", "low", "close", "volume")
            df = df.toPandas()

        if df.empty:
            return pd.DataFrame()

        if "trade_date" not in df.columns:
            if "dateString" in df.columns:
                df["trade_date"] = pd.to_datetime(df["dateString"])
            elif "date" in df.columns:
                df["trade_date"] = pd.to_datetime(df["date"], unit="ms")

        keep_cols = ["code", "trade_date", "open", "high", "low", "close", "volume"]
        existing_cols = [c for c in keep_cols if c in df.columns]
        df = df[existing_cols].copy()
        df = df.sort_values("trade_date")
        return df
    except Exception as e:
        print(f"Error in load_stock_raw_data: {e}")
        if report_errors:
            st.error(f"Error loading stock data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_stock_raw_data(code, report_errors=True):
    return _load_stock_raw_data_impl(code, report_errors)


@lru_cache(maxsize=128)
def load_stock_raw_data_threadsafe(code):
    return _load_stock_raw_data_impl(code, report_errors=False)


def process_interval_data(df, interval, days, report_errors=True):
    try:
        if df is None or df.empty:
            return None

        pdf = df.copy()
        if "trade_date" not in pdf.columns:
            return None

        if interval == "DAILY":
            df_agg = pdf[["code", "trade_date", "open", "high", "low", "close", "volume"]].copy()
        elif interval == "WEEKLY":
            df_agg = pdf.resample("W-MON", on="trade_date").agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "code": "first"
            }).reset_index()
            df_agg = df_agg.dropna(subset=["open"])
        elif interval == "MONTHLY":
            df_agg = pdf.resample("ME", on="trade_date").agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "code": "first"
            }).reset_index()
            df_agg = df_agg.dropna(subset=["open"])
        else:
            return None

        df_agg = df_agg.sort_values("trade_date")
        for window_size in [5, 25, 75]:
            df_agg[f"MA{window_size}"] = df_agg["close"].rolling(window=window_size, min_periods=1).mean()

        max_date = df_agg["trade_date"].max()
        if pd.isna(max_date):
            return None

        cutoff_date = max_date - datetime.timedelta(days=days)
        pdf = df_agg[df_agg["trade_date"] >= cutoff_date].copy()

        pdf = pdf.rename(columns={"trade_date": "date"})
        if "date" in pdf.columns:
            pdf["date"] = pdf["date"].dt.strftime("%Y-%m-%d")

        keep_cols = ["date", "open", "high", "low", "close", "volume"]
        expected_mas = [f"MA{ma}" for ma in [5, 25, 75]]
        for ma_col in expected_mas:
            if ma_col in pdf.columns:
                keep_cols.append(ma_col)
        final_cols = [c for c in keep_cols if c in pdf.columns]
        return pdf[final_cols]
    except Exception as e:
        print(f"Error in process_interval_data: {e}")
        if report_errors:
            st.error(f"Error processing data: {e}")
        return None


def load_multi_interval_data(code, interval_configs, report_errors=True):
    raw_df = load_stock_raw_data(code, report_errors=report_errors)
    if raw_df is None or raw_df.empty:
        return {interval: None for interval, _ in interval_configs}

    data_map = {}
    for interval, days in interval_configs:
        data_map[interval] = process_interval_data(raw_df, interval, days, report_errors=report_errors)
    return data_map


def load_multi_interval_data_threadsafe(code, interval_configs):
    raw_df = load_stock_raw_data_threadsafe(code)
    if raw_df is None or raw_df.empty:
        return {interval: None for interval, _ in interval_configs}

    data_map = {}
    for interval, days in interval_configs:
        data_map[interval] = process_interval_data(raw_df, interval, days, report_errors=False)
    return data_map


@st.cache_data(ttl=3600)
def load_and_process_data(code, interval, days, report_errors=True):
    raw_df = load_stock_raw_data(code, report_errors=report_errors)
    return process_interval_data(raw_df, interval, days, report_errors=report_errors)
