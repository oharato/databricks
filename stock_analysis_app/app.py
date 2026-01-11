import streamlit as st
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import datetime
from lightweight_charts.widgets import StreamlitChart

# Page config
st.set_page_config(layout="wide")
st.title("Stock Price Analysis App (Databricks)")

# Initialize Spark
# Databricks Apps environment automatically handles authentication when using databricks-connect
def get_spark():
    session = DatabricksSession.builder.serverless().getOrCreate()
    try:
        # Validate session to handle INACTIVITY_TIMEOUT
        session.sql("SELECT 1").collect()
    except Exception as e:
        if "INACTIVITY_TIMEOUT" in str(e) or "session_id is no longer usable" in str(e):
            # print("Session timed out. Reconnecting...")
            session.stop()
            session = DatabricksSession.builder.serverless().getOrCreate()
        else:
             # If it's another error, we might want to let it bubble up or handle it
             # For now, let's just log and try to return the session or raise
             st.error(f"Spark connection check failed: {e}")
             raise e
    return session

try:
    spark = get_spark()


except Exception as e:
    st.error(f"Failed to connect to Databricks: {e}")
    st.stop()

# --- Config ---
# In a real scenario, you might query distinct codes from the table
TARGET_CODES = ['1301', '3031'] 

# Sidebar controls
st.sidebar.header("Configuration")
selected_code = st.sidebar.selectbox("Select Stock Code", TARGET_CODES)
interval = st.sidebar.selectbox("Interval", ["DAILY", "WEEKLY", "MONTHLY"])
days_to_show = st.sidebar.slider("Days to Show", 30, 365, 100)

# 1. Loading and Processing Data
@st.cache_data(ttl=3600)
def load_and_process_data(code, interval, days):
    try:
        # Load table
        table_name = "main.default.stock_prices"
        df = spark.table(table_name).filter(F.col("code") == code)
        
        # Date conversion logic from sample.md
        if "dateString" in df.columns:
            df = df.withColumn("trade_date", F.to_timestamp(F.col("dateString"), "yyyy-MM-dd"))
        elif "date" in df.columns:
            df = df.withColumn("trade_date", (F.col("date").cast("double") / 1000).cast("timestamp"))
        else:
            # Fallback or error if columns differ
            pass

        # Aggregation Logic
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
        
        # Moving Averages
        w = Window.partitionBy("code").orderBy("trade_date")
        for window_size in [5, 25, 75]:
            df_agg = df_agg.withColumn(f"ma_{window_size}", F.avg("close").over(w.rowsBetween(-window_size + 1, 0)))

        # Filtering by date
        # We need to compute max date first. 
        # Note: In Streamlit/Spark apps, optimize collecting only what's needed.
        max_date_row = df_agg.select(F.max("trade_date")).collect()
        if not max_date_row or max_date_row[0][0] is None:
            return None
            
        max_date = max_date_row[0][0]
        cutoff_date = max_date - datetime.timedelta(days=days)
        
        # Filter and Convert to Pandas
        pdf = df_agg.filter(F.col("trade_date") >= cutoff_date).orderBy("trade_date").toPandas()
        
        # Rename for lightweight-charts
        rename_dict = {'trade_date': 'date'}
        for ma in [5, 25, 75]:
            rename_dict[f'ma_{ma}'] = f'MA{ma}'
        
        return pdf.rename(columns=rename_dict)
    except Exception as e:
        st.error(f"Error processing data: {e}")
        return None

with st.spinner('Loading data...'):
    data = load_and_process_data(selected_code, interval, days_to_show)

if data is not None and not data.empty:
    st.subheader(f"{selected_code} - {interval}")
    
    # Lightweight Charts Implementation
    chart = StreamlitChart(width=900, height=600, toolbox=True)
    chart.legend(True)
    chart.topbar.textbox('title', f'{selected_code} {interval}')
    
    # Set Data
    chart.set(data)
    
    # Add Moving Averages
    ma_colors = {5: 'orange', 25: '#A020F0', 75: '#008000'}
    for ma in [5, 25, 75]:
        ma_name = f'MA{ma}'
        if ma_name in data.columns:
            line = chart.create_line(name=ma_name, color=ma_colors[ma], width=1)
            line.set(data)
            
    chart.load()

else:
    st.info("No data found for the selected criteria.")
