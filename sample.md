### (推奨) `lightweight-charts-python` を使用した完全版スクリプト

TradingView の Lightweight Charts を使用して、より高機能なチャートを表示します。
日足・週足・月足の切り替えではなく、全てのチャートを順に表示する構成です。

1. **ライブラリのインストール**:
   ```bash
   %pip install lightweight-charts
   ```

2. **実行コード**:

```python
from lightweight_charts import Chart
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime

# --- 設定 ---
TARGET_CODES = ['1301', '3031'] # 表示したい銘柄コード
# ------------

# 1. データ読み込み & 日付変換
df = spark.table("main.default.stock_prices").filter(F.col("code").isin(TARGET_CODES))

if "dateString" in df.columns:
    df = df.withColumn("trade_date", F.to_timestamp(F.col("dateString"), "yyyy-MM-dd"))
elif "date" in df.columns:
    df = df.withColumn("trade_date", (F.col("date").cast("double") / 1000).cast("timestamp"))
else:
    raise ValueError("dateString or date column not found")

# 2. 移動平均計算関数
def add_moving_averages(df_in, windows=[5, 25, 75]):
    w = Window.partitionBy("code").orderBy("trade_date")
    for window_size in windows:
        df_in = df_in.withColumn(f"ma_{window_size}", F.avg("close").over(w.rowsBetween(-window_size + 1, 0)))
    return df_in

# 3. 集計・整形関数
def process_data(df_base, interval, days_to_show):
    # Aggregation
    if interval == 'DAILY':
        df_agg = df_base.select("code", "trade_date", "open", "high", "low", "close", "volume")
    elif interval == 'WEEKLY':
        df_agg = df_base.withColumn("year_week", F.date_trunc("week", "trade_date")) \
            .groupBy("code", "year_week").agg(
                F.first("open").alias("open"), F.max("high").alias("high"), F.min("low").alias("low"), F.last("close").alias("close"),
                F.sum("volume").alias("volume"), F.min("trade_date").alias("trade_date")
            )
    elif interval == 'MONTHLY':
        df_agg = df_base.withColumn("year_month", F.date_trunc("month", "trade_date")) \
            .groupBy("code", "year_month").agg(
                F.first("open").alias("open"), F.max("high").alias("high"), F.min("low").alias("low"), F.last("close").alias("close"),
                F.sum("volume").alias("volume"), F.min("trade_date").alias("trade_date")
            )

    # MA Calculation
    df_ma = add_moving_averages(df_agg, [5, 25, 75])

    # Filtering
    max_date_row = df_ma.select(F.max("trade_date")).collect()[0][0]
    if not max_date_row: return None
    cutoff_date = max_date_row - datetime.timedelta(days=days_to_show)
    
    # Convert to Pandas & Rename for lightweight-charts
    pdf = df_ma.filter(F.col("trade_date") >= cutoff_date).orderBy("trade_date").toPandas()
    
    # Rename MA columns to match line names (MA5, MA25...) to avoid NameError
    rename_dict = {'trade_date': 'date'}
    for ma in [5, 25, 75]:
        rename_dict[f'ma_{ma}'] = f'MA{ma}'
        
    return pdf.rename(columns=rename_dict)

# 4. メイン描画処理 (安定動作のため1つのチャートのみ表示)
# 注意: Databricks Notebook環境では複数のChartインスタンスをループで作成すると
# "AssertionError: cannot start a process twice" エラーになる場合があります。
# そのため、ここでは変数を指定して1つのチャートを描画する形式にします。

target_code = '1301'
target_label = 'Daily' # Daily, Weekly, Monthly
days = 100 # 期間

# データ取得
data = process_data(df.filter(F.col("code") == target_code), target_label.upper(), days)

if data is not None and not data.empty:
    print(f"--- {target_code} {target_label} Chart ({len(data)} rows) ---")

    # Chart 初期化
    # toolbox=False で軽量化
    chart = Chart(width=800, height=500, toolbox=False)
    chart.legend(True)
    chart.topbar.textbox('title', f'{target_code} {target_label}')

    # データセット
    chart.set(data)

    # 移動平均線の追加
    ma_colors = {5: 'orange', 25: '#A020F0', 75: '#008000'}
    for ma in [5, 25, 75]:
        ma_name = f'MA{ma}'
        if ma_name in data.columns:
            line = chart.create_line(name=ma_name, color=ma_colors[ma], width=1)
            line.set(data)

    # 表示 (同期的に表示)
    chart.show()
else:
    print("No data found.")

```

