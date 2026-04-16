---
name: scan-new-high-breakout
description: "新高値ブレイク銘柄をスキャンする。Use when: 52週新高値を更新した銘柄を探したい、新高値ブレイク投資法、出来高急増を伴う上値ブレイク銘柄のリストアップ、順張りスクリーニング、CAN SLIM スタイルのスキャン。Queries main.default.stock_prices and main.default.stock_list on Databricks."
argument-hint: "スキャン対象日や絞り込みたい条件（セクター、市場など）を指定（例: '直近の取引日', 'プライム市場のみ', 'セクター=電気機器'）"
---

# 新高値ブレイク銘柄スキャン

## 投資法の概要
**新高値ブレイク（New High Breakout）** とは、過去一定期間の最高値（上値抵抗線）を更新した銘柄を買う順張り手法。新高値圏では「やれやれ売り」の含み損ホルダーが存在しないため上値が軽く、トレンド継続の可能性が高い。

---

## スクリーニング条件（デフォルト）

| 条件 | デフォルト値 | 変数名 |
|------|-------------|--------|
| 新高値の基準期間 | 過去 250 営業日（約 52 週） | `LOOKBACK_DAYS` |
| 出来高急増の基準 | 当日出来高 ≥ 過去 50 日平均の **1.5 倍** | `VOLUME_RATIO` |
| 価格フィルタ | なし（デフォルト） | — |
| 市場・セクターフィルタ | なし（デフォルト。引数で指定可） | — |

---

## 手順

### Step 1. 最新取引日を確認する
```sql
SELECT MAX(dateString) AS latest_date
FROM main.default.stock_prices;
```

### Step 2. 新高値ブレイク銘柄を抽出する（SQL）

```sql
WITH base AS (
  SELECT
    code,
    dateString,
    open,
    high,
    low,
    close,
    volume,
    -- 当日を除く過去250営業日の最高値（上値抵抗線）
    MAX(high) OVER (
      PARTITION BY code
      ORDER BY dateString
      ROWS BETWEEN 250 PRECEDING AND 1 PRECEDING
    ) AS high_250d,
    -- 過去50日平均出来高
    AVG(volume) OVER (
      PARTITION BY code
      ORDER BY dateString
      ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING
    ) AS avg_volume_50d
  FROM main.default.stock_prices
  WHERE TO_DATE(dateString) >= DATE_SUB(CURRENT_DATE(), 300)  -- 計算に必要な期間を確保
),
screened AS (
  SELECT *
  FROM base
  WHERE
    dateString = (SELECT MAX(dateString) FROM main.default.stock_prices)  -- 最新取引日のみ
    AND close > high_250d                     -- ① 終値が252日高値を上回る
    AND volume >= avg_volume_50d * 1.5        -- ② 出来高が50日平均の1.5倍以上
    AND high_250d IS NOT NULL                 -- データが揃っている銘柄のみ
)
SELECT
  s.code,
  l.name,
  l.market,
  l.sector33,
  s.dateString       AS breakout_date,
  s.close            AS breakout_close,
  s.high_250d        AS resistance_high,
  ROUND(s.close / s.high_250d - 1, 4) AS breakout_pct,  -- 突破率
  s.volume           AS breakout_volume,
  ROUND(s.volume / s.avg_volume_50d, 2) AS volume_ratio  -- 出来高倍率
FROM screened s
JOIN main.default.stock_list l ON l.code = s.code
ORDER BY volume_ratio DESC;
```

> **市場・セクターで絞り込む場合**: `WHERE` 句に `AND l.market = 'プライム'` や `AND l.sector33 = '電気機器'` を追加する。

---

### Step 3. Python で実行する

#### Local SQL Mode（ローカル開発 / `just run`）

```python
import os
import pandas as pd
from databricks import sql

# --- パラメータ ---
LOOKBACK_DAYS   = 300   # クエリ範囲（計算バッファ込み）
RESISTANCE_DAYS = 250   # 上値抵抗線の基準期間
VOLUME_LOOKBACK = 50    # 平均出来高の基準期間
VOLUME_RATIO    = 1.5   # 出来高急増のしきい値

conn_params = dict(
    server_hostname=os.getenv("DATABRICKS_HOST"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
)

query = f"""
WITH base AS (
  SELECT
    code, dateString, close, high, volume,
    MAX(high) OVER (
      PARTITION BY code ORDER BY dateString
      ROWS BETWEEN {RESISTANCE_DAYS} PRECEDING AND 1 PRECEDING
    ) AS high_{RESISTANCE_DAYS}d,
    AVG(volume) OVER (
      PARTITION BY code ORDER BY dateString
      ROWS BETWEEN {VOLUME_LOOKBACK} PRECEDING AND 1 PRECEDING
    ) AS avg_volume_{VOLUME_LOOKBACK}d
  FROM main.default.stock_prices
  WHERE TO_DATE(dateString) >= DATE_SUB(CURRENT_DATE(), {LOOKBACK_DAYS})
),
screened AS (
  SELECT *
  FROM base
  WHERE
    dateString = (SELECT MAX(dateString) FROM main.default.stock_prices)
    AND close > high_{RESISTANCE_DAYS}d
    AND volume >= avg_volume_{VOLUME_LOOKBACK}d * {VOLUME_RATIO}
    AND high_{RESISTANCE_DAYS}d IS NOT NULL
)
SELECT
  s.code, l.name, l.market, l.sector33,
  s.dateString AS breakout_date,
  s.close      AS breakout_close,
  s.high_{RESISTANCE_DAYS}d AS resistance_high,
  ROUND(s.close / s.high_{RESISTANCE_DAYS}d - 1, 4) AS breakout_pct,
  s.volume     AS breakout_volume,
  ROUND(s.volume / s.avg_volume_{VOLUME_LOOKBACK}d, 2) AS volume_ratio
FROM screened s
JOIN main.default.stock_list l ON l.code = s.code
ORDER BY volume_ratio DESC
"""

with sql.connect(**conn_params) as conn:
    with conn.cursor() as cur:
        cur.execute(query)
        df = cur.fetchall_arrow().to_pandas()

print(f"新高値ブレイク銘柄数: {len(df)}")
print(df.to_string(index=False))
```

#### Databricks Apps Mode（本番 / Spark）

```python
from utils import get_spark
from pyspark.sql import functions as F, Window

spark = get_spark()

# パラメータ
RESISTANCE_DAYS = 250
VOLUME_LOOKBACK = 50
VOLUME_RATIO    = 1.5

df_raw = (
    spark.table("main.default.stock_prices")
    .withColumn("trade_date", F.to_date(F.col("dateString")))
    .filter(F.col("trade_date") >= F.date_sub(F.current_date(), 300))
)

w_resistance = (
    Window.partitionBy("code")
    .orderBy("dateString")
    .rowsBetween(-RESISTANCE_DAYS, -1)
)
w_volume = (
    Window.partitionBy("code")
    .orderBy("dateString")
    .rowsBetween(-VOLUME_LOOKBACK, -1)
)

df_with_stats = (
    df_raw
    .withColumn("high_250d", F.max("high").over(w_resistance))
    .withColumn("avg_volume_50d", F.avg("volume").over(w_volume))
)

latest_date = df_raw.agg(F.max("dateString")).collect()[0][0]

df_breakout = (
    df_with_stats
    .filter(F.col("dateString") == latest_date)
    .filter(F.col("close") > F.col("high_250d"))
    .filter(F.col("volume") >= F.col("avg_volume_50d") * VOLUME_RATIO)
    .filter(F.col("high_250d").isNotNull())
)

stock_list = spark.table("main.default.stock_list")

result = (
    df_breakout
    .join(stock_list, on="code", how="left")
    .select(
        "code", "name", "market", "sector33",
        F.col("dateString").alias("breakout_date"),
        F.col("close").alias("breakout_close"),
        "high_250d",
        F.round(F.col("close") / F.col("high_250d") - 1, 4).alias("breakout_pct"),
        F.col("volume").alias("breakout_volume"),
        F.round(F.col("volume") / F.col("avg_volume_50d"), 2).alias("volume_ratio"),
    )
    .orderBy(F.col("volume_ratio").desc())
    .toPandas()
)

print(f"新高値ブレイク銘柄数: {len(result)}")
print(result.to_string(index=False))
```

---

## 出力の読み方

| カラム | 説明 |
|--------|------|
| `code` / `name` | 銘柄コード・銘柄名 |
| `market` | 上場市場（プライム / スタンダード / グロース など） |
| `sector33` | 東証33業種分類 |
| `breakout_close` | ブレイクアウト当日の終値 |
| `resistance_high` | 過去250日の最高値（突破された抵抗線） |
| `breakout_pct` | 抵抗線を何%上抜けしたか（0.01 = 1%） |
| `volume_ratio` | 当日出来高 ÷ 50日平均出来高（高いほど強い）|

---

## カスタマイズ例

```sql
-- プライム市場のみ
AND l.market = 'プライム'

-- 特定セクターのみ
AND l.sector33 = '電気機器'

-- 上場来高値ブレイク（全期間）に変更
-- ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING

-- 週足で確認（Gold テーブル使用）
FROM main.default.stock_prices_weekly
-- ※ dateString の代わりに week_start カラムを使用
```

---

## よくある問題

| 症状 | 原因 | 対処 |
|------|------|------|
| 結果が0件 | 最新日のデータが未反映 | `REFRESH MATERIALIZED VIEW main.default.stock_prices` を実行 |
| 計算期間不足でNULLが多い | 上場間もない銘柄 | `high_250d IS NOT NULL` フィルタが自動で除外 |
| クエリが遅い | 全銘柄×全期間の Scan | `WHERE TO_DATE(dateString) >= DATE_SUB(CURRENT_DATE(), 300)` で期間を絞る |
| `dateString` カラムが見つからない | スキーマの違い | `data_provider._detect_stock_prices_schema()` で自動検出 |
