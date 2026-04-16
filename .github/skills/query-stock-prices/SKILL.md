---
name: query-stock-prices
description: "Query the stock_prices table on Databricks. Use when writing SQL or Python queries against main.default.stock_prices, analyzing candlestick data (OHLCV), filtering by stock code or date range, aggregating weekly/monthly prices, or debugging data access issues. Covers both Local SQL Mode (databricks-sql-connector) and Databricks Apps Mode (Spark)."
argument-hint: "Describe what you want to query (e.g. 'daily OHLCV for code 1301', 'all sectors last 90 days')"
---

# Query stock_prices on Databricks

## テーブル仕様

### `main.default.stock_prices` (Materialized View)
日本株の日足 OHLCV データ。

| カラム | 型 | 説明 |
|---|---|---|
| `code` | STRING | 銘柄コード (例: `'1301'`) |
| `dateString` | STRING | 日付文字列 (例: `'2024-01-15'`) |
| `open` | DOUBLE | 始値 |
| `high` | DOUBLE | 高値 |
| `low` | DOUBLE | 安値 |
| `close` | DOUBLE | 終値 |
| `volume` | LONG | 出来高 |

> **注意**: `stock_prices` は Materialized View のため `OPTIMIZE` は実行不可。  
> スキーマは環境によって `dateString` / `date` / `trade_date` が異なる場合がある。`_detect_stock_prices_schema()` で自動検出している。

### 関連テーブル
| テーブル | 主なカラム | 用途 |
|---|---|---|
| `main.default.stock_list` | `code`, `name`, `market`, `sector33`, `sector17`, `scale` | 銘柄マスタ |
| `main.default.stock_prices_weekly` | `code`, `week_start`, `open`, `high`, `low`, `close`, `volume` | 週足 Gold テーブル |
| `main.default.stock_prices_monthly` | `code`, `month_start`, `open`, `high`, `low`, `close`, `volume` | 月足 Gold テーブル |

---

## 接続モード

実行環境によって使い分ける。`utils.IS_SQL_MODE` で判定済み。

| 環境 | モード | ライブラリ |
|---|---|---|
| ローカル (`just run`) | Local SQL Mode | `databricks-sql-connector` |
| Databricks Apps (本番) | Spark Mode | `databricks-connect` |

---

## SQL クエリパターン

### 1. 単一銘柄・直近 N 日間
```sql
SELECT code, dateString, open, high, low, close, volume
FROM main.default.stock_prices
WHERE code = '1301'
  AND TO_DATE(dateString) >= DATE_SUB(CURRENT_DATE(), 365)
ORDER BY dateString
```

### 2. 複数銘柄を一括取得 (IN 句 — 推奨)
```sql
SELECT code, dateString, open, high, low, close, volume
FROM main.default.stock_prices
WHERE code IN ('1301', '1305', '1321')
  AND TO_DATE(dateString) >= DATE_SUB(CURRENT_DATE(), 90)
ORDER BY code, dateString
```

### 3. 月足集計 (リサンプル)
```sql
SELECT
  code,
  DATE_TRUNC('month', TO_DATE(dateString)) AS month_start,
  FIRST_VALUE(open)  OVER w AS open,
  MAX(high)          OVER w AS high,
  MIN(low)           OVER w AS low,
  LAST_VALUE(close)  OVER w AS close,
  SUM(volume)        OVER w AS volume
FROM main.default.stock_prices
WHERE code = '1301'
WINDOW w AS (PARTITION BY code, DATE_TRUNC('month', TO_DATE(dateString))
             ORDER BY dateString
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

### 4. 週足集計 (月曜始まり)
```sql
SELECT
  code,
  DATE_TRUNC('week', TO_DATE(dateString)) AS week_start,
  FIRST_VALUE(open)  OVER w AS open,
  MAX(high)          OVER w AS high,
  MIN(low)           OVER w AS low,
  LAST_VALUE(close)  OVER w AS close,
  SUM(volume)        OVER w AS volume
FROM main.default.stock_prices
WHERE code = '1301'
WINDOW w AS (PARTITION BY code, DATE_TRUNC('week', TO_DATE(dateString))
             ORDER BY dateString
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```
> 週足・月足は Gold テーブル (`stock_prices_weekly`, `stock_prices_monthly`) も利用可能。

### 5. 銘柄マスタとの JOIN
```sql
SELECT p.code, l.name, l.sector33, p.dateString, p.close
FROM main.default.stock_prices p
JOIN main.default.stock_list l ON l.code = p.code
WHERE p.code IN ('1301', '7203')
  AND TO_DATE(p.dateString) >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY p.code, p.dateString
```

---

## Python コードパターン

### Local SQL Mode (databricks-sql-connector)
```python
import os
from databricks import sql

conn_params = dict(
    server_hostname=os.getenv("DATABRICKS_HOST"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
)

query = """
    SELECT code, dateString, open, high, low, close, volume
    FROM main.default.stock_prices
    WHERE code = '1301'
      AND TO_DATE(dateString) >= DATE_SUB(CURRENT_DATE(), 365)
    ORDER BY dateString
"""

with sql.connect(**conn_params) as conn:
    with conn.cursor() as cur:
        cur.execute(query)
        df = cur.fetchall_arrow().to_pandas()
```

### Databricks Apps Mode (Spark)
```python
from utils import get_spark
from pyspark.sql import functions as F

spark = get_spark()
df = (
    spark.table("main.default.stock_prices")
    .filter(F.col("code") == "1301")
    .withColumn("trade_date", F.to_date(F.col("dateString")))
    .filter(F.col("trade_date") >= F.date_sub(F.current_date(), 365))
    .select("code", "trade_date", "open", "high", "low", "close", "volume")
    .orderBy("trade_date")
    .toPandas()
)
```

### 既存のユーティリティ関数を使う (推奨)
`data_provider.py` に実装済みの関数を優先的に使用すること。

| 関数 | 説明 |
|---|---|
| `load_bulk_raw_data(codes: tuple, lookback_days=None)` | 複数銘柄をバルク取得 → `{code: DataFrame}` |
| `load_bulk_multi_interval_data(codes, interval_configs)` | 複数銘柄 × 複数インターバルを一括取得 |
| `_detect_stock_prices_schema()` | 日付カラム名を自動検出 |
| `_build_bulk_stock_query(codes, lookback_days)` | IN 句クエリ文字列を生成 |

---

## セキュリティ注意事項

- ユーザー入力の銘柄コードは必ず `_escape_sql_literal()` または パラメータバインディングを使用すること
- 接続情報 (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) は環境変数または `.env` から取得し、コードにハードコードしない
- `DATABRICKS_TOKEN` をログやエラーメッセージに含めない

---

## よくある問題

| 症状 | 原因 | 対処 |
|---|---|---|
| `dateString` カラムが見つからない | スキーマの違い | `_detect_stock_prices_schema()` を使う |
| クエリが遅い | N+1 クエリ | `IN` 句のバルク取得に変更する |
| `OPTIMIZE` が失敗する | MV は OPTIMIZE 不可 | スキップして問題なし |
| データが古い | MV のリフレッシュ漏れ | `REFRESH MATERIALIZED VIEW main.default.stock_prices` を実行 |
