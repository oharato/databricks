"""
optimize_tables.py
------------------
1. stock_prices に OPTIMIZE + ZORDER を実行してデータスキッピングを最大化する。
2. 週足・月足の Gold テーブルを事前集計して作成 / 更新する。

使い方:
    cd /workspaces/databricks
    just optimize          # OPTIMIZE + ZORDER のみ
    just build-gold-tables # Gold テーブル作成 (週足・月足)
    just build-gold-tables --all  # OPTIMIZE + Gold テーブル

引数:
    --optimize     OPTIMIZE + ZORDER を実行（デフォルト ON）
    --gold-tables  Gold テーブルを作成
    --all          両方実行
    --dry-run      実行せず SQL のみ表示
"""

import argparse
import os
import sys

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

PRICES_TABLE = "main.default.stock_prices"
WEEKLY_TABLE = "main.default.stock_prices_weekly"
MONTHLY_TABLE = "main.default.stock_prices_monthly"


def get_connection():
    try:
        from databricks import sql as dbsql
    except ImportError:
        print("[ERROR] databricks-sql-connector が未インストールです。pip install databricks-sql-connector")
        sys.exit(1)

    if not all([DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN]):
        print("[ERROR] 環境変数 DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN を設定してください。")
        sys.exit(1)

    return dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )


def detect_table_type(conn) -> str:
    """stock_prices のテーブル種別を返す (例: 'MANAGED', 'EXTERNAL', 'MATERIALIZED_VIEW', 'VIEW')。"""
    with conn.cursor() as cur:
        cur.execute(f"DESCRIBE EXTENDED {PRICES_TABLE}")
        for row in cur.fetchall():
            if row[0] == "Type":
                return row[1].upper()
    return "UNKNOWN"


def detect_date_col(conn) -> str:
    """stock_prices の日付カラム名を実際にクエリして確認する。"""
    candidates = [
        (["code", "dateString", "open", "high", "low", "close", "volume"], "dateString"),
        (["code", "date",       "open", "high", "low", "close", "volume"], "date"),
        (["code", "trade_date", "open", "high", "low", "close", "volume"], "trade_date"),
    ]
    with conn.cursor() as cur:
        for columns, date_col in candidates:
            try:
                cur.execute(
                    f"SELECT {', '.join(columns)} FROM {PRICES_TABLE} LIMIT 1"
                )
                print(f"[INFO] 日付カラム: {date_col}")
                return date_col
            except Exception:
                continue
    raise RuntimeError(f"{PRICES_TABLE} のスキーマを検出できませんでした。")


def run_sql(conn, sql: str, dry_run: bool, label: str) -> None:
    print(f"\n[SQL] {label}")
    print(sql.strip())
    if dry_run:
        print("  ↑ --dry-run のためスキップ")
        return
    with conn.cursor() as cur:
        cur.execute(sql)
    print(f"  ✓ 完了")


def do_optimize(conn, date_col: str, dry_run: bool) -> None:
    table_type = detect_table_type(conn)
    print(f"[INFO] テーブル種別: {table_type}")

    if table_type in ("MATERIALIZED_VIEW", "VIEW"):
        print(
            f"[SKIP] {PRICES_TABLE} は {table_type} であるため OPTIMIZE は実行できません。\n"
            f"       Databricks がストレージを自動管理しています。\n"
            f"       MV を最新データに更新したい場合は以下を Databricks UI / Notebook で実行してください:\n"
            f"         REFRESH MATERIALIZED VIEW {PRICES_TABLE}"
        )
        return

    sql = f"OPTIMIZE {PRICES_TABLE} ZORDER BY (code, {date_col})"
    run_sql(conn, sql, dry_run, "OPTIMIZE + ZORDER")


def do_build_gold_tables(conn, date_col: str, dry_run: bool) -> None:
    """週足・月足の Gold テーブルを CREATE OR REPLACE TABLE ... AS SELECT で作成する。"""

    if date_col == "dateString":
        date_expr = "TO_DATE(dateString)"
        trunc_week = "DATE_TRUNC('WEEK', TO_DATE(dateString))"
        trunc_month = "DATE_TRUNC('MONTH', TO_DATE(dateString))"
    elif date_col == "trade_date":
        date_expr = "CAST(trade_date AS DATE)"
        trunc_week = "DATE_TRUNC('WEEK', CAST(trade_date AS DATE))"
        trunc_month = "DATE_TRUNC('MONTH', CAST(trade_date AS DATE))"
    else:  # date (epoch ms)
        date_expr = "CAST(FROM_UNIXTIME(date / 1000) AS DATE)"
        trunc_week = "DATE_TRUNC('WEEK', CAST(FROM_UNIXTIME(date / 1000) AS DATE))"
        trunc_month = "DATE_TRUNC('MONTH', CAST(FROM_UNIXTIME(date / 1000) AS DATE))"

    weekly_sql = f"""
CREATE OR REPLACE TABLE {WEEKLY_TABLE}
USING DELTA
AS
SELECT
    code,
    {trunc_week}                  AS week_start,
    FIRST_VALUE(open)  OVER w     AS open,
    MAX(high)          OVER w     AS high,
    MIN(low)           OVER w     AS low,
    LAST_VALUE(close)  OVER w     AS close,
    SUM(volume)        OVER w     AS volume
FROM {PRICES_TABLE}
WINDOW w AS (
    PARTITION BY code, {trunc_week}
    ORDER BY {date_expr}
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY code, {trunc_week}
    ORDER BY {date_expr} DESC
) = 1
"""

    monthly_sql = f"""
CREATE OR REPLACE TABLE {MONTHLY_TABLE}
USING DELTA
AS
SELECT
    code,
    {trunc_month}                 AS month_start,
    FIRST_VALUE(open)  OVER w     AS open,
    MAX(high)          OVER w     AS high,
    MIN(low)           OVER w     AS low,
    LAST_VALUE(close)  OVER w     AS close,
    SUM(volume)        OVER w     AS volume
FROM {PRICES_TABLE}
WINDOW w AS (
    PARTITION BY code, {trunc_month}
    ORDER BY {date_expr}
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY code, {trunc_month}
    ORDER BY {date_expr} DESC
) = 1
"""

    run_sql(conn, weekly_sql, dry_run, f"CREATE Gold テーブル {WEEKLY_TABLE}")
    run_sql(conn, monthly_sql, dry_run, f"CREATE Gold テーブル {MONTHLY_TABLE}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Databricks テーブル最適化スクリプト")
    parser.add_argument("--optimize",     action="store_true", help="OPTIMIZE + ZORDER を実行")
    parser.add_argument("--gold-tables",  action="store_true", help="週足・月足 Gold テーブルを作成")
    parser.add_argument("--all",          action="store_true", help="--optimize + --gold-tables を両方実行")
    parser.add_argument("--dry-run",      action="store_true", help="SQL を表示するだけで実行しない")
    args = parser.parse_args()

    if not (args.optimize or args.gold_tables or args.all):
        parser.print_help()
        sys.exit(0)

    run_optimize    = args.optimize or args.all
    run_gold_tables = args.gold_tables or args.all

    print("[INFO] Databricks に接続中...")
    conn = get_connection()

    print("[INFO] スキーマを検出中...")
    date_col = detect_date_col(conn)

    if run_optimize:
        do_optimize(conn, date_col, args.dry_run)

    if run_gold_tables:
        do_build_gold_tables(conn, date_col, args.dry_run)
        # Gold テーブルは通常の Delta テーブルなので OPTIMIZE 可能
        for tbl in [WEEKLY_TABLE, MONTHLY_TABLE]:
            run_sql(conn, f"OPTIMIZE {tbl} ZORDER BY (code)", args.dry_run, f"OPTIMIZE {tbl}")

    conn.close()
    print("\n[INFO] 処理完了")


if __name__ == "__main__":
    main()
