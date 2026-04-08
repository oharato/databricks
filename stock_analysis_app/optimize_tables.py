"""
Delta テーブル最適化スクリプト (#2)

- stock_prices を code 列で ZORDER することで、銘柄コードを WHERE 条件に
  したクエリのファイルスキャン量を大幅に削減する。
- 初回実行は数分かかる場合があります。以降は差分のみ最適化されるため高速。

実行方法:
    just optimize-tables
    # または
    python stock_analysis_app/optimize_tables.py
"""

import os
import sys
import time
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

HOST = os.getenv("DATABRICKS_HOST")
PATH = os.getenv("DATABRICKS_HTTP_PATH")
TOKEN = os.getenv("DATABRICKS_TOKEN")

if not (HOST and PATH and TOKEN):
    sys.exit("[ERROR] DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN が未設定です")

from databricks import sql  # noqa: E402

STATEMENTS = [
    # 銘柄コードと日付カラムで Z-ORDER → WHERE code = '...' が高速化
    "OPTIMIZE main.default.stock_prices ZORDER BY (code)",
    # stock_list は行数が少ないため ZORDER 不要、OPTIMIZE のみ
    "OPTIMIZE main.default.stock_list",
]

print(f"接続先: {HOST}")
conn = sql.connect(server_hostname=HOST, http_path=PATH, access_token=TOKEN)

try:
    for stmt in STATEMENTS:
        print(f"\n実行中: {stmt}")
        t0 = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(stmt)
            result = cur.fetchall()
        elapsed = time.perf_counter() - t0
        print(f"  完了 ({elapsed:.1f}s)  結果: {result}")
finally:
    conn.close()

print("\nすべての最適化が完了しました。")
