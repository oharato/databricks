import sys; sys.path.append('stock_analysis_app'); import data_provider; from utils import IS_SQL_MODE, HTTP_PATH, get_spark; import os; import pandas as pd; from databricks import sql;
conn = sql.connect(server_hostname=os.getenv('DATABRICKS_HOST'), http_path=HTTP_PATH, access_token=os.getenv('DATABRICKS_TOKEN'))
query = """
WITH latest_prices AS (
  SELECT code, close FROM main.default.stock_prices WHERE date = (SELECT MAX(date) FROM main.default.stock_prices)
),
latest_shares AS (
  SELECT ticker as code, number_of_issued_shares FROM main.default.edinet WHERE year IN (2023, 2024)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY submit_date DESC) = 1
)
SELECT p.code, p.close * s.number_of_issued_shares as market_cap
FROM latest_prices p
JOIN latest_shares s ON p.code = s.code
"""
with conn.cursor() as cursor:
    cursor.execute(query)
    df = cursor.fetchall_arrow().to_pandas()
print(df.head())
