import os
import streamlit as st
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

def get_spark():
    """Sparkセッションを取得または初期化する (Databricks Apps モードのみ)"""
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
