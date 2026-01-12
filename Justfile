# Justfile for Stock Analysis App

# -----------------------------------------------------------------------------
# 設定
# -----------------------------------------------------------------------------
set dotenv-filename := "stock_analysis_app/.env"
set dotenv-load := true

# パス定義
python := ".venv/bin/python"
streamlit := ".venv/bin/streamlit"
workspace_path := "/Workspace/Users/oharato@live.jp/stock-charts"

# -----------------------------------------------------------------------------
# タスク
# -----------------------------------------------------------------------------

# 利用可能なコマンド一覧を表示
default:
    @just --list

# ローカルで Streamlit アプリを起動 (.env の SQL Warehouse 設定を使用)
run:
    @echo "ローカルでアプリを起動します..."
    {{streamlit}} run stock_analysis_app/app.py

# コードの自動同期を開始 (Databricks Sync Watch モード)
sync:
    @echo "{{workspace_path}} への自動同期を開始します..."
    # stock_analysis_app フォルダの中身をリモートへ同期
    databricks sync --watch stock_analysis_app {{workspace_path}}

# Databricks Apps へデプロイ
deploy app_name="stock-charts":
    @echo "Databricks Workspace へコードを同期中..."
    databricks sync stock_analysis_app {{workspace_path}}
    
    @echo "Databricks Apps へデプロイ中: {{app_name}}"
    databricks apps deploy {{app_name}} --source-code-path {{workspace_path}}

# 依存ライブラリのインストール
install:
    @echo "Python ライブラリをインストール中..."
    {{python}} -m pip install -r stock_analysis_app/requirements.txt

# Databricks CLI のインストール (Linux/Mac)
install-cli:
    @echo "Databricks CLI をインストール中..."
    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh

# 接続設定(.env)の確認
check-env:
    @echo "環境変数の設定を確認中..."
    @if [ -z "$DATABRICKS_HOST" ]; then echo "[エラー] DATABRICKS_HOST が設定されていません"; else echo "[OK] DATABRICKS_HOST"; fi
    @if [ -z "$DATABRICKS_TOKEN" ]; then echo "[エラー] DATABRICKS_TOKEN が設定されていません"; else echo "[OK] DATABRICKS_TOKEN"; fi
    @if [ -z "$DATABRICKS_HTTP_PATH" ]; then echo "[警告] DATABRICKS_HTTP_PATH がありません (ローカル実行には必要です)"; else echo "[OK] DATABRICKS_HTTP_PATH"; fi
    @echo "完了"
