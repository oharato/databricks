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

# stock_prices テーブルを OPTIMIZE + ZORDER BY (code, <date_col>) で最適化
optimize:
    @echo "stock_prices を最適化中 (OPTIMIZE + ZORDER)..."
    {{python}} stock_analysis_app/scripts/optimize_tables.py --optimize

# 週足・月足 Gold テーブルを事前集計して作成
build-gold-tables:
    @echo "Gold テーブル (週足・月足) を作成中..."
    {{python}} stock_analysis_app/scripts/optimize_tables.py --gold-tables

# OPTIMIZE + Gold テーブル作成を一括実行
optimize-all:
    @echo "全最適化処理を実行中..."
    {{python}} stock_analysis_app/scripts/optimize_tables.py --all

# SQL のみ確認（実行なし）
optimize-dry-run:
    {{python}} stock_analysis_app/scripts/optimize_tables.py --all --dry-run
