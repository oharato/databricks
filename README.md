# Stock Analysis App on Databricks

Databricks 上の株価データ (`main.default.stock_prices`) を可視化する Streamlit アプリケーションです。
Databricks Apps としてデプロイすることを想定していますが、ローカル環境での開発もサポートしています。

## 前提条件

*   Python 3.10 以上
*   [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) がインストール済みであること
*   `just` コマンドランナー (オプションですが推奨)

## セットアップ

1.  **リポジトリの準備**:
    ```bash
    git clone <repository-url>
    cd <repository-dir>
    ```

2.  **依存ライブラリのインストール**:
    ```bash
    # venv の作成と有効化 (任意)
    python -m venv .venv
    source .venv/bin/activate

    # インストール
    pip install -r stock_analysis_app/requirements.txt
    
    # Just を使用する場合
    just install
    ```

3.  **環境設定 (.env)**:
    `stock_analysis_app/.env` ファイルを作成し、Databricks 接続情報を設定します。
    ローカル実行には `DATABRICKS_HTTP_PATH` (SQL Warehouse) が必要です。

    ```ini
    # .env
    DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
    DATABRICKS_TOKEN=dapi...
    DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
    ```

## 開発と実行

このプロジェクトは `Justfile` を使用してタスクを管理しています。

| コマンド | 説明 |
| :--- | :--- |
| `just run` | ローカルで Streamlit アプリを起動します (SQL Warehouse モード) |
| `just sync` | ファイルの変更を検知し、自動的に Databricks Workspace へ同期します |
| `just deploy` | コードを Databricks Workspace へ同期し、Databricks Apps へデプロイします |
| `just check-env` | `.env` ファイルの設定状況を確認します |

### ローカル実行 (Local SQL Mode)

ローカル環境では、軽量な **Databricks SQL Connector** を使用して SQL Warehouse へ接続します。Databricks Connect (Spark) のセットアップは不要です。

```bash
just run
# または
streamlit run stock_analysis_app/app.py
```

### デプロイ (Databricks Apps)

Databricks Apps 上では、**Databricks Connect** (Spark Session) を使用してデータにアクセスします。認証情報はプラットフォームによって自動的に管理されます。

```bash
just deploy
```
※ `Justfile` 内の `workspace_path` 変数が、ご自身のワークスペースパス (例: `/Workspace/Users/me@example.com/stock-charts`) と一致しているか確認してください。

## ディレクトリ構成

```text
.
├── Justfile                      # タスクランナー定義
├── docs/
│   └── SPEC.md                   # 仕様書
├── stock_analysis_app/
│   ├── .env                      # 環境変数 (git管理外)
│   ├── .gitignore
│   ├── app.py                    # アプリケーションのエントリーポイント
│   ├── app.yaml                  # Databricks Apps 設定
│   └── requirements.txt          # Python 依存関係
└── README.md
```


## GRANT

```sql
-- アプリのサービスプリンシパルに権限を付与
GRANT USE CATALOG ON CATALOG main TO `c4e81045-2e73-457c-b913-315bbd48ff1c`;
GRANT USE SCHEMA ON SCHEMA main.default TO `c4e81045-2e73-457c-b913-315bbd48ff1c`;
GRANT SELECT ON TABLE main.default.stock_prices TO `c4e81045-2e73-457c-b913-315bbd48ff1c`;
GRANT SELECT ON TABLE main.default.stock_list TO `c4e81045-2e73-457c-b913-315bbd48ff1c`;
```