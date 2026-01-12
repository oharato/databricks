# アプリケーション仕様書 (Stock Analysis App)

## 概要
Databricks 上に格納された日本株の株価データ (`main.default.stock_prices`) を分析・可視化するためのダッシュボードアプリケーション。Streamlit フレームワークを使用し、Databricks Apps としてホスティングされる。

## システムアーキテクチャ

本アプリは、実行環境に応じて自動的に接続モードを切り替える「ハイブリッド接続アーキテクチャ」を採用している。

### 1. 接続モード

| モード | 実行環境 | 使用ライブラリ | 接続先リソース | 特徴 |
| :--- | :--- | :--- | :--- | :--- |
| **Local SQL Mode** | ローカル PC (VS Code等) | `databricks-sql-connector` | SQL Warehouse | **軽量**。Spark 環境不要。結果を Pandas DataFrame で受け取り処理する。 |
| **Databricks Apps Mode** | Databricks Apps (本番) | `databricks-connect` | Serverless Compute | **高速・スケーラブル**。Spark DataFrame API を使用して分散処理を行う。 |

### 2. データソース
*   **Catalog**: `main`
*   **Schema**: `default`
*   **Table**: `stock_prices`
    *   主なカラム: `code` (銘柄コード), `date` または `dateString` (日付), `open`, `high`, `low`, `close`, `volume`

## 機能要件

### 1. 設定サイドバー
*   **銘柄選択**: プリセットされた銘柄リスト (`TARGET_CODES`) から1つを選択する。
    *   現状のターゲット: `1301`, `3031`

### 2. データ可視化
選択された銘柄について、以下の3つの時間枠でチャートを並列表示する。各チャートには **TradingView Lightweight Charts** を使用する。

1.  **MONTHLY (月足)**
    *   表示期間: 過去3000日分
    *   集計: 月次データにリサンプリング
2.  **WEEKLY (週足)**
    *   表示期間: 過去600日分
    *   集計: 週次 (月曜始まり) データにリサンプリング
3.  **DAILY (日足)**
    *   表示期間: 過去120日分
    *   集計: なし (生データ)

### 3. テクニカル指標
各チャート上に、以下の単純移動平均線 (SMA) をオーバーレイ表示する。
*   **5日 (MA5)**: オレンジ色
*   **25日 (MA25)**: 紫色
*   **75日 (MA75)**: 緑色

## 技術スタック

*   **言語**: Python 3.10+
*   **UIフレームワーク**: Streamlit
*   **チャートライブラリ**: lightweight-charts (Python wrapper for TradingView Lightweight Charts)
*   **データ処理**:
    *   Pandas (ローカル用)
    *   PySpark (Databricks用)
*   **インフラ**: Databricks Apps

## デプロイフロー

1.  ローカル開発環境 (`Justfile`, `databricks sync`) を使用してコードを作成。
2.  Databricks Workspace 上のユーザーディレクトリへコードを同期。
3.  `databricks apps deploy` コマンドにより、アプリインスタンスを作成・更新。
