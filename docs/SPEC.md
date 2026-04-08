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

| テーブル | 種別 | 主なカラム | 用途 |
| :--- | :--- | :--- | :--- |
| `main.default.stock_prices` | Materialized View | `code`, `dateString`, `open`, `high`, `low`, `close`, `volume` | 日足生データ（全銘柄） |
| `main.default.stock_list` | テーブル | `code`, `name`, `market`, `sector33`, `sector17`, `scale` | 銘柄マスタ |
| `main.default.stock_prices_weekly` | Delta テーブル (Gold) | `code`, `week_start`, `open`, `high`, `low`, `close`, `volume` | 週足事前集計 |
| `main.default.stock_prices_monthly` | Delta テーブル (Gold) | `code`, `month_start`, `open`, `high`, `low`, `close`, `volume` | 月足事前集計 |

> **注意**: `stock_prices` は Materialized View のため `OPTIMIZE` は適用不可。Databricks がストレージを自動管理する。最新データへの手動更新は `REFRESH MATERIALIZED VIEW main.default.stock_prices` で実施。

### 3. データ取得アーキテクチャ

複数銘柄のデータ取得は **バルク IN クエリ**（1回の SQL で全銘柄を取得）を採用している。

```
# 旧: N銘柄 × N回クエリ
WHERE code = '1301'  -- 銘柄ごとに発行
WHERE code = '1305'
...

# 現: 1クエリで全銘柄を取得
WHERE code IN ('1301', '1305', ...) AND <日付フィルタ> ORDER BY code, dateString
```

スキーマ検出（`dateString` / `date` / `trade_date`）はアプリ起動時に自動実行し、1時間キャッシュされる。

## 機能要件

### 1. メイン画面（`app.py`）

#### サイドバー
*   **お気に入りリスト管理**: 複数リストの作成・切り替え・削除
*   **銘柄フィルタ**: 市場 / 33業種 / 17業種 でのチェックボックスフィルタ
*   **銘柄検索**: コード・名称でのテキスト検索
*   **一括選択 / クリア**: フィルタ結果の全選択・全解除

#### チャート表示
選択された銘柄について、以下の3つの時間枠でチャートを並列表示する。各チャートには **TradingView Lightweight Charts** を使用する。

| インターバル | 表示期間 | 集計 |
| :--- | :--- | :--- |
| MONTHLY (月足) | 過去 5000日 | 月次リサンプリング |
| WEEKLY (週足) | 過去 600日 | 週次リサンプリング (月曜始まり) |
| DAILY (日足) | 過去 120日 | 生データ |

### 2. セクターチャート画面（`pages/01_Sector_Charts.py`）

国内プライム 33業種の代表銘柄（TOPIX 規模順・最大5銘柄/業種）を一覧表示する監視画面。

#### 表示期間（監視用に短縮）

| インターバル | 表示期間 |
| :--- | :--- |
| MONTHLY (月足) | 過去 3年 (1095日) |
| WEEKLY (週足) | 過去 1年 (365日) |
| DAILY (日足) | 過去 6か月 (180日) |

#### パフォーマンス設計
*   全銘柄のデータを **1 クエリ**（`load_bulk_multi_interval_data`）で取得
*   描画ループを `@st.fragment` でラップし、ページ全体のリランを防止
*   コンパクトチャート（`render_chart_compact`）を使用: ツールボックス・凡例・クロスヘア・スクロール・スケール無効

### 3. テクニカル指標
各チャート上に以下の単純移動平均線 (SMA) をオーバーレイ表示する。
*   **5日 (MA5)**: オレンジ色
*   **25日 (MA25)**: 紫色
*   **75日 (MA75)**: 緑色

## 技術スタック

*   **言語**: Python 3.10+
*   **UIフレームワーク**: Streamlit
*   **チャートライブラリ**: lightweight-charts (Python wrapper for TradingView Lightweight Charts)
*   **データ処理**:
    *   Pandas (ローカル用)
    *   PySpark (Databricks Apps用)
*   **インフラ**: Databricks Apps

## デプロイフロー

1.  ローカル開発環境 (`Justfile`, `databricks sync`) を使用してコードを作成。
2.  Databricks Workspace 上のユーザーディレクトリへコードを同期。
3.  `databricks apps deploy` コマンドにより、アプリインスタンスを作成・更新。

## テーブル最適化フロー

Gold テーブルの初回作成および定期更新は `just` コマンドで実施する。

```bash
just build-gold-tables   # 週足・月足 Gold テーブルを作成・更新
just optimize-all        # Gold テーブル作成 + OPTIMIZE + ZORDER
just optimize-dry-run    # 実行せず SQL のみ確認
```

> `stock_prices` は Materialized View のため `just optimize` はスキップされる（エラーにはならない）。
> Gold テーブル（`stock_prices_weekly` / `stock_prices_monthly`）には `OPTIMIZE ZORDER BY (code)` が適用される。
