ファイルを同期
ソースコードのあるフォルダに移動
cd my-app-source
ソースファイルをDatabricksに同期
databricks sync --watch . /Workspace/Users/oharato@live.jp/stock-charts
app.yamlとrequirements.txtの作成
起動時に実行するコマンドラインと設定する環境変数を記述したapp.yamlというファイルを作成します。
requirements.txtというファイルを作成し、pipでインストールするパッケージを一覧表示します。
Databricksアプリへのデプロイ
databricks apps deploy stock-charts --source-code-path /Workspace/Users/oharato@live.jp/stock-charts
後続のデプロイでは、完全なパスを省略できます。