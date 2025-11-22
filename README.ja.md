# Kafka DLQ Inspector

[English](README.md) | 日本語

Kafka のデッドレターキュー（DLQ）トピックを発見、検査、集約、エクスポート、リプレイ、監視するための包括的な Kotlin ベースの Spring Boot アプリケーションです。

## 概要

Kafka DLQ Inspector は、Apache Kafka 環境におけるデッドレターキューを管理するための堅牢なソリューションを提供します。このアプリケーションは、REST API エンドポイント、CLI コマンド、および Prometheus メトリクスを公開し、チームがメッセージ処理の失敗を監視および回復するのを支援します。

## 機能

- **トピック発見**: 設定可能な正規表現パターンに一致する DLQ トピックを自動的に発見
- **メッセージ検査**: JSON および Apache Avro フォーマットをサポートした DLQ メッセージのストリーミングとデコード
- **高度なフィルタリング**: パーティション、オフセット範囲、タイムスタンプ、ヘッダー、例外タイプによるメッセージのフィルタリング
- **ページネーション**: 効率的なブラウジングのために設定可能なページサイズでメッセージを取得
- **集約**: トピック、パーティション、例外タイプ、時間ウィンドウ別の統計を計算
- **メッセージリプレイ**: ドライランモードとレート制限機能を備えた、元のトピックまたは代替トピックへのメッセージのリプレイ
- **エクスポート**: オフライン分析用に JSON または CSV フォーマットでメッセージをエクスポート
- **アラート**: しきい値を超えた場合の Slack およびジェネリック Webhook による設定可能な通知
- **メトリクス**: 包括的な Prometheus メトリクスと Spring Boot Actuator エンドポイント
- **CLI**: スクリプトと自動化のためのオプションのコマンドラインインターフェース
- **OpenAPI**: /swagger-ui.html でのインタラクティブな API ドキュメント

## 要件

- Java 21 以降
- Apache Kafka 2.8+（3.x でテスト済み）
- Docker（Kafka をローカルで実行し、統合テストを行うため）
- Gradle 8+（wrapper 経由で含まれる）

## ローカルでの実行

### 前提条件

システムに Docker がインストールされ、実行されていることを確認してください

### アプリケーションのビルド

Gradle を使用して fat JAR をビルドします：

```bash
./gradlew clean build shadowJar
```

アーティファクトは `build/libs/kafka-dlq-inspector-0.1.0-all.jar` に作成されます

### サポートサービスの起動

Docker Compose を使用して Kafka および Schema Registry を起動します：

```bash
docker compose -f docker/docker-compose.yml up -d kafka schema-registry
```

これにより以下が起動されます：
- Kafka ブローカー（ポート 9092）
- Schema Registry（ポート 8081）

### アプリケーションの実行

アプリケーションを起動します：

```bash
java -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar
```

API は `http://localhost:8080` で利用可能になります

### インストールの確認

ヘルスエンドポイントを確認します：

```bash
curl http://localhost:8080/actuator/health
```

OpenAPI ドキュメントにアクセスします：

```bash
open http://localhost:8080/swagger-ui.html
```

## デモスクリプト

すべての機能を実演する包括的なデモスクリプトが提供されています：

```bash
./demo.sh
```

このスクリプトは以下を行います：
- すべての依存関係を起動
- アプリケーションをビルドして実行
- テスト用の DLQ トピックとサンプルデータを作成
- CLI コマンドを実演
- REST API エンドポイントを実演
- 高度な使用シナリオを表示

サービスを停止するには：

```bash
./demo.sh stop
```

## 設定

アプリケーションは `src/main/resources/application.yml` で設定されます。主要な設定プロパティ：

### Kafka 接続

```yaml
spring:
  kafka:
    bootstrap-servers:
      - localhost:9092
    properties:
      schema.registry.url: http://localhost:8081
```

### DLQ 発見

```yaml
kafka:
  dlq:
    topic-pattern: ".*\\.dlq"  # DLQ トピックに一致する正規表現パターン
    poll-size: 500              # ポーリングごとの最大レコード数
    poll-timeout-ms: 1000       # コンシューマのポーリングタイムアウト
    max-records: 1000           # 操作ごとに読み取る最大レコード数
```

### API と検索の制限

```yaml
kafka:
  export:
    max-records: 1000           # エクスポート操作ごとの最大レコード数
  search:
    default-limit: 200          # デフォルトの検索結果制限
  aggregation:
    limit-per-topic: 500        # 集約用のトピックごとの最大メッセージ数
  replay:
    max-messages: 1000          # リプレイ操作ごとの最大メッセージ数
  alerting:
    threshold: 1000             # デフォルトのアラートしきい値

api:
  pagination:
    default-page-size: 50       # ページネーションエンドポイントのデフォルトページサイズ

cli:
  default-limit: 500            # CLI コマンドのデフォルト制限
```

### エラーハンドリング

```yaml
kafka:
  kafka-config:
    error-handler-backoff-ms: 5000  # エラーハンドラのバックオフ間隔
```

### 通知

```yaml
notifications:
  slack:
    webhook-url: ""             # アラート用の Slack Webhook URL
  webhook:
    url: ""                     # アラート用のジェネリック Webhook URL
```

## API エンドポイント

OpenAPI 仕様は以下にあります：
```
src/main/resources/openapi.yaml
```

### トピック

- `GET /api/topics` - 発見されたすべての DLQ トピックをリスト

### メッセージ

- `GET /api/topics/{topic}/messages` - ページネーション付きでトピックからメッセージをリスト
  - クエリパラメータ：`partition`、`offsetFrom`、`offsetTo`、`page`、`size`
- `GET /api/topics/{topic}/messages/{partition}/{offset}` - 特定のメッセージを取得

### 集約

- `POST /api/aggregations` - フィルタを使用して集約を計算

### リプレイ

- `POST /api/replay` - メッセージを宛先トピックにリプレイ
  - リクエストボディ：ソーストピック、宛先トピック、フィルタ、ドライランフラグ、レート制限を含む `ReplayRequest`

### エクスポート

- `POST /api/export/json` - メッセージを JSON フォーマットでエクスポート
- `POST /api/export/csv` - メッセージを CSV フォーマットでエクスポート

### Actuator

- `GET /actuator/health` - アプリケーションのヘルスステータス
- `GET /actuator/info` - アプリケーション情報
- `GET /actuator/prometheus` - Prometheus メトリクス

### OpenAPI ドキュメント

- `GET /swagger-ui.html` - インタラクティブな API ドキュメント
- `GET /v3/api-docs` - OpenAPI 仕様（JSON）

## CLI の使用

`cli.enabled` プロパティを設定して CLI を有効にします：

```bash
java -Dcli.enabled=true -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq [command]
```

利用可能なコマンド：

- `list-topics` - すべての DLQ トピックをリスト
- `show --topic <topic> --limit <n>` - トピックからメッセージを表示
- `aggregate` - 集約統計を表示
- `replay --source <topic> --destination <topic> --dry-run <true|false>` - メッセージをリプレイ
- `export --topic <topic> --file <path>` - メッセージを JSON にエクスポート

## 監視

### Prometheus メトリクス

アプリケーションはカスタムメトリクスを公開します：

- `dlq.messages.read` - DLQ トピックから読み取られたメッセージのカウンター
- `dlq.replay.attempts` - リプレイ試行のカウンター
- `dlq.alerts.triggered` - トリガーされたアラートのカウンター

Prometheus がメトリクスエンドポイントをスクレイプするように設定します：

```yaml
scrape_configs:
  - job_name: 'kafka-dlq-inspector'
    static_configs:
    - targets: ['localhost:8080']
    metrics_path: '/actuator/prometheus'
```
