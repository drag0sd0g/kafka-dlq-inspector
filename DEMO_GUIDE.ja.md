# デモスクリプトガイド

[English](DEMO_GUIDE.md) | 日本語

このガイドでは、`demo.sh` スクリプトを使用して Kafka DLQ Inspector のすべての機能を実演する方法を説明します。

## クイックスタート

完全なデモを実行：
```bash
./demo.sh
```

すべてのサービスを停止：
```bash
./demo.sh stop
```

## デモの内容

デモスクリプトは以下のステップを自動的に実行します：

### 1. 前提条件のチェック
- Docker がインストールされていることを確認
- Java 21+ がインストールされていることを確認
- curl がインストールされていることを確認
- jq の確認（オプション、JSON 出力をより見やすくするため）

### 2. アプリケーションのビルド
- Gradle を使用してアプリケーション JAR をビルド
- JAR が既に存在する場合はリビルドをスキップ（リビルドオプション付き）

### 3. Docker サービスの起動
- ポート 2181 で Zookeeper を起動
- ポート 9092 で Kafka を起動
- ポート 8081 で Schema Registry を起動
- すべてのサービスが準備完了するまで待機

### 4. テストデータの作成
- DLQ トピックを作成：
  - `test-service-orders.dlq`（2 パーティション、10 メッセージ）
  - `test-service-payments.dlq`（1 パーティション、5 メッセージ）
  - `test-service-orders.retry`（1 パーティション、リプレイテスト用に空）
- サンプル JSON メッセージでトピックを追加
- テスト用にさまざまな例外タイプを含むメッセージ

### 5. アプリケーションの起動
- Kafka DLQ Inspector アプリケーションを起動
- アプリケーションが正常になるまで待機
- `/tmp/kafka-dlq-inspector.log` に出力をログ記録

### 6. CLI コマンドの実演
すべての CLI コマンドの例を表示：
- `list-topics` - すべての DLQ トピックをリスト
- `show` - トピックからメッセージを表示
- `aggregate` - 集約統計を表示
- `replay` - メッセージをリプレイ（ドライランモード）
- `export` - メッセージを JSON ファイルにエクスポート

### 7. REST API の実演
すべての REST API エンドポイントの例を表示：
- GET `/actuator/health` - ヘルスチェック
- GET `/api/topics` - トピックのリスト
- GET `/api/topics/{topic}/messages` - ページネーション付きメッセージのリスト
- GET `/api/topics/{topic}/messages/{partition}/{offset}` - 特定のメッセージを取得
- POST `/api/aggregations` - 集約を計算
- POST `/api/export/json` - JSON にエクスポート
- POST `/api/export/csv` - CSV にエクスポート
- POST `/api/replay` - メッセージをリプレイ
- GET `/actuator/prometheus` - Prometheus メトリクス
- OpenAPI ドキュメントへの参照

### 8. 高度なシナリオ
高度なフィルタリング機能を実演：
- パーティションでフィルタ
- オフセット範囲でフィルタ
- フィルタ付き集約
- 正規表現パターンで検索

### 9. サマリー
以下のサマリーを表示：
- 実行中のサービス
- 作成されたテストデータ
- 実演された機能
- 便利な URL とコマンド

## カスタマイズ

スクリプトの先頭でこれらの変数を変更することで、スクリプトをカスタマイズできます：

```bash
APP_PORT=8080              # アプリケーションポート
KAFKA_PORT=9092            # Kafka ブローカーポート
APP_JAR="..."              # アプリケーション JAR へのパス
TEST_TOPIC_PREFIX="test-service"  # テストトピックのプレフィックス
```

## トラブルシューティング

### Docker が利用できない
"Docker is not installed" エラーが表示される場合：
- Docker Desktop をインストール（Mac/Windows）
- または Docker Engine をインストール（Linux）

### ポートの競合
ポート 8080、9092、2181、または 8081 が既に使用されている場合：
- 競合するサービスを停止
- またはスクリプト内のポート番号を変更

### アプリケーションの起動失敗
アプリケーションログを確認：
```bash
tail -f /tmp/kafka-dlq-inspector.log
```

### サービスがクリーンに停止しない
Docker サービスを手動で停止：
```bash
docker compose -f docker/docker-compose.yml down -v
```

アプリケーションプロセスを終了：
```bash
kill $(cat /tmp/kafka-dlq-inspector.pid)
rm /tmp/kafka-dlq-inspector.pid
```

## 手動テスト

ステップを手動で実行したい場合：

1. Docker サービスを起動：
```bash
docker compose -f docker/docker-compose.yml up -d zookeeper kafka schema-registry
```

2. アプリケーションをビルド：
```bash
./gradlew clean build shadowJar
```

3. アプリケーションを起動：
```bash
java -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar
```

4. Kafka CLI ツールまたはデモスクリプトの `create_test_data()` 関数を使用してテストトピックとデータを作成

5. CLI コマンドをテスト：
```bash
java -Dcli.enabled=true -Dspring.main.web-application-type=none \
  -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar \
  dlq list-topics
```

6. REST API をテスト：
```bash
curl http://localhost:8080/api/topics | jq .
```

## テストシナリオ

### シナリオ 1：トピックの発見
**目標**：DLQ トピックが発見されることを確認

**ステップ**：
1. `./demo.sh` を実行してテストトピックを作成
2. CLI を使用：`java -Dcli.enabled=true -Dspring.main.web-application-type=none -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq list-topics`
3. または API を使用：`curl http://localhost:8080/api/topics`

**期待される結果**：`test-service-orders.dlq` と `test-service-payments.dlq` が表示されるはず

### シナリオ 2：メッセージの検査
**目標**：DLQ トピックからメッセージを表示

**ステップ**：
1. CLI を使用：`java -Dcli.enabled=true -Dspring.main.web-application-type=none -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq show --topic test-service-orders.dlq --limit 5`
2. または API を使用：`curl http://localhost:8080/api/topics/test-service-orders.dlq/messages?size=5`

**期待される結果**：注文データを含む 5 つのメッセージが表示されるはず

### シナリオ 3：集約
**目標**：DLQ メッセージに関する統計を取得

**ステップ**：
1. CLI を使用：`java -Dcli.enabled=true -Dspring.main.web-application-type=none -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq aggregate`
2. または API を使用：`curl -X POST http://localhost:8080/api/aggregations -H "Content-Type: application/json" -d '{}'`

**期待される結果**：メッセージ数、パーティション数、例外タイプの統計が表示されるはず

### シナリオ 4：メッセージリプレイ（ドライラン）
**目標**：メッセージリプレイ機能をテスト

**ステップ**：
1. CLI を使用：`java -Dcli.enabled=true -Dspring.main.web-application-type=none -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq replay --source test-service-orders.dlq --destination test-service-orders.retry --dry-run`
2. または API を使用：`curl -X POST http://localhost:8080/api/replay -H "Content-Type: application/json" -d '{"cluster":"local","sourceTopic":"test-service-orders.dlq","destinationTopic":"test-service-orders.retry","dryRun":true}'`

**期待される結果**：リプレイされるメッセージのリストが表示されるはず（実際にはリプレイされません）

### シナリオ 5：JSON へのエクスポート
**目標**：オフライン分析用に DLQ メッセージをエクスポート

**ステップ**：
1. CLI を使用：`java -Dcli.enabled=true -Dspring.main.web-application-type=none -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar dlq export --topic test-service-orders.dlq --file /tmp/orders.json`
2. または API を使用：`curl -X POST http://localhost:8080/api/export/json -H "Content-Type: application/json" -d '{"topics":["test-service-orders.dlq"]}' -o /tmp/orders.json`
3. エクスポートされたファイルを表示：`cat /tmp/orders.json | jq .`

**期待される結果**：トピックからのすべてのメッセージを含む JSON ファイルが表示されるはず

### シナリオ 6：パーティションでフィルタ
**目標**：特定のパーティションからメッセージを取得

**ステップ**：
1. API を使用：`curl "http://localhost:8080/api/topics/test-service-orders.dlq/messages?partition=0&size=3"`

**期待される結果**：パーティション 0 からのメッセージのみが表示されるはず

### シナリオ 7：オフセット範囲でフィルタ
**目標**：オフセット範囲内のメッセージを取得

**ステップ**：
1. API を使用：`curl "http://localhost:8080/api/topics/test-service-orders.dlq/messages?offsetFrom=0&offsetTo=2"`

**期待される結果**：オフセット 0、1、2 のメッセージが表示されるはず

### シナリオ 8：Prometheus による監視
**目標**：アプリケーションメトリクスを表示

**ステップ**：
1. メトリクスを表示：`curl http://localhost:8080/actuator/prometheus`
2. DLQ メトリクスをフィルタ：`curl -s http://localhost:8080/actuator/prometheus | grep dlq_`

**期待される結果**：`dlq_messages_read_total`、`dlq_replay_attempts_total` などのメトリクスが表示されるはず

## CI/CD との統合

デモスクリプトは、統合テスト用の CI/CD パイプラインで使用できます：

```yaml
# GitHub Actions ワークフローの例
- name: Run Integration Tests
  run: |
    ./demo.sh
    # ここにアサーションを追加
    ./demo.sh stop
```

## パフォーマンスに関する注意事項

- デモは小規模なデータセット（合計 15 メッセージ）を作成します
- パフォーマンステストの場合、`create_test_data()` でメッセージ数を増やします
- ロードテスト用に別のテストトピックを使用することを検討してください
- 大量のメッセージでのメモリ使用量を監視してください

## セキュリティに関する注意事項

- デモスクリプトはデフォルトのセキュリティ設定で実行されます
- 本番環境では、認証と認可を設定してください
- Kafka クラスタアクセスを保護してください
- API エンドポイントには HTTPS を使用してください
- 機密性の高い設定（Webhook URL など）を保護してください
