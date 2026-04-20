# beam-pubsub-to-bigtable

Apache Beam (Go SDK) を使って Pub/Sub から Bigtable へストリーミング書き込みを行うパイプライン。
ランナーは DirectRunner（ローカル）と Dataflow に対応。

## アーキテクチャ

```
Pub/Sub (location-sub)
        │  Locations (protobuf)
        ▼
ConvertToMutation   ── Location ごとに KV<RowKey, Mutation> に変換
        │
        ▼
WriteToBigtable     ── bundle 単位で ApplyBulk (最大 1000 件/バッチ)
        │
        ▼
Bigtable (location テーブル / measurements カラムファミリ)
```

- **RowKey**: `{user_id}#{walking_id}#{reverse_timestamp}`
  最新レコードが先頭に並ぶよう `MaxInt64 - UnixNano` で降順に整列。
- **Column**: `measurements:data` に `Location` の protobuf バイト列をそのまま保存。
- **バリデーション**: `user_id` / `walking_id` / `timestamp` が欠けているレコードは破棄。

## ディレクトリ構成

```
cmd/location/        # Beam パイプラインのエントリポイント / ParDo
internal/bigtable/   # Bigtable Writer (DoFn)
proto/location/      # .proto 定義 (location.v1)
pkg/pb/              # buf generate で生成される Go コード
terraform/           # Pub/Sub トピック / Bigtable インスタンス・テーブル
.github/workflows/   # Dataflow デプロイ (workflow_dispatch)
```

## 前提

- Go 1.25+
- [buf](https://buf.build/) (proto 再生成する場合)
- `gcloud` CLI と GCP プロジェクトへの権限
- Terraform (インフラを構築する場合)

## セットアップ

### 1. インフラ構築

```bash
cd terraform
terraform init
terraform apply
```

作成されるもの:
- Pub/Sub トピック `location` とサブスクリプション `location-sub`
- Bigtable インスタンス `dataflow-sample` (asia-northeast1-a / SSD 1 ノード)
- テーブル `location` / カラムファミリ `measurements` (GC: 最長 7 日)

### 2. proto コード生成

```bash
buf generate
```

`pkg/pb/location/v1/location.pb.go` が出力される。

## 実行

### ローカル (DirectRunner)

```bash
go run ./cmd/location/ \
  --input_subscription=location-sub \
  --bigtable_project=<PROJECT_ID> \
  --bigtable_instance=dataflow-sample \
  --bigtable_table=location
```

### Dataflow

```bash
go run ./cmd/location/ \
  --input_subscription=location-sub \
  --bigtable_project=<PROJECT_ID> \
  --bigtable_instance=dataflow-sample \
  --bigtable_table=location \
  --runner=dataflow \
  --project=<PROJECT_ID> \
  --region=asia-northeast1 \
  --machine_type=e2-standard-2 \
  --staging_location=gs://<BUCKET>/staging \
  --temp_location=gs://<BUCKET>/temp \
  --job_name=<JOB_NAME>
```

既存ジョブを差し替える場合は `--update` を付与。

### GitHub Actions

`.github/workflows/deploy.yaml` を `workflow_dispatch` で起動すると Dataflow にデプロイされる。
以下の secrets を事前に設定:

| Secret | 用途 |
| --- | --- |
| `WIF_PROVIDER` | Workload Identity Federation のプロバイダ |
| `WIF_SERVICE_ACCOUNT` | デプロイに使うサービスアカウント |

## Pub/Sub に流すメッセージ形式

`proto/location/location.proto` の `Locations` を serialize したバイト列。

```proto
message Locations {
  repeated Location items = 1;
}

message Location {
  string walking_id = 1;
  google.protobuf.Timestamp timestamp = 2;
  double lat = 3;
  double lng = 4;
  double altitude = 5;
  double accuracy = 6;
  double speed = 7;
  string user_id = 8;
}
```
