#!/bin/bash

# 1. CSVファイルの存在確認
if [ ! -f "ufo_sighting_data.csv" ]; then
  echo "エラー: ufo_sighting_data.csv が見つかりません"
  echo "以下のURLからデータをダウンロードしてください："
  echo "https://www.kaggle.com/datasets/camnugent/ufo-sightings-around-the-world"
  exit 1
fi

echo "CSVファイルが見つかりました。BigQueryへのロードを開始します..."

# 2. プロジェクトIDを設定（環境変数から取得）
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project)}"
DATASET_NAME="ufo_sightings"
TABLE_NAME="sightings"

echo "使用するプロジェクト: ${PROJECT_ID}"

# 3. CSVヘッダーを修正（直接ファイルを書き換え）
sed -i '' '1s/state\/province/state_province/g' ufo_sighting_data.csv

# 4. データセット作成
bq mk --dataset --location=US ${PROJECT_ID}:${DATASET_NAME} 2>/dev/null || echo "データセットは既に存在します"

# 5. BigQueryにロード
bq load \
  --autodetect \
  --source_format=CSV \
  --replace \
  --max_bad_records=100 \
  ${PROJECT_ID}:${DATASET_NAME}.${TABLE_NAME} \
  ufo_sighting_data.csv

# 6. 確認
bq query --use_legacy_sql=false "SELECT COUNT(*) as total_sightings FROM \`${PROJECT_ID}.${DATASET_NAME}.${TABLE_NAME}\`"
