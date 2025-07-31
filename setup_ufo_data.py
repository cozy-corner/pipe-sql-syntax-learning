import pandas as pd
from google.cloud import bigquery
import requests
import zipfile
import io

# 1. Kaggleからデータをダウンロード（手動でダウンロードするか、Kaggle APIを使用）
# kaggle datasets download -d camnugent/ufo-sightings-around-the-world

# 2. CSVファイルを読み込み
df = pd.read_csv('ufo_sightings_data.csv')

# データの確認
print(f"データ件数: {len(df)}")
print(f"カラム: {df.columns.tolist()}")
print("\nデータサンプル:")
print(df.head())

# 3. BigQueryクライアントの初期化
client = bigquery.Client()

# 4. データセットの作成
dataset_id = "your_project_id.ufo_sightings"
dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"  # または "asia-northeast1"（東京）

# データセット作成（既存の場合はスキップ）
try:
    dataset = client.create_dataset(dataset, timeout=30)
    print(f"データセット {dataset.dataset_id} を作成しました")
except Exception as e:
    print(f"データセットは既に存在するか、エラーが発生しました: {e}")

# 5. BigQueryにアップロード
table_id = "your_project_id.ufo_sightings.sightings"

job_config = bigquery.LoadJobConfig(
    autodetect=True,  # スキーマ自動検出
    write_disposition="WRITE_TRUNCATE",  # 既存テーブルを上書き
)

# DataFrameをBigQueryにロード
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()  # 完了まで待機

print(f"テーブル {table_id} にデータをロードしました")
print(f"テーブルサイズ: {client.get_table(table_id).num_bytes / 1024 / 1024:.2f} MB")
print(f"行数: {client.get_table(table_id).num_rows}")