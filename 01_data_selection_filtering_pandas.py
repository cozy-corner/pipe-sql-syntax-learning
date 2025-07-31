import pandas as pd

# CSVを読み込み（データ型を適切に処理）
df = pd.read_csv('ufo_sighting_data.csv', low_memory=False)
# 数値列を適切に変換
df['length_of_encounter_seconds'] = pd.to_numeric(df['length_of_encounter_seconds'], errors='coerce')

print("カラム名確認:")
print(df.columns.tolist())
print()

# 1. 基本的なフィルタリング：アメリカでの円盤型UFO目撃情報
print("1. 円盤型UFO目撃情報（アメリカ）:")
result1 = (df[(df['country'] == 'us') & (df['UFO_shape'] == 'disk')]
          [['Date_time', 'city', 'state_province', 'UFO_shape', 'length_of_encounter_seconds']]
          .sort_values('Date_time', ascending=False)
          .head(10))
print(result1)
print()

# 2. 長時間目撃（30分以上）
print("2. 長時間目撃（30分以上）:")
result2 = (df[(df['length_of_encounter_seconds'] >= 1800) & (df['country'] == 'us')]
          .sort_values('length_of_encounter_seconds', ascending=False)
          .assign(minutes=lambda x: x['length_of_encounter_seconds'] / 60)
          [['Date_time', 'city', 'state_province', 'UFO_shape', 'minutes', 'described_duration_of_encounter']]
          .head(20))
print(result2)