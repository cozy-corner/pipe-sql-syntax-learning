import pandas as pd

# CSVを読み込み（データ型を適切に処理）
df = pd.read_csv('ufo_sighting_data.csv', low_memory=False)
df['length_of_encounter_seconds'] = pd.to_numeric(df['length_of_encounter_seconds'], errors='coerce')

# 1. 基本的な集計：国別のUFO目撃件数
print("1. 国別のUFO目撃件数:")
result1 = (df.groupby('country')
          .size()
          .reset_index(name='sighting_count')
          .sort_values('sighting_count', ascending=False)
          .head(10))
print(result1)
print()

# 2. 複数の集計関数：UFO形状別の統計
print("2. UFO形状別の統計:")
result2 = (df[df['UFO_shape'].notna() & df['length_of_encounter_seconds'].notna()]
          .groupby('UFO_shape')
          .agg({
              'UFO_shape': 'count',
              'length_of_encounter_seconds': ['mean', 'max', 'min']
          })
          .round(2))

# カラム名を整理
result2.columns = ['count', 'avg_duration', 'max_duration', 'min_duration']
result2 = result2.reset_index()

# 1000件以上のみ抽出してソート
result2 = (result2[result2['count'] >= 1000]
          .sort_values('count', ascending=False)
          .head(15))
print(result2)