#!/usr/bin/env python3
"""
ウィンドウ関数のPandas実装
"""

import pandas as pd

# CSVを読み込み
df = pd.read_csv('ufo_sighting_data.csv', low_memory=False)

# 都市別の目撃件数を集計
city_counts = (df[(df['country'] == 'us') & 
                  df['state_province'].notna() & 
                  df['city'].notna()]
               .groupby(['state_province', 'city'])
               .size()
               .reset_index(name='sighting_count'))

# ランキング関数を適用
city_counts['city_rank'] = (city_counts.groupby('state_province')['sighting_count']
                            .rank(method='min', ascending=False)
                            .astype(int))

# 上位5都市のみフィルタ
result = (city_counts[city_counts['city_rank'] <= 5]
          .sort_values(['state_province', 'city_rank'])
          .head(100))

# 結果を保存
result.to_csv('query_results/04_window_function_pandas.csv', index=False)

# 結果のプレビュー
print("Pandas結果（最初の20行）:")
print(result.head(20))
print(f"\n総行数: {len(result)}")