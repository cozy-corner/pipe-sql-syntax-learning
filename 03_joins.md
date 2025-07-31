# 結合処理比較（テーブル結合）

## 基本的なテーブル結合：都市情報と統計情報の結合

都市の基本情報とその都市のUFO目撃統計を結合して、詳細な分析を行います。

### 従来のSQL
```sql
WITH us_cities AS (
  SELECT DISTINCT
    city,
    state_province
  FROM `your-project-id.ufo_sightings.sightings`
  WHERE country = 'us' AND city IS NOT NULL AND state_province IS NOT NULL
  LIMIT 20
),
city_stats AS (
  SELECT 
    city,
    state_province,
    COUNT(*) as total_sightings,
    AVG(length_of_encounter_seconds) as avg_duration
  FROM `your-project-id.ufo_sightings.sightings`
  WHERE country = 'us' AND city IS NOT NULL AND state_province IS NOT NULL
    AND length_of_encounter_seconds IS NOT NULL
  GROUP BY city, state_province
)
SELECT 
  c.city,
  c.state_province,
  s.total_sightings,
  ROUND(s.avg_duration, 2) as avg_duration_seconds
FROM us_cities c
JOIN city_stats s ON c.city = s.city AND c.state_province = s.state_province
ORDER BY s.total_sightings DESC
LIMIT 10
```

### Pipe Syntax
```sql
WITH us_cities AS (
  SELECT DISTINCT
    city,
    state_province
  FROM `your-project-id.ufo_sightings.sightings`
  WHERE country = 'us' AND city IS NOT NULL AND state_province IS NOT NULL
  LIMIT 20
),
city_stats AS (
  SELECT 
    city,
    state_province,
    COUNT(*) as total_sightings,
    AVG(length_of_encounter_seconds) as avg_duration
  FROM `your-project-id.ufo_sightings.sightings`
  WHERE country = 'us' AND city IS NOT NULL AND state_province IS NOT NULL
    AND length_of_encounter_seconds IS NOT NULL
  GROUP BY city, state_province
)
FROM us_cities AS c
|> JOIN city_stats AS s ON c.city = s.city AND c.state_province = s.state_province
|> SELECT 
    c.city,
    c.state_province,
    s.total_sightings,
    ROUND(s.avg_duration, 2) as avg_duration_seconds
|> ORDER BY total_sightings DESC
|> LIMIT 10
```

### Pandas
```python
import pandas as pd
import numpy as np

# CSVを読み込み（データ型を適切に処理）
df = pd.read_csv('ufo_sighting_data.csv', low_memory=False)

# 都市リスト（上位20都市）
us_cities = (df[(df['country'] == 'us') & df['city'].notna() & df['state_province'].notna()]
             [['city', 'state_province']]
             .drop_duplicates()
             .head(20))

# 都市別の統計情報
city_stats = (df[(df['country'] == 'us') & df['city'].notna() & df['state_province'].notna() & 
                 pd.to_numeric(df['length_of_encounter_seconds'], errors='coerce').notna()]
              .assign(length_of_encounter_seconds=lambda x: pd.to_numeric(x['length_of_encounter_seconds'], errors='coerce'))
              .groupby(['city', 'state_province'])
              .agg({
                  'city': 'size',
                  'length_of_encounter_seconds': 'mean'
              })
              .rename(columns={'city': 'total_sightings', 'length_of_encounter_seconds': 'avg_duration'})
              .reset_index())

# JOINして結果を取得
result = (us_cities.merge(city_stats, on=['city', 'state_province'], how='inner')
          .assign(avg_duration_seconds=lambda x: x['avg_duration'].round(2))
          [['city', 'state_province', 'total_sightings', 'avg_duration_seconds']]
          .sort_values('total_sightings', ascending=False)
          .head(10))

print(result)

# 注意: BigQueryとPandasでは、DISTINCT/drop_duplicates()の処理順序が異なるため、
# 取得される20都市が異なり、結果も異なります。構文の学習が目的です。
```

## Pipe Syntaxの結合処理の特徴

1. **統一された構文**: JOINも他の演算子と同様にパイプで接続
2. **明確な処理順序**: FROM → JOIN → SELECT → ORDER BY の流れ
3. **WITH句との組み合わせ**: CTEとPipe Syntaxを併用可能
4. **エイリアスの制限**: SELECT後はテーブルエイリアスが無効になる（ORDER BYでは列名を直接指定）
5. **テーブルエイリアス**: FROM句とJOIN句でテーブルエイリアス（c, s）を使用可能

## 実行結果ファイル

- `query_results/03_join_results.csv` - 都市情報と統計の結合結果

**確認済み**: 従来SQLとPipe Syntaxで完全に同じ結果が得られています。Pandasは処理順序の違いにより異なる結果となります。

## 実行してみよう

```bash
# WITH句とPipe Syntaxの組み合わせ例
bq query --use_legacy_sql=false --format=csv > query_results/03_join_results.csv "
WITH us_cities AS (
  SELECT DISTINCT city, state_province
  FROM \`your-project-id.ufo_sightings.sightings\`
  WHERE country = 'us' AND city IS NOT NULL AND state_province IS NOT NULL
  LIMIT 20
),
city_stats AS (
  SELECT city, state_province, COUNT(*) as total_sightings,
         AVG(length_of_encounter_seconds) as avg_duration
  FROM \`your-project-id.ufo_sightings.sightings\`
  WHERE country = 'us' AND city IS NOT NULL AND state_province IS NOT NULL
    AND length_of_encounter_seconds IS NOT NULL
  GROUP BY city, state_province
)
FROM us_cities AS c
|> JOIN city_stats AS s ON c.city = s.city AND c.state_province = s.state_province
|> SELECT c.city, c.state_province, s.total_sightings,
         ROUND(s.avg_duration, 2) as avg_duration_seconds
|> ORDER BY total_sightings DESC
|> LIMIT 10
"
```