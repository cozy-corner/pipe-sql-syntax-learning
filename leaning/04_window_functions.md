# ウィンドウ関数比較

## ウィンドウ関数の例：州ごとのUFO目撃件数ランキング

### 従来のSQL
```sql
SELECT 
  state_province,
  city,
  COUNT(*) as sighting_count,
  RANK() OVER (PARTITION BY state_province ORDER BY COUNT(*) DESC) as city_rank
FROM `your-project-id.ufo_sightings.sightings`
WHERE country = 'us' 
  AND state_province IS NOT NULL 
  AND city IS NOT NULL
GROUP BY state_province, city
QUALIFY city_rank <= 5
ORDER BY state_province, city_rank
```

### Pipe Syntax
```sql
FROM `your-project-id.ufo_sightings.sightings`
|> WHERE country = 'us' AND state_province IS NOT NULL AND city IS NOT NULL
|> AGGREGATE COUNT(*) as sighting_count GROUP BY state_province, city
|> EXTEND RANK() OVER (PARTITION BY state_province ORDER BY sighting_count DESC) as city_rank
|> WHERE city_rank <= 5
|> ORDER BY state_province, city_rank
```

### Pandas
```python
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

# 上位5都市のみフィルタ（BigQueryと同じく100行に制限）
result = (city_counts[city_counts['city_rank'] <= 5]
          .sort_values(['state_province', 'city_rank'])
          .head(100))
```


## Pipe Syntaxのウィンドウ関数の特徴

1. **EXTEND演算子での使用**: ウィンドウ関数は`EXTEND`演算子内で使用
2. **PARTITION BYとORDER BY**: 従来のSQLと同じ構文でウィンドウを定義
3. **QUALIFY句の代替**: Pipe Syntaxでは`WHERE`句でウィンドウ関数の結果をフィルタ
4. **処理順序**: AGGREGATE → EXTEND（ウィンドウ関数） → WHERE（フィルタ）の順
5. **複数のウィンドウ関数**: 一つの`EXTEND`内で複数のウィンドウ関数を定義可能

## 実行結果ファイル

- `query_results/04_window_function_results.csv` - ウィンドウ関数の結果

## 実行してみよう

```bash
# ウィンドウ関数の例
bq query --use_legacy_sql=false "
FROM \`your-project-id.ufo_sightings.sightings\`
|> WHERE country = 'us' AND state_province IS NOT NULL AND city IS NOT NULL
|> AGGREGATE COUNT(*) as sighting_count GROUP BY state_province, city
|> EXTEND RANK() OVER (PARTITION BY state_province ORDER BY sighting_count DESC) as city_rank
|> WHERE city_rank <= 5
|> ORDER BY state_province, city_rank
"
```