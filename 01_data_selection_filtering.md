# データ選択・フィルタリング比較

## 1. 基本的なフィルタリング：アメリカでの円盤型UFO目撃情報

### 従来のSQL
```sql
SELECT 
  Date_time,
  city,
  state_province,
  UFO_shape,
  length_of_encounter_seconds
FROM `your-project-id.ufo_sightings.sightings`
WHERE country = 'us' 
  AND UFO_shape = 'disk'
ORDER BY Date_time DESC
LIMIT 10
```

### Pipe Syntax
```sql
FROM `your-project-id.ufo_sightings.sightings`
|> WHERE country = 'us' AND UFO_shape = 'disk'
|> SELECT Date_time, city, state_province, UFO_shape, length_of_encounter_seconds
|> ORDER BY Date_time DESC
|> LIMIT 10
```

### Pandas
```python
import pandas as pd

# CSVを読み込み（データ型を適切に処理）
df = pd.read_csv('ufo_sighting_data.csv', low_memory=False)
df['length_of_encounter_seconds'] = pd.to_numeric(df['length_of_encounter_seconds'], errors='coerce')

result = (df[(df['country'] == 'us') & (df['UFO_shape'] == 'disk')]
         [['Date_time', 'city', 'state_province', 'UFO_shape', 'length_of_encounter_seconds']]
         .sort_values('Date_time', ascending=False)
         .head(10))
```

## 2. 複雑な条件：長時間目撃（30分以上）

### 従来のSQL
```sql
SELECT 
  Date_time,
  city,
  state_province,
  UFO_shape,
  length_of_encounter_seconds / 60 as minutes,
  described_duration_of_encounter
FROM `your-project-id.ufo_sightings.sightings`
WHERE length_of_encounter_seconds >= 1800
  AND country = 'us'
ORDER BY length_of_encounter_seconds DESC
LIMIT 20
```

### Pipe Syntax
```sql
FROM `your-project-id.ufo_sightings.sightings`
|> WHERE length_of_encounter_seconds >= 1800 AND country = 'us'
|> EXTEND length_of_encounter_seconds / 60 AS minutes
|> ORDER BY length_of_encounter_seconds DESC
|> SELECT Date_time, city, state_province, UFO_shape, minutes, described_duration_of_encounter
|> LIMIT 20
```

### Pandas
```python
result = (df[(df['length_of_encounter_seconds'] >= 1800) & (df['country'] == 'us')]
         .sort_values('length_of_encounter_seconds', ascending=False)
         .assign(minutes=lambda x: x['length_of_encounter_seconds'] / 60)
         [['Date_time', 'city', 'state_province', 'UFO_shape', 'minutes', 'described_duration_of_encounter']]
         .head(20))
```


## Pipe Syntaxの特徴と注意点

1. **読みやすさ**: データの流れが上から下へ順番に処理される
2. **段階的な処理**: 各ステップが明確に分離されている
3. **EXTENDによる列追加**: 新しい列を追加する際に`EXTEND column_expression AS alias`を使用
4. **処理順序が重要**: FROM → WHERE → EXTEND → ORDER BY → SELECT → LIMIT の順序で記述
5. **WHERE句の位置**: フィルタリングを早い段階で実行して効率化

## 実行結果ファイル

- `query_results/01_disk_ufo_results.csv` - 円盤型UFO目撃情報
- `query_results/02_long_duration_results.csv` - 長時間目撃情報

**確認済み**: 従来SQLとPipe Syntaxで完全に同じ結果が得られています。

## 実行してみよう

```bash
# Pipe Syntaxでの実行例
bq query --use_legacy_sql=false "
FROM \`your-project-id.ufo_sightings.sightings\`
|> WHERE country = 'us' AND UFO_shape = 'disk'
|> SELECT Date_time, city, state_province, UFO_shape, length_of_encounter_seconds
|> ORDER BY Date_time DESC
|> LIMIT 10
"
```