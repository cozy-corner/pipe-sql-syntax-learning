# 集計処理比較

## 1. 基本的な集計：国別のUFO目撃件数

### 従来のSQL
```sql
SELECT 
  country,
  COUNT(*) as sighting_count
FROM `your-project-id.ufo_sightings.sightings`
GROUP BY country
ORDER BY sighting_count DESC
LIMIT 10
```

### Pipe Syntax
```sql
FROM `your-project-id.ufo_sightings.sightings`
|> AGGREGATE 
    COUNT(*) as sighting_count
    GROUP BY country
|> ORDER BY sighting_count DESC
|> LIMIT 10
```

### Pandas
```python
import pandas as pd

# CSVを読み込み（データ型を適切に処理）
df = pd.read_csv('ufo_sighting_data.csv', low_memory=False)
df['length_of_encounter_seconds'] = pd.to_numeric(df['length_of_encounter_seconds'], errors='coerce')

result = (df.groupby('country')
         .size()
         .reset_index(name='sighting_count')
         .sort_values('sighting_count', ascending=False)
         .head(10))
```

## 2. 複数の集計関数：UFO形状別の統計

### 従来のSQL
```sql
SELECT 
  UFO_shape,
  COUNT(*) as count,
  AVG(length_of_encounter_seconds) as avg_duration,
  MAX(length_of_encounter_seconds) as max_duration,
  MIN(length_of_encounter_seconds) as min_duration
FROM `your-project-id.ufo_sightings.sightings`
WHERE UFO_shape IS NOT NULL
  AND length_of_encounter_seconds IS NOT NULL
GROUP BY UFO_shape
HAVING COUNT(*) >= 1000
ORDER BY count DESC
LIMIT 15
```

### Pipe Syntax
```sql
FROM `your-project-id.ufo_sightings.sightings`
|> WHERE UFO_shape IS NOT NULL AND length_of_encounter_seconds IS NOT NULL
|> AGGREGATE 
    COUNT(*) as count,
    AVG(length_of_encounter_seconds) as avg_duration,
    MAX(length_of_encounter_seconds) as max_duration,
    MIN(length_of_encounter_seconds) as min_duration
    GROUP BY UFO_shape
|> WHERE count >= 1000
|> ORDER BY count DESC
|> LIMIT 15
```

### Pandas
```python
result = (df[df['UFO_shape'].notna() & df['length_of_encounter_seconds'].notna()]
         .groupby('UFO_shape')
         .agg({
             'UFO_shape': 'count',
             'length_of_encounter_seconds': ['mean', 'max', 'min']
         })
         .round(2))

# カラム名を整理
result.columns = ['count', 'avg_duration', 'max_duration', 'min_duration']
result = result.reset_index()

# 1000件以上のみ抽出してソート
result = (result[result['count'] >= 1000]
         .sort_values('count', ascending=False)
         .head(15))
```

## Pipe Syntaxの集計処理の特徴

1. **AGGREGATE演算子**: `AGGREGATE ... GROUP BY`構文で集計を実行
2. **HAVING句の統合**: 集計後の条件も通常のWHERE句として記述
3. **読みやすい流れ**: フィルタ → 集計 → 後処理の順序が明確
4. **自動カラム出力**: グループ化カラムと集計カラムが自動的に出力される

### HAVING句との比較

**従来のSQL**:
```sql
SELECT UFO_shape, COUNT(*) as count
FROM table
GROUP BY UFO_shape  
HAVING COUNT(*) >= 1000  -- 集計後の条件
```

**Pipe Syntax**:
```sql
FROM table
|> AGGREGATE COUNT(*) as count GROUP BY UFO_shape
|> WHERE count >= 1000  -- 集計後の条件（HAVINGの代わり）
```

Pipe Syntaxでは「2回目のWHERE」がHAVING句の役割を果たし、構文が統一されています。

## 実行結果ファイル

- `query_results/03_country_counts.csv` - 国別UFO目撃件数
- `query_results/04_shape_statistics.csv` - UFO形状別統計

**確認済み**: 従来SQLとPipe Syntaxで完全に同じ結果が得られています。

## 実行してみよう

```bash
# Pipe Syntaxでの実行例
bq query --use_legacy_sql=false "
FROM \`your-project-id.ufo_sightings.sightings\`
|> AGGREGATE 
    COUNT(*) as sighting_count
    GROUP BY country
|> ORDER BY sighting_count DESC
|> LIMIT 10
"
```