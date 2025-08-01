# BigQuery パイプ構文を試してみた - SQL、Pandasとの比較で理解する

## 1. パイプ構文とは

BigQueryの新しいクエリ構文。データ処理の流れを上から下へパイプ演算子（`|>`）で繋いで記述し、処理順序が明確。

Google Cloudの公式ブログ「[Exploring pipe syntax: Real world use cases](https://cloud.google.com/blog/ja/products/data-analytics/exploring-pipe-syntax-real-world-use-cases/)」でも詳しく紹介されている。

本記事では、Kaggleの[UFO Sightings Around the World](https://www.kaggle.com/datasets/camnugent/ufo-sightings-around-the-world)データセットを使用。従来のSQL、パイプ構文、Pandasの3つの構文を比較しながら、パイプ構文の基本を解説する。

比較対象としてPandasも取り上げる。PandasはPythonのデータ分析ライブラリで、表形式データの操作や前処理に広く使われている。筆者が機械学習による需要予測の前処理で使用した経験があり、メソッドチェーンによるデータ処理の流れがパイプ構文と似ているため、理解の助けになると考えた。

## 2. データ選択・フィルタリング

### 円盤型UFO目撃情報の抽出

アメリカでの円盤型UFO目撃情報を抽出する例。

**従来のSQL**
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

**パイプ構文**
```sql
FROM `your-project-id.ufo_sightings.sightings`
|> WHERE country = 'us' AND UFO_shape = 'disk'
|> SELECT Date_time, city, state_province, UFO_shape, length_of_encounter_seconds
|> ORDER BY Date_time DESC
|> LIMIT 10
```

**Pandas**
```python
result = (df[(df['country'] == 'us') & (df['UFO_shape'] == 'disk')]
         [['Date_time', 'city', 'state_province', 'UFO_shape', 'length_of_encounter_seconds']]
         .sort_values('Date_time', ascending=False)
         .head(10))
```

**比較ポイント**
- パイプ構文とPandasは処理の流れが上から下へ順番に記述
- 従来のSQLは英語の文法的な構造（SELECT ... FROM ... WHERE）
- パイプ構文では各ステップが明確に分離

### 長時間目撃（30分以上）の例

**従来のSQL**
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

**パイプ構文**
```sql
FROM `your-project-id.ufo_sightings.sightings`
|> WHERE length_of_encounter_seconds >= 1800 AND country = 'us'
|> EXTEND length_of_encounter_seconds / 60 AS minutes
|> ORDER BY length_of_encounter_seconds DESC
|> SELECT Date_time, city, state_province, UFO_shape, minutes, described_duration_of_encounter
|> LIMIT 20
```

**Pandas**
```python
result = (df[(df['length_of_encounter_seconds'] >= 1800) & (df['country'] == 'us')]
         .sort_values('length_of_encounter_seconds', ascending=False)
         .assign(minutes=lambda x: x['length_of_encounter_seconds'] / 60)
         [['Date_time', 'city', 'state_province', 'UFO_shape', 'minutes', 'described_duration_of_encounter']]
         .head(20))
```

パイプ構文では`EXTEND`演算子を使って新しい列を追加。Pandasの`assign`メソッドに相当。

**実行結果の例（最初の3行）**
| Date_time | city | state_province | UFO_shape | length_of_encounter_seconds |
|---|---|---|---|---|
| 9/9/2012 14:00 | norfolk | va | disk | 30.0 |
| 9/9/2010 00:31 | new york city (manhattan) | ny | disk | 180.0 |
| 9/9/2004 23:00 | kansas city | mo | disk | 5400.0 |

## 3. 集計処理

### UFO形状別統計

集計処理の例として、複数の集計関数を使用。

**従来のSQL**
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

**パイプ構文**
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

**Pandas**
```python
result = (df[df['UFO_shape'].notna() & df['length_of_encounter_seconds'].notna()]
         .groupby('UFO_shape')
         .agg({
             'UFO_shape': 'count',
             'length_of_encounter_seconds': ['mean', 'max', 'min']
         })
         .round(2))
result.columns = ['count', 'avg_duration', 'max_duration', 'min_duration']
result = result.reset_index()
result = (result[result['count'] >= 1000]
         .sort_values('count', ascending=False)
         .head(15))
```

**HAVINGとWHEREの違い**

従来のSQLでは集計後の条件に`HAVING`句を使用。パイプ構文では`WHERE`句で統一。構文がシンプルに。

パイプ構文の特徴として、`WHERE`句は何回でも使用可能：
- 1回目の`WHERE` - 元データのフィルタリング（従来のWHERE句）
- 2回目の`WHERE` - 集計後のフィルタリング（従来のHAVING句）
- 3回目以降の`WHERE` - ウィンドウ関数後のフィルタリング（従来のQUALIFY句）など

**実行結果（上位5形状）**
| UFO_shape | count | avg_duration | max_duration | min_duration |
|---|---|---|---|---|
| light | 16,565 | 13,170.3 | 66,276,000 | 0.01 |
| triangle | 7,865 | 1,664.3 | 2,631,600 | 0.01 |
| circle | 7,607 | 4,768.1 | 10,526,400 | 0.05 |
| fireball | 6,208 | 4,023.9 | 10,526,400 | 0.01 |
| unknown | 5,874 | 2,970.9 | 5,184,000 | 0.01 |

## 4. テーブル結合（参考）

パイプ構文でのJOIN記法を簡単に紹介。基本的な構文は以下の通り。

**従来のSQL**
```sql
SELECT t1.col1, t2.col2
FROM table1 t1
JOIN table2 t2 ON t1.id = t2.id
WHERE t1.status = 'active'
```

**パイプ構文**
```sql
FROM table1 AS t1
|> JOIN table2 AS t2 ON t1.id = t2.id
|> WHERE t1.status = 'active'
|> SELECT t1.col1, t2.col2
```

**Pandas**
```python
result = (table1[table1['status'] == 'active']
          .merge(table2, on='id', how='inner')
          [['col1', 'col2']])
```

JOINもパイプ演算子で繋げて記述可能。処理の流れが上から下へと明確になる。

## 5. ウィンドウ関数

### 州ごとのUFO目撃件数ランキング

各州の上位5都市を抽出する例。

**従来のSQL**
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

**パイプ構文**
```sql
FROM `your-project-id.ufo_sightings.sightings`
|> WHERE country = 'us' AND state_province IS NOT NULL AND city IS NOT NULL
|> AGGREGATE COUNT(*) as sighting_count GROUP BY state_province, city
|> EXTEND RANK() OVER (PARTITION BY state_province ORDER BY sighting_count DESC) as city_rank
|> WHERE city_rank <= 5
|> ORDER BY state_province, city_rank
```

**Pandas**
```python
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
          .sort_values(['state_province', 'city_rank']))
```

パイプ構文では、ウィンドウ関数は`EXTEND`演算子の中で使用。従来のSQLの`QUALIFY`句の代わりに`WHERE`句を使用。

**実行結果（最初の8行）**
| state_province | city | sighting_count | city_rank |
|---|---|---|---|
| ak | anchorage | 83 | 1 |
| ak | fairbanks | 48 | 2 |
| ak | wasilla | 23 | 3 |
| ak | north pole | 19 | 4 |
| ak | juneau | 12 | 5 |
| al | birmingham | 54 | 1 |
| al | huntsville | 47 | 2 |
| al | mobile | 23 | 3 |

## 6. 処理順序の違い

### 宣言的 vs 手続き的

集計処理を例に、処理順序の違いを確認。

**従来のSQL**
```sql
SELECT city, COUNT(*) as count
FROM table
WHERE country = 'us'
GROUP BY city
HAVING COUNT(*) >= 10
ORDER BY count DESC
LIMIT 10
```
実行順序: FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT

**パイプ構文**
```sql
FROM table
|> WHERE country = 'us'
|> AGGREGATE COUNT(*) as count GROUP BY city
|> WHERE count >= 10
|> ORDER BY count DESC
|> LIMIT 10
```
実行順序: 記述順序と同じ（上から下へ順次実行）

**Pandas**
```python
(df
 .query("country == 'us'")
 .groupby('city').size()
 .reset_index(name='count')
 .query('count >= 10')
 .sort_values('count', ascending=False)
 .head(10))
```
実行順序: 記述順序と同じ（メソッドチェーンの順）

パイプ構文とPandasは手続き的な記述で、処理の流れが直感的。一方、従来のSQLは宣言的で、記述順序と実行順序が異なる。

## 7. まとめ

### 3つの構文の特徴

| | 従来のSQL | パイプ構文 | Pandas |
|---|---|---|---|
| 記述スタイル | 宣言的 | 手続き的 | 手続き的 |
| 処理の流れ | 記述順 ≠ 実行順 | 記述順 = 実行順 | 記述順 = 実行順 |
| 可読性 | 複雑なクエリでは追いにくい | 上から下へ明確 | メソッドチェーンで明確 |
| デバッグ | 全体を実行 | 途中で切って確認可能 | 途中で切って確認可能 |

パイプ構文は、データ処理の流れを直感的に表現でき、Pandasユーザーにとっても理解しやすい構文。BigQueryで複雑なクエリを書く際は、パイプ構文の利用がおすすめ。
