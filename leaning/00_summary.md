# BigQuery Pipe Syntax 学習まとめ

## Pipe Syntaxとは

BigQueryの新しいクエリ構文で、データ処理の流れを上から下へパイプ（`|>`）で繋いで記述します。従来のSQLよりも読みやすく、処理の順序が明確になります。

## 構文比較一覧

### 1. データ選択・フィルタリング

#### 従来のSQL
```sql
SELECT column1, column2
FROM table
WHERE condition
ORDER BY column1 DESC
LIMIT 10
```

#### Pipe Syntax
```sql
FROM table
|> WHERE condition
|> SELECT column1, column2
|> ORDER BY column1 DESC
|> LIMIT 10
```

#### Pandas
```python
result = (df[df['condition']]
         [['column1', 'column2']]
         .sort_values('column1', ascending=False)
         .head(10))
```

### 2. 集計処理

#### 従来のSQL
```sql
SELECT 
  group_column,
  COUNT(*) as count,
  AVG(value) as avg_value
FROM table
WHERE condition
GROUP BY group_column
HAVING COUNT(*) >= 100
ORDER BY count DESC
```

#### Pipe Syntax
```sql
FROM table
|> WHERE condition
|> AGGREGATE 
    COUNT(*) as count,
    AVG(value) as avg_value
    GROUP BY group_column
|> WHERE count >= 100
|> ORDER BY count DESC
```

#### Pandas
```python
result = (df[df['condition']]
         .groupby('group_column')
         .agg({'column': 'count', 'value': 'mean'})
         .rename(columns={'column': 'count', 'value': 'avg_value'})
         .reset_index()
         .query('count >= 100')
         .sort_values('count', ascending=False))
```

### 3. テーブル結合

#### 従来のSQL
```sql
SELECT 
  t1.column1,
  t2.column2
FROM table1 t1
JOIN table2 t2 ON t1.id = t2.id
WHERE t1.condition = true
ORDER BY t1.column1
```

#### Pipe Syntax
```sql
FROM table1 AS t1
|> JOIN table2 AS t2 ON t1.id = t2.id
|> WHERE t1.condition = true
|> SELECT t1.column1, t2.column2
|> ORDER BY column1
```

#### Pandas
```python
result = (table1[table1['condition']]
         .merge(table2, on='id', how='inner')
         [['column1', 'column2']]
         .sort_values('column1'))
```

### 4. ウィンドウ関数

#### 従来のSQL
```sql
SELECT 
  column1,
  column2,
  RANK() OVER (PARTITION BY group_col ORDER BY value DESC) as rank
FROM table
WHERE condition
QUALIFY rank <= 5
ORDER BY group_col, rank
```

#### Pipe Syntax
```sql
FROM table
|> WHERE condition
|> EXTEND RANK() OVER (PARTITION BY group_col ORDER BY value DESC) as rank
|> WHERE rank <= 5
|> ORDER BY group_col, rank
```

#### Pandas
```python
result = df[df['condition']].copy()
result['rank'] = (result.groupby('group_col')['value']
                  .rank(method='min', ascending=False)
                  .astype(int))
result = (result[result['rank'] <= 5]
         .sort_values(['group_col', 'rank']))
```

## Pipe Syntaxの主な特徴

1. **処理順序の明確化**
   - データの流れが上から下へ順番に処理される
   - 各ステップが明確に分離されている

2. **演算子の違い**
   - `AGGREGATE` - GROUP BY集計
   - `EXTEND` - 新しい列の追加（計算列、ウィンドウ関数など）
     - 計算列の例: `EXTEND column1 + column2 AS sum_value`
     - ウィンドウ関数の例: `EXTEND RANK() OVER (...) AS rank`
   - `WHERE` - フィルタリング（HAVINGとQUALIFYの役割も兼ねる）

3. **構文の統一性**
   - HAVING句やQUALIFY句の代わりにWHERE句を使用
   - すべての操作がパイプ演算子で統一的に記述

4. **制限事項**
   - SELECT後はテーブルエイリアスが使えない
   - 処理順序を意識した記述が必要

## 処理順序の比較

### 集計処理の例

#### 従来のSQL
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

#### Pipe Syntax
```sql
FROM table
|> WHERE country = 'us'
|> AGGREGATE COUNT(*) as count GROUP BY city
|> WHERE count >= 10
|> ORDER BY count DESC
|> LIMIT 10
```
実行順序: 記述順序と同じ（上から下へ順次実行）

#### Pandas
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

### 学び

Pipe SyntaxとPandasの処理順序はほぼ同じです：
- 両者とも手続き的な記述（記述順序 = 実行順序）
- データの流れが直感的で理解しやすい
- 従来のSQLは宣言的で、記述順序と実行順序が異なる

### 宣言的 vs 手続き的

#### 宣言的な書き方（従来のSQL）
- 英語の文法に近い構造（"SELECT ... FROM ... WHERE ..."）
- 最終的に欲しい結果（SELECT）を最初に明示
- 複雑なクエリでは処理の流れを追いにくい

#### 手続き的な書き方（Pipe Syntax、Pandas）
- レシピや手順書のような順次的な記述
- 処理の流れが上から下へ順番に進む
- デバッグが容易（途中でパイプを切って確認できる）

## 学習ファイル一覧

1. [01_data_selection_filtering.md](01_data_selection_filtering.md) - 基本的なデータ選択とフィルタリング
2. [02_aggregation.md](02_aggregation.md) - 集計処理とGROUP BY
3. [03_joins.md](03_joins.md) - テーブル結合
4. [04_window_functions.md](04_window_functions.md) - ウィンドウ関数

各ファイルには、UFO目撃データを使った具体的な例と、従来のSQL・Pipe Syntax・Pandasでの実装比較が含まれています。