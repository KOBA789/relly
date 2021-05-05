# relly

![](docs/header.png)

**:warning: これは簡単なクエリ言語を実装したブランチです :warning:**

「WEB+DB PRESS Vol. 122『特集3 作って学ぶ RDBMS のしくみ』」で取り扱っているコードについては [wdb ブランチ](https://github.com/KOBA789/relly/tree/wdb) を参照してください。

## relly とは

relly は RDBMS のしくみを学ぶための小さな RDBMS 実装です。

## 動かし方

テーブルヒープファイルをコマンドライン引数に付けて `cargo run` すると対話的なクエリインターフェースが起動します。

```
$ cargo run table.rly
    Finished dev [unoptimized + debuginfo] target(s) in 0.01s
     Running `target/debug/relly table.rly`
>
```

## クエリ言語

実装は `src/lang.rs` にあります。

### テーブルを作る `CreateTable`

`num_key_elems` プロパティは `table::Table` 構造体の `num_key_elems` フィールドと同じ意味です。

実行すると、テーブルのメタページの ID が返ります。

入力例:
```json
{
  "CreateTable": {
    "num_key_elems": 1
  }
}
```

出力例:
```
table_page_id = 0
```

### 行を挿入する `Insert`

`table` プロパティはテーブルのメタページの ID を指定します。

`num_key_elems` プロパティは `table::Table` 構造体の `num_key_elems` フィールドと同じ意味です。

`record` プロパティは挿入する行の内容です。カラムの値は文字列で表し、行はその配列です。

入力例:
```json
{
  "Insert": {
    "table": 0,
    "num_key_elems": 1,
    "record": ["z", "Alice", "Smith"]
  }
}
```

### テーブルを検索する `Query`

詳細は `src/lang.rs` の `PlanNode` 列挙体を参照してください。

入力例:
```json
{ "Query":
  { "Filter":
    { "from": { "SeqScan": { "table": 0 } },
      "where":
        { "Or": [
          { "Eq": [
            { "Column": 1 },
            { "Literal": "Charlie" } ] },
          { "Eq": [
            { "Column": 1 },
            { "Literal": "Alice" } ] } ] } } } }
```

出力例:
```
Tuple("y" [79], "Charlie" [43, 68, 61, 72, 6c, 69, 65], "Williams" [57, 69, 6c, 6c, 69, 61, 6d, 73])
Tuple("z" [7a], "Alice" [41, 6c, 69, 63, 65], "Smith" [53, 6d, 69, 74, 68])
```
