# squery-swift
Simple SQLite Query Library for Swift (iOS)

まだ開発中です。 Now Developing...

## 準備
1. **Linked Frameworks and Libraries**に「libsqlite3.tbd」を追加します
1. 必要なソースは**SQuery.swift**だけです

## DBのOpenとClose
```swift
// Open
let dbConn = SQuery(at: "db_file_path").open()
//又は
let dbUrl = FileManager.default
	.urls(for: .documentDirectory, in: .userDomainMask).first!
	.appendPathComponent("dbfile.db")
let dbConn = SQuery(url: dbUrl).open()

// Close
defer { dbConn?.close() }
```

Pathを省略すると、基本的にアプリの**Document**から操作を行う。ファイルが存在しない場合は、ファイルを作成する。 これは基本動作で、`SQuery("db_file_path", mode: .readWrite)`のようにmodeを指定して変更できる。

## Create Table
```swift
if let table = SQuery(at: "some.db").talbeCreator(name: "TableName") {
	defer { table.close() } // 自動でDBをclose
	table
		.addAutoInc("idx") // PK and AUTO INCREMENT
		.addColumn("title", type: .text, notNull: true)
		.addColumn("date", type: .integer)
		.addColumn("media", type: .integer)
		.addColumn("progress", type: .float)
		.addColumn("total", type: .integer)
		.addColumn("fin", type: .integer)
		.addColumn("rating", type: .float)
		.addColumn("memo", type: .text)
		.addColumn("link", type: .text)
		.create(ifNotExists: true)
}
```
他に、`addPrimaryKey()`でPrimary Key(主キー)を指定できる

## Select
```swift
// SELECT * FROM anime WHERE fin=0 ORDER BY date DESC, title ASC;
if let table = SQuery(at: "some.db").from("anime") {
	defer { table.close() } // 自動でDBをclose	
	let cursor: SQLiteCursor = table
		.setWhere("fin=?", false)
		.orderBy("date", desc: true)
		.orderBy("title")
		.select() // 結果を「Cursor」で返す
	defer { cursor.close() }
	// ...
}
```
