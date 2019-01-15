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
// SELECT * FROM account WHERE joinDate >= 2018 ORDER BY joinDate, age DESC;
if let table = SQuery(at: "user.db").from("account") {
	defer { table.close() } // 自動でDBをclose	
	let cursor: SQLiteCursor = table
		.setWhere("joinDate >= ?", 2018)
		.orderBy("joinDate")
		.orderBy("age", desc: true)
		.select() // 結果を「Cursor」で返す
	defer { cursor.close() }
	// ...
}
```

他にも `gorupBy()`,`limit()`,`distnict()`などが使える。

### whereAnd
where句の場合、ANDで条件を繋ぐ事がよくある。その時に`whereAnd()`を使えば便利。
```swift
// SELECT * FROM account WHERE (joinDate >= 2018) AND (age >= 18);
if let table = SQuery(at: "user.db").from("account") {
	defer { table.close() } // 自動でDBをclose	
	let cursor: SQLiteCursor = table
		.whereAnd("joinDate >= ?", 2018)
		.whereAnd("age >= ?", 18)
		.select()
	defer { cursor.close() }
	// ...
}
```

### SQLiteCursor
#### Cursorオブジェクトからデータを習得する方法
```swift
if let tblAcc = SQuery(at:"user.db").from("account") {
	defer { tblAcc.close() }
	if let cursor = tblAcc
		.setWhere("joinDate >= ", 2018)
		.orderBy("joinDate")
		.orderBy("age", desc: true)
		.columns("id","name","age","joinDate")
		.select()
	{
		defer { cursor.close() }
		while cursor.next() {
			let id = cursor.getString(0)
			let name = cursor.getString(1)
			let age = cursor.getInt(2)

			let joindateRaw = cursor.getString(3)
			let joinDate: Date? = joindateRaw != nil
			? SQuery.newDateTimeFormat.date(from: joindateRaw)
			: nil
			// ...		
		}		
	}
}
```

Cursorが持っているデータをDictionaryで貰うこともできる
```swift
let result: [[String:Any?]] = cursor.toDictionaryAll(closeCursor: true)
```


#### CursorからData Objectを作成
先ずは、Data classに**SQueryRow** protocolを具現する.
```swift
class Account: SQueryRow {
	var id = ""
	var name = ""
	var age = 0
	var joinDate: Date? = nil

	func loadFrom(cursor: SQLiteCursor) {
		cursor.forEachColumn { cur, i in
			let name = cur.getColumnName(i)
			switch name {
			case "id": self.id = cursor.getString(i) ?? ""
			case "name": self.id = cursor.getString(i) ?? ""
			case "age": self.id = cursor.getint(i) ?? 0
			case "joinDate": 
				let joindateRaw = cursor.getString(i)
				self.joinDate = joindateRaw != nil
					? SQuery.newDateTimeFormat.date(from: joindateRaw)
					: nil
			default: break
			}
		}
	}

	func toValues() -> [String:Any?] {
	// ...
	}
}
```

Seelctの結果をDataオブジェクトの配列で貰える
```swift
if let table = SQuery(at: "user.db").from("account") {
	defer { table.close() }
	let rows: [Account] = table
		.setWhere("joinDate >= ?", 2018)
		.orderBy("joinDate")
		.orderBy("age", desc: true)
		.select { Account() } // ここで空のDataオブジェクトを生成する
	// ...
}
```

配列を作成したくない場合、(for each)
```swift
if let table = SQuery(at: "user.db").from("account") {
	defer { table.close() }
	table
		.setWhere("joinDate >= ?", 2018)
		.orderBy("joinDate")
		.orderBy("age", desc: true)
		.select(factory:{ Account() }) { row: Account in
		// ...
		}
	// ...
}
```
