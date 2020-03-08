# squery-swift
Simple SQLite Query Library for Swift (iOS)

まだ開発中です。 Now Developing...

# Install
**Linked Frameworks and Libraries**に「libsqlite3.tbd」を追加

## Swift Package Manager
Go to Project -> Swift Packages and add the repository:
```
https://github.com/Sizuha/squery-swift
```
## 手動
必要なソースは**SQuery.swift**だけです（frameworkを追加したくない場合は**SQuery.swift**だけをコピー）

# DBのOpenとClose
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

Pathを省略すると、基本的にアプリの**Document**から操作を行う。ファイルが存在しない場合は、ファイルを作成する。 これは基本動作で、`SQuery(at: "db_file_path", mode: .readWrite)`のようにmodeを指定して変更できる。

# Create Table
```swift
let db = SQuery(at: "some.db")
defer { db.close() }
let error = db.createTable(TableScheme(name: "TableName", columns: [
	.key("idx", autoInc: true) // PK and AUTO INCREMENT
	.column("title", type: .text, notNull: true),
	.column("date", type: .integer),
	.column("media", type: .integer),
	.column("progress", type: .float),
	.column("total", type: .integer),
	.column("fin", type: .integer),
	.column("rating", type: .float),
	.column("memo", type: .text),
	.column("link", type: .text),
]), ifNotExists: true)
```

# Drop Table
```swift
if let table = SQuery(at: "some.db").from("TableName") {
	defer { table.close() }
	let _ = table.drop()
}
```

# Select
```swift
// SELECT * FROM account WHERE joinDate >= '2018-01-01 00:00:00' ORDER BY joinDate, age DESC;
if let table = SQuery(at: "user.db").from("account") {
	defer { table.close() } // 自動でDBをclose	
	let cursor: SQLiteCursor = table
		.setWhere("joinDate >= ?", "2018-01-01 00:00:00")
		.orderBy("joinDate")
		.orderBy("age", desc: true)
		.select() // 結果を「Cursor」で返す
	defer { cursor.close() }
	// ...
}
```

他にも `gorupBy()`,`limit()`,`distnict()`などが使える。

## andWhere
where句の場合、ANDで条件を繋ぐ事がよくある。その時に`andWhere()`を使えば便利。
```swift
// SELECT * FROM account WHERE (joinDate >= '2018-01-01 00:00:00') AND (age >= 18);
if let table = SQuery(at: "user.db").from("account") {
	defer { table.close() } // 自動でDBをclose	
	let cursor: SQLiteCursor = table
		.andWhere("joinDate >= ?", "2018-01-01 00:00:00")
		.andWhere("age >= ?", 18)
		.select()
	defer { cursor.close() }
	// ...
}
```

## SQLiteCursor
### Cursorオブジェクトからデータを習得する方法
```swift
if let tblAcc = SQuery(at:"user.db").from("account") {
	defer { tblAcc.close() }
	if let cursor = tblAcc
		.setWhere("joinDate >= ", "2018-01-01 00:00:00")
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

Cursorが持っているデータをDictionaryで貰うこともできる。
```swift
let result: [[String:Any?]] = cursor.toDictionaryAll(closeCursor: true)
```


### CursorからData Objectを作成
先ずは、Data classに**SQueryRow** protocolを具現する。
```swift
class Account: SQueryRow {
	static let tableName = "account"
	static let F_ID = "id"
	static let F_NAME = "name"
	static let F_AGE = "age"
	static let F_JOIN = "joinDate"

	var id = ""
	var name = ""
	var age = 0
	var joinDate: Date? = nil	
	private val dateFmt = SQuery.newDateTimeFormat

	func load(from cursor: SQLiteCursor) {
		cursor.forEachColumn { cur, i in
			let name = cur.getColumnName(i)
			switch name {
			case Account.F_ID: self.id = cur.getString(i) ?? ""
			case Account.F_NAME: self.name = cur.getString(i) ?? ""
			case Account.F_AGE: self.age = cur.getint(i) ?? 0
			case Account.F_JOIN: 
				let joindateRaw = cur.getString(i)
				self.joinDate = joindateRaw != nil
					? dateFmt.date(from: joindateRaw)
					: nil
			default: break
			}
		}
	}

	func toValues() -> [String:Any?] {
		return [
			Account.F_ID: id,
			Account.F_NAME: name,
			Account.F_AGE: age,
			Account.F_JOIN: joinDate != nil ? dateFmt.string(from: joinDate) : sqlNil
		]
	}
}
```

Seelctの結果をDataオブジェクトの配列で貰える。
```swift
if let table = SQuery(at: "user.db").from(Account.tableName) {
	defer { table.close() }
	let rows: [Account] = table
		.setWhere("\(Account.F_JOIN) >= ?", "2018-01-01 00:00:00")
		.orderBy(Account.F_JOIN)
		.orderBy(Account.F_AGE, desc: true)
		.select { Account() /* ここで空のDataオブジェクトを生成する */ }.0
	// ...
}
```

配列を作成したくない場合、(for each)
```swift
if let table = SQuery(at: "user.db").from(Account.tableName) {
	defer { table.close() }
	let _ = table
		.setWhere("\(Account.F_JOIN) >= ?", "2018-01-01 00:00:00")
		.orderBy(Account.F_JOIN)
		.orderBy(Account.F_AGE, desc: true)
		.select(factory:{ Account() }) { row: Account in
		// ...
		}
	// ...
}
```

結果のrowが１個か０の場合
```swift
if let table = SQuery(at: "user.db").from(Account.tableName) {
	defer { table.close() }
	if let row = table.setWhere("\(Account.F_ID)=?", "xxx").selectOne{ Account() }.0 {
		// 結果あり
	}
}
```

#### SQueryRowEx
SQueryRowEx Protocolを使うと、Tableの定義も一緒にData Classの中でできる。
```swift
class Account: SQueryRowEx {
	static let tableScheme = TableScheme(name: "account", columns: [
		.key(F_ID, type: .text, notNull: true),
		.column(F_NAME, type: .text, notNull: true),
		.column(F_AGE, type: .integer, notNull: true),
		.column(F_JOIN, type: .text, notNull: true),
	])

	static let F_ID = "id"
	static let F_NAME = "name"
	static let F_AGE = "age"
	static let F_JOIN = "joinDate"

	var id = ""
	var name = ""
	var age = 0
	var joinDate: Date? = nil	
	private val dateFmt = SQuery.newDateTimeFormat

	func load(from cursor: SQLiteCursor) {
		// 省略
	}

	func toValues() -> [String:Any?] {
		// 省略
	}
}


// それと、Table名の代わりに、Class名.classを使うことができる

// Create Table
if let db = SQuery(at: db) {
	defer { db.close() }
	db.create(tables: [Account.class])
}

// Select Table (FROM)
guard let table = SQuery(at: db)?.from(Account.class) else { fatalError() }
```

# Insert
```swift
if let table = SQuery(at: "user.db").from(Account.tableName) {
	defer { table.close() }
	let item = Account()
	item.id = "xxx"
	// ...
	let _ = table.insert(values: item)
}
```

## Auto Incrementのcolumnの例外処理
Auto Incrementで宣言されたcolumnはINSERTで直接データをセットできない。この場合、下記の様に除外するcolumnを指定できる。
```swift
if let table = SQuery(at: "some.db").from("TableName") {
	defer { table.close() }
	let item = [String:Any?]()
	// ...ここでデータの中身を入れる...
	
	// idxがAuto Incrementの場合
	if table.insert(values: item, except:["idx"]).isSuccess {
		// 成功
	} else {
		// 失敗
	}
}
```

# Update
```swift
// UPDATE account WHERE id='xxx' SET name='TESTER', ...;
if let table = SQuery(at: "user.db").from(Account.tableName) {
	defer { table.close() }
	
	let item = Account()
	item.id = "xxx"
	item.name = "TESTER"
	item.age = 20
	item.joinDate = Date()
	let rowCount = table
		// 変更してはいけない「主キー(PK)」のcolumn名をここで指定しておく
		.keys(Account.F_ID)
		// keys()でPKを指定しておくと、WHERE句も自動で作成される
		.update(set: item)
		.rowCount
}
```

```swift
// UPDATE account WHERE id='xxx' SET name='TESTER';
if let table = SQuery(at: "user.db").from(Account.tableName) {
	defer { table.close() }
	let rowCount = table
		.setWhere("\(Account.F_ID)=?", "xxx")
		.update(set: ["name": "TESTER"])
		.rowCount
}
```

# Insert or Update
先にINSERTを試して失敗する場合、UPDATEを実行する。
つまり新規のデータを登録する時、既に存在する場合は入れ替える。
```swift
if let table = SQuery(at: "user.db").from(Account.tableName) {
	defer { table.close() }
	
	let item = Account()
	item.id = "xxx"
	item.name = "TESTER"
	item.age = 20
	item.joinDate = Date()
	let _ = table
		.keys(Account.F_ID)
		.values(item)
		.insertOrUpdate()
}
```

# Delete
```swift
if let table = SQuery(at: "user.db").from(Account.tableName) {
	defer { table.close() }
	// DELETE FROM account WHERE id='xxx';
	let _ = table.setWhere("\(Account.F_ID)=?","xxx").delete()
}
```

# nilの扱いに注意！！
SwiftのDictionaryは、「nil」値が収納できない！
```swift
var sample = [String:Any?]()
sample["comment"] = nil

// この場合、sampleの中に「comment」自体が存在しない。つまり、「キー」にnilを投入するのは「キー」を削除することを意味する。
```

なので、DBに「nil」を収納したい場合は「nil」の代わりに「sqlNil」を使う！
```swift
import SQuery

var sample = [String:Any?]()
sample["comment"] = sqlNil

// nilを確認
let isNil = sample["comment"] is SqlNil
```



