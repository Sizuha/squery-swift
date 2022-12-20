# squery-swift
Simple SQLite Query Library for Swift (iOS)

# Install

## Swift Package Manager
Go to Project -> Swift Packages and add the repository:
```
https://github.com/Sizuha/squery-swift
```
## 手動
必要なソースは**SQuery.swift**のみです。**SQuery.swift**だけをコピー。

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

又は

```swift
let db = SQuery(at: "some.db")
defer { db.close() }
let error = db.createTable(TableScheme(name: "TableName", columns: [
    .def("idx", type: .integer, [.autoInc]) // PK and AUTO INCREMENT
    .def("title", type: .text, [.notNull]),
    .def("date", type: .integer),
    .def("media", type: .integer),
    .def("progress", type: .float),
    .def("total", type: .integer),
    .def("fin", type: .integer),
    .def("rating", type: .float),
    .def("memo", type: .text),
    .def("link", type: .text),
]), ifNotExists: true)
```

# Drop Table
```swift
guard let table = SQuery(at: "some.db").from("TableName") else { return }
defer { table.close() }
let _ = table.drop()
```

# Select
```swift
// SELECT * FROM account WHERE joinDate >= '2018-01-01 00:00:00' ORDER BY joinDate, age DESC;
guard let table = SQuery(at: "user.db").from("account") else { return }
defer { table.close() } // 自動でDBをclose	
let cursor: SQLiteCursor = table
    .where("joinDate >= ?", "2018-01-01 00:00:00")
    .orderBy("joinDate")
    .orderBy("age", desc: true)
    .select() // 結果を「Cursor」で返す
defer { cursor.close() }
// ...
```

他にも `gorupBy()`,`limit()`,`distnict()`などが使える。

## andWhere
where句の場合、ANDで条件を繋ぐ事がよくある。その時に`andWhere()`を使えば便利。
```swift
// SELECT * FROM account WHERE (joinDate >= '2018-01-01 00:00:00') AND (age >= 18);
guard let table = SQuery(at: "user.db").from("account") else { return }
defer { table.close() } // 自動でDBをclose	
let cursor = table
    // 最初のandWhereはwhereと同じ意味になる        
    .andWhere("joinDate >= ?", "2018-01-01 00:00:00")
    .andWhere("age >= ?", 18)
    .select()
defer { cursor.close() }
// ...
```

## SQLiteCursor
### Cursorオブジェクトからデータを習得する方法
```swift
guard let tblAcc = SQuery(at:"user.db").from("account") else { return }
defer { tblAcc.close() }

let cursor = tblAcc
    .where("joinDate >= ", "2018-01-01 00:00:00")
    .orderBy("joinDate")
    .orderBy("age", desc: true)
    .columns("id","name","age","joinDate")
    .select()

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
		[
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
guard let table = SQuery(at: "user.db").from(Account.tableName) else { return }
defer { table.close() }
let rows: [Account] = table
    .where("\(Account.F_JOIN) >= ?", "2018-01-01 00:00:00")
    .orderBy(Account.F_JOIN)
    .orderBy(Account.F_AGE, desc: true)
    .select(as: Account() /* 空のDataオブジェクトを生成するコード */).rows
// ...
```

配列を作成したくない場合、(for each)
```swift
guard let table = SQuery(at: "user.db").from(Account.tableName) else { return }
defer { table.close() }
let _ = table
    .where("\(Account.F_JOIN) >= ?", "2018-01-01 00:00:00")
    .orderBy(Account.F_JOIN)
    .orderBy(Account.F_AGE, desc: true)
    .select(as: Account()) { row: Account in
    // ...
    }
// ...
```

結果のrowが「１個」か「無し」の場合
```swift
guard let table = SQuery(at: "user.db").from(Account.tableName) else { return }
defer { table.close() }
if let row = table
    .where("\(Account.F_ID)=?", "xxx")
    .selectOne(as: Account()).row 
{
    // 結果あり
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
	db.create(tables: [Account.self])
}

// Select Table (FROM)
guard let table = SQuery(at: db)?.from(Account.self) else { fatalError() }
```

# Insert
```swift
guard let table = SQuery(at: "user.db").from(Account.tableName) else { return }
defer { table.close() }
let item = Account()
item.id = "xxx"
// ...
let _ = table.insert(values: item)
```

## Auto Incrementのcolumnの例外処理
Auto Incrementで宣言されたcolumnはINSERTで直接データをセットできない。この場合、下記の様に除外するcolumnを指定できる。
```swift
guard let table = SQuery(at: "some.db").from("TableName") else { return }
defer { table.close() }
let item = [String:Any?]()
// ...ここでデータの中身を入れる...

// idxがAuto Incrementの場合
if table.insert(values: item, except:["idx"]).isSuccess {
    // 成功
} else {
    // 失敗
}
```

# Update
```swift
// UPDATE account WHERE id='xxx' SET name='TESTER', ...;
guard let table = SQuery(at: "user.db").from(Account.tableName) else { return }
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
```

```swift
// UPDATE account WHERE id='xxx' SET name='TESTER';
guard let table = SQuery(at: "user.db").from(Account.tableName) else { return }
defer { table.close() }
let rowCount = table
    .where("\(Account.F_ID)=?", "xxx")
    .update(set: ["name": "TESTER"])
    .rowCount
```

# Insert or Update
先にINSERTを試して失敗する場合、UPDATEを実行する。
つまり新規のデータを登録する時、既に存在する場合は入れ替える。
```swift
guard let table = SQuery(at: "user.db").from(Account.tableName) else { return }
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
```

# Delete
```swift
guard let table = SQuery(at: "user.db").from(Account.tableName) else { return }
defer { table.close() }
// DELETE FROM account WHERE id='xxx';
let _ = table.where("\(Account.F_ID)=?","xxx").delete()
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
