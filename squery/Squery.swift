//
//  Squery.swift
//  Simple SQLite Query Library for Swift
//
//	require library: libsqlite3.tbd
//

import Foundation
import SQLite3

protocol SQueryRow {
	func loadFrom(cursor: SQLiteCursor)
	func toValues() -> Dictionary<String,Any>
}

enum SQLiteOpenMode {
	case readWriteCreate // rwc
	case readWrite // rw
	case readonly // ro
	case memory // memory
}

enum SQueryJoin {
	case inner
	case leftOuter
	case cross
}

private var enableDebugMode = true
public func setEnableSQueryDebug(_ flag: Bool = true) {
	enableDebugMode = false
}

private func printLog(_ text: String, _ args: CVarArg...) {
	if enableDebugMode {
		NSLog("[SQuery] \(text)", args)
	}
}

/**
SQLiteのクエリの結果(row達)を探索する

使い方
---

rowを探索する
```
if let conn = SQuery("sample.db").open() {
	let cursor = conn.query("SELECT * FROM user")
	defer {
		// Cursorの仕様が終わったら必ずcloseする!
		cursor.close()
		conn.close()
	}
	while cursor.next() {
		// ...
	}
}
```

rowからデータを習得
```

```
*/
class SQLiteCursor {
	private var stmt: OpaquePointer? = nil
	
	private var columnCountRaw: Int32 = 0
	var columnCount: Int {
		get {
			return Int(columnCountRaw)
		}
	}
	
	private var columnNameMap = Dictionary<String,Int>()

	/// Cursorオブジェクトを作成する。
	/// 直接オブジェクトを作成する事は無く、SQLiteConnectionクラスから実行されたクエリの結果として作られる。
	///
	/// - Parameter stmt:
	///   `sqlite3_prepare_v2()`もしくはSQLiteConnectionクラスの`prepare()`の戻り値
	required init(_ stmt: OpaquePointer) {
		self.stmt = stmt
		columnCountRaw = sqlite3_column_count(stmt)
		
		for i in 0..<columnCountRaw {
			let colName = getColumnNameRaw(i)
			columnNameMap[colName] = Int(i)
		}
	}
	
	/// 初期状態に戻る。
	/// rowをまた習得するには `next()` をコール。
	func reset() {
		sqlite3_reset(stmt)
	}
	
	/// 次のrowを習得する
	/// - Returns: rowがあったら**true**
	func next() -> Bool {
		let res = sqlite3_step(stmt)
		switch res {
		case SQLITE_ROW:
			return true
		case SQLITE_DONE:
			return false
		default:
			return false
		}
	}
	
	/// Coursorの作業を完全に終了する。
	/// 仕様が終わったCursorは必ず`close()`する事。
	func close() {
		if let stmt = stmt {
			sqlite3_finalize(stmt)
		}
		stmt = nil
	}
	
	/// column名で、Cursor内のindexを習得
	///
	/// - Parameters:
	///   - name: column名
	/// - Returns:
	///   1) column名が存在する場合: columnのindex
	///   2) 存在しない場合: nil
	func getColumnIndex(name: String) -> Int? {
		return columnNameMap[name]
	}
	
	private func getColumnNameRaw(_ col: Int32) -> String {
		return String(cString: sqlite3_column_name(stmt, col))
	}
	
	/// columnのindexでcolumnの名前を習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:
	///   indexが存在する場合**column名**、存在しない場合**nil**を返す
	func getColumnName(_ col: Int) -> String? {
		for (colName, colIdx) in columnNameMap {
			if colIdx == col {
				return colName
			}
		}
		return nil
	}
	
	/// 各column毎に処理を行う。
	///
	/// - Parameter each: 各column毎で呼ばれるClosure。
	/// - Parameter cursor: Coursorオブジェクト（自身）
	/// - Parameter index: 現在のcolumnのindex
	func forEachColumn(_ each: (_ cursor: SQLiteCursor, _ index: Int)->Void) {
		for i in 0..<columnCount {
			each(self, i)
		}
	}
	
	//--- get Datas ---

	/// columnのデータが**nil**か確認
	///
	/// - Parameter col: columnのindex
	/// - Returns: **nil**の場合**true**
	func isNull(_ col: Int) -> Bool {
		let dataType = sqlite3_column_type(stmt, Int32(col))
		return dataType == SQLITE_NULL
	}
	
	/// columnから32bitのInt型データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:
	///   1) データが**NULL**の場合: nil
	///   2) それ以外: Int型の値
	func getInt(_ col: Int) -> Int? {
		return isNull(col)
			? nil
			: Int(sqlite3_column_int(stmt, Int32(col)))
	}
	
	/// columnからInt64型データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:
	///   1) データが**NULL**の場合: nil
	///   2) それ以外: Int64型の値
	func getInt64(_ col: Int) -> Int64? {
		return isNull(col)
			? nil
			: Int64(sqlite3_column_int64(stmt, Int32(col)))
	}

	func getString(_ col: Int) -> String? {
		return isNull(col)
			? nil
			: String(cString: sqlite3_column_text(stmt, Int32(col)))
	}
	
	func getDouble(_ col: Int) -> Double? {
		return isNull(col)
			? nil
			: sqlite3_column_double(stmt, Int32(col))
	}
	
	func getFloat(_ col: Int) -> Float? {
		if let value = getDouble(col) {
			return Float(value)
		}
		return nil
	}
	
	func getBool(_ col: Int) -> Bool? {
		return isNull(col)
			? nil
			: getInt(col) != 0
	}
	
	func getBlob(_ col: Int) -> [UInt8]? {
		guard !isNull(col) else { return nil }
		if let data = sqlite3_column_blob(stmt, Int32(col)) {
			return data.load(as: [UInt8].self)
		}
		return nil
	}
	
	func getBlobRaw(_ col: Int) -> UnsafeRawPointer {
		return sqlite3_column_blob(stmt, Int32(col))
	}
	
}

/**
SQLite DBを操作するクラス。

DBファイルを開いて、クエリを実行できる。
*/
class SQLiteConnection {
	private var db: OpaquePointer? = nil
	
	required init(_ db: OpaquePointer) {
		self.db = db
	}
	
	func close() {
		if let db = db {
			sqlite3_close(db)
		}
		db = nil
	}
	
	var isClosed: Bool {
		get { return db == nil }
	}
	
	func getLastError() -> String? {
		if let db = db {
			let errMsgRaw = sqlite3_errmsg(db)
			return String(cString: errMsgRaw!)
		}
		
		return nil
	}
	
	func getLastChangedRowCount() -> Int {
		if let db = db {
			return Int(sqlite3_changes(db))
		}
		return 0
	}
	
	private func prepare(sql: String, _ args: Any?...) -> OpaquePointer? {
		printLog("prepare sql: \(sql)")
		
		var stmt: OpaquePointer? = nil
		if sqlite3_prepare_v2(db, sql, Int32(sql.utf8.count), &stmt, nil) == SQLITE_OK {
			bindAll(stmt, args)
		}
		return stmt
	}
	private func prepare(sql: String, args: [Any?]) -> OpaquePointer? {
		printLog("prepare sql: \(sql)")
		
		var stmt: OpaquePointer? = nil
		if sqlite3_prepare_v2(db, sql, Int32(sql.utf8.count), &stmt, nil) == SQLITE_OK {
			bindAll(stmt, args: args)
		}
		return stmt
	}

	private func bindAll(_ stmt: OpaquePointer?, _ args: Any?...) {
		guard let stmt = stmt else {
			return
		}
		
		var idx: Int32 = 0
		for arg in args {
			idx = bindSingle(stmt, idx, arg)
		}
	}
	private func bindAll(_ stmt: OpaquePointer?, args: [Any?]) {
		guard let stmt = stmt else {
			return
		}
		
		var idx: Int32 = 0
		for arg in args {
			idx = bindSingle(stmt, idx, arg)
		}
	}

	private func bindSingle(_ stmt: OpaquePointer?, _ idx: Int32, _ arg: Any?) -> Int32 {
		switch arg {
		case nil:
			sqlite3_bind_null(stmt, idx)
			break
			
		case is Int8, is Int16, is Int32:
			sqlite3_bind_int(stmt, idx, arg as! Int32)
			break
			
		case is Int, is Int64:
			sqlite3_bind_int64(stmt, idx, arg as! Int64)
			break
			
		case is [UInt8]:
			let data = arg as! [UInt8]
			sqlite3_bind_blob(stmt, idx, data, Int32(data.count), nil)
			break
			
		case is Date:
			let data = arg as! Date
			let timestamp = SQuery.toTimestamp(data)
			sqlite3_bind_int64(stmt, idx, timestamp)
			break
			
		default:
			let data = arg as! String
			let length = Int32(data.lengthOfBytes(using: String.Encoding.utf8))
			sqlite3_bind_text(stmt, idx, data, length, nil)
			break
		}
		
		return idx+1
	}
	
	func query(sql: String, _ args: Any?...) -> SQLiteCursor {
		let stmt = prepare(sql: sql, args)
		return SQLiteCursor(stmt!)
	}
	func query(sql: String, args: [Any?]) -> SQLiteCursor {
		let stmt = prepare(sql: sql, args: args)
		return SQLiteCursor(stmt!)
	}
	
	private func executeScalar(_ stmt: OpaquePointer?) -> Int? {
		guard let stmt = stmt else {
			return nil
		}
		
		let curosr = SQLiteCursor(stmt)
		defer {
			curosr.close()
		}
		if curosr.next() {
			if let value = curosr.getInt(0) {
				return value
			}
		}
		
		return nil
	}

	func executeScalar(sql: String, _ args: Any?...) -> Int? {
		let stmt = prepare(sql: sql, args)
		return executeScalar(stmt)
	}
	func executeScalar(sql: String, args: [Any?]) -> Int? {
		let stmt = prepare(sql: sql, args: args)
		return executeScalar(stmt)
	}
	
	private func excute(_ stmt: OpaquePointer) -> Bool {
		let res = sqlite3_step(stmt)
		defer {
			sqlite3_finalize(stmt)
		}
		
		switch res {
		case SQLITE_OK, SQLITE_ROW, SQLITE_DONE:
			return true
		default:
			return false
		}
	}
	
	func execute(sql: String, _ args: Any?...) -> Bool {
		let stmt = prepare(sql: sql, args)
		return excute(stmt!)
	}
	func execute(sql: String, args: [Any?]) -> Bool {
		let stmt = prepare(sql: sql, args: args)
		return excute(stmt!)
	}
}

/**
SQLite DBをべ便利に扱う為のライブラリ

使い方
---
Open & Close
```
let dbConn = SQuery("db_file_path").open()
dbConn?.close()
```

Tableを指定してクエリを作成する
```
let db = SQuery("db_file_path")
// from() メソッドは自動でDBをopenする
let cursor = db.from("Table名")?.select()

// 使用後
cursor?.close()
db.close()
```

参照
---
TableQuery class
*/
class SQuery {
	private var dataSource: String
	private var dbConn: SQLiteConnection? = nil


	/// SQLite DBファイルをOpen又は作成する
	///
	/// - Parameters:
	///   - dbfile: 対象のDBファイル
	///     1) URI形式(`file:`で始まる): file:/path/filename.db
	///     2) パスとファイル名: 自動でURI形式に変換する。URI Encodeも適用。パスを省略すると、アプリのDocumentのパスを使う。
	///   - mode:
	///	    - .readWriteCreate (default) = 読み書きができる・ファイルが存在しない場合作成する
	///	    - .readWrite = 読み書きができる
	///     - .readonly = 修正不可
	///     - .memory = メモリーDB
	required init(_ dbfile: String, mode: SQLiteOpenMode = .readWriteCreate) {
		if !dbfile.starts(with: "file:") {
			dataSource = "file:"
			
			let path = dbfile.starts(with: "/")
				? dbfile
				: NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)[0]
			
			if let encoded =
				path.addingPercentEncoding(withAllowedCharacters: NSCharacterSet.urlQueryAllowed)
			{
				dataSource.append(encoded)
			}
			else {
				dataSource.append(path)
			}
		}
		else {
			dataSource = dbfile
		}
		
		dataSource.append("?mode=")
		switch mode {
		case .readonly:
			dataSource.append("ro")
		case .readWrite:
			dataSource.append("rw")
		case .memory:
			dataSource.append("memory")
		default:
			dataSource.append("rwc")
		}
	}
	
	/// DBファイルを開く
	///
	/// このメソッドで直接DBファイルを開くよりは、`from()`メソッドを使うことをおすすめする。
	/// - Returns:
	///   1) DBファイルを開いて、SQLiteConnection オブジェクトとして返す
	///   2) 以前、開いたものがあったら、それを返す
	///   3) 失敗したら、nil
	func open() -> SQLiteConnection? {
		if dbConn == nil || dbConn?.isClosed == true {
			var db: OpaquePointer?
			let res = sqlite3_open(dataSource, &db)
			dbConn = SQLiteConnection(db!)
			
			if res != SQLITE_OK {
				printLog("Can't open DB -> %@", dbConn!.getLastError()!)
				close()
				return nil;
			}
		}
		return dbConn
	}
	
	/// DBファイルを閉じる
	func close() {
		dbConn?.close()
		dbConn = nil
	}
	
	/// Tableを指定する
	///
	/// 以後、指定したTableに対してクエリを実行する事になる
	///
	/// 参照
	/// ---
	/// TableQuery class
	///
	/// - Parameter table: Table名
	/// - Returns: クエリを作成できる**TableQuery**オブジェクト
	func from(table: String) -> TableQuery? {
		if let db = open() {
			return TableQuery(db, table: table)
		}
		return nil
	}
	
	
	//--- Utils ---
	
	static var utcTimeZone: TimeZone {
		get {
			return TimeZone(abbreviation: "UTC")!
		}
	}
	
	static var dateTimeFormat: DateFormatter {
		get {
			let fmt = DateFormatter()
			fmt.locale = Locale.current
			fmt.dateFormat = "yyyy-MM-dd HH:mm:ss"
			fmt.timeZone = utcTimeZone
			return fmt
		}
	}
	static var dateFormat: DateFormatter {
		get {
			let fmt = DateFormatter()
			fmt.locale = Locale.current
			fmt.dateFormat = "yyyy-MM-dd"
			fmt.timeZone = utcTimeZone
			return fmt
		}
	}
	static var timeFormat: DateFormatter {
		get {
			let fmt = DateFormatter()
			fmt.locale = Locale.current
			fmt.dateFormat = "HH:mm:ss"
			fmt.timeZone = utcTimeZone
			return fmt
		}
	}

	static func toTimestamp(_ datetime: Date) -> Int64 {
		return Int64((datetime.timeIntervalSince1970 * 1000.0).rounded())
	}
	
	static func toDate(timestamp: Int64) -> Date {
		return Date(timeIntervalSince1970: TimeInterval(timestamp))
	}
}

/**
SQLite DBの一つのTableに対して、クエリ分を作成し、実行する
*/
class TableQuery {
	private let db: SQLiteConnection
	private let tableName: String
	
	private var sqlDistnict = false
	
	private var sqlJoin = ""
	private var sqlJoinTables = [String]()
	private var sqlJoinOn = ""
	private var sqlJoinOnArgs = [Any?]()
	
	private var sqlWhere = String()
	private var sqlWhereArgs = [Any?]()
	
	private var sqlOrderBy = ""
	
	private var sqlGroupByCols = [String]()
	private var sqlHaving = ""
	private var sqlHavingArgs = [Any?]()

	private var sqlLimitCount = 0
	private var sqlLimitOffset = 0
	
	private var sqlColumns = [String]()
	private var sqlKeyColumns = [String]()
	
	private var sqlValues = Dictionary<String,Any>()
	
	/// DBからTableを指定て、instanceを作る
	///
	/// Examples
	/// ---
	/// ```
	/// if let dbConn = SQuery("some.db").open() {
	/// 	let table = TableQuery(dbConn, "tableName")
	///		// ...
	/// }
	/// ```
	/// SQueryクラスの`from()`メソッドをおすすめ
	/// ```
	/// let table = SQuery("some.db").from("tableName")
	/// ```
	///
	/// - Parameters:
	///   - db: 開いたDBオブジェクト
	///   - table: Table名
	required init(_ db: SQLiteConnection, table: String) {
		self.db = db
		tableName = table
	}
	
	/// DBを閉じる
	///
	/// SQueryクラスの`close()`やSQLiteConnectionクラスの`close()`と同じ機能
	func close() {
		db.close()
	}
	
	/// クエリの設定を初期化する
	///
	/// ただし、`keys()`の設定は残る。
	/// クエリを一度実行してから、また別の設定でクエリを実行すためには一度`reset()`する事をおすすめする。
	/// - Returns: 自分のinstance
	func reset() -> TableQuery {
		sqlDistnict = false
		
		sqlJoin = ""
		sqlJoinTables.removeAll()
		sqlJoinOn = ""
		sqlJoinOnArgs.removeAll()
		
		sqlWhere = ""
		sqlWhereArgs.removeAll()
		
		sqlGroupByCols.removeAll()
		sqlHaving = ""
		sqlHavingArgs.removeAll()
		
		sqlOrderBy = ""
		sqlLimitCount = 0
		sqlLimitOffset = 0
		
		sqlColumns.removeAll()
		//sqlKeyColumns.removeAll()
		sqlValues.removeAll()
		return self
	}
	
	func distnict(_ flag: Bool = true) -> TableQuery {
		sqlDistnict = flag
		return self
	}
	
	func join(type joinType: SQueryJoin, tables: [String], on joinOn: String, _ args: Any?...) -> TableQuery {
		return join(type: joinType, tables: tables, on: joinOn, args: args)
	}
	func join(type joinType: SQueryJoin, tables: [String], on joinOn: String, args: [Any?]) -> TableQuery {
		switch joinType {
		case .cross:
			sqlJoin = "CROSS JOIN"
			break;
		case .leftOuter:
			sqlJoin = "LEFT OUTER JOIN"
		default:
			sqlJoin = "INNER JOIN"
			break;
		}

		sqlJoinTables = tables
		sqlJoinOn = joinOn
		sqlJoinOnArgs = args
		
		return self
	}

	/// 参照
	/// ---
	/// `func setWhere(_ whereText: String, args: [Any?]) -> TableQuery`
	func setWhere(_ whereText: String, _ args: Any?...) -> TableQuery {
		return setWhere(whereText, args: args)
	}
	/// WHERE句を作成する
	///
	/// SQLiteの「?」パラメーターに対応
	/// ```
	/// // SELECT count(*) FROM account WHERE id=\(id) AND pass=\(pwd)
	/// let cursor = SQuery("user.db").from("account")?
	/// 	.setWhere("id=? AND pass=?", id, pwd)
	/// 	.select()
	/// ```
	///
	/// - Parameters:
	///   - whereText: WHERE句に入る条件
	///   - args: 条件の中の「?」に対応するパラメータ達
	/// - Returns: 自分のinstance
	func setWhere(_ whereText: String, args: [Any?]) -> TableQuery {
		sqlWhereArgs.removeAll()
		
		sqlWhere = "(\(whereText))"
		for arg in args {
			sqlWhereArgs.append(arg)
		}
		
		return self
	}
	
	/// 参照
	/// ---
	/// `func whereAnd(_ whereText: String, args: [Any?]) -> TableQuery`
	func whereAnd(_ whereText: String, _ args: Any?...) -> TableQuery {
		return whereAnd(whereText, args: args)
	}
	/// `setWhere()`と同じだが、現在のWHERE句にAND条件で追加する
	/// ```
	/// // SELECT count(*) FROM account WHERE (id=\(id)) AND (pass=\(pwd))
	/// let loginOk = SQuery("user.db").from("account")?
	/// 	.setWhere("id=?", id)
	/// 	.whereAnd("pass=?", pwd)
	/// 	.count() == 1
	/// ```
	/// `setWhere()`を使わずに`whereAnd()`だけでWHERE句を作成することもできる
	/// ```
	/// let loginOk = SQuery("user.db").from("account")?
	/// 	.whereAnd("id=?", id)
	/// 	.whereAnd("pass=?", pwd)
	/// 	.count() == 1
	/// ```
	///
	/// 参照
	/// ---
	/// `func setWhere(_ whereText: String, args: [Any?]) -> TableQuery`
	///
	/// - Parameters:
	///   - whereText: 追加する条件
	///   - args: 条件の中の「?」に対応するパラメータ達
	/// - Returns: 自分のinstance
	func whereAnd(_ whereText: String, args: [Any?]) -> TableQuery {
		if sqlWhere.isEmpty {
			sqlWhere = "(\(whereText))"
		}
		else {
			sqlWhere.append(" AND (\(whereText))")
		}
		
		for arg in args {
			sqlWhereArgs.append(arg)
		}
		
		return self
	}
	
	/// ORDER BY句に並べ条件を追加する
	///
	/// ```
	/// // SELECT * from account ORDER BY joinDate DESC, name ASC
	/// let cursor = SQuery("user.db").from("account")?
	/// 	.orderBy("joinDate", false)
	/// 	.orderBy("name")
	/// 	.select()
	///
	/// ```
	/// - Parameters:
	///   - field: ソートするcolumn名
	///   - asc:
	///     1) true = 昇順 (default)
	///     2) false = 降順
	/// - Returns: 自分のinstance
	func orderBy(_ field: String, asc: Bool = true) -> TableQuery {
		if sqlOrderBy.count > 0 {
			sqlOrderBy.append(",")
		}
		
		sqlOrderBy.append("\(field)")
		if (!asc) {
			sqlOrderBy.append(" DESC")
		}
		return self
	}
	
	/// ORDER BY句全体を作成する
	///
	/// ```
	/// // SELECT * from account ORDER BY joinDate DESC, name ASC
	/// let cursor = SQuery("user.db").from("account")?
	/// 	.setOrderBy("joinDate DESC, name ASC")
	/// 	.select()
	///
	/// ```
	/// - Parameter orderByRaw: 自分のinstance
	/// - Returns: 自分のinstance
	func setOrderBy(_ orderByRaw: String) -> TableQuery {
		sqlOrderBy = orderByRaw
		return self
	}
	
	/// HAVING条件なしのGROUP BY句を作成する
	/// 参照
	/// ---
	/// `func groupBy(_ cols: [String], having: String, args: [Any?]) -> TableQuery`
	///
	/// - Parameter cols: GROUP BYするcolumn達
	/// - Returns: 自分のinstance
	func groupBy(_ cols: String...) -> TableQuery {
		sqlGroupByCols = cols
		sqlHaving = ""
		sqlHavingArgs.removeAll()
		return self
	}
	/// GROUP BY句を作成する
	/// 参照
	/// ---
	/// `func groupBy(_ cols: [String], having: String, args: [Any?]) -> TableQuery`
	func groupBy(_ cols: [String], having: String, args: Any?...) -> TableQuery {
		return groupBy(cols, having: having, args: args)
	}
	/// GROUP BY句を作成する
	///
	/// - Parameters:
	///   - cols: GROUP BYするcolumn達
	///   - having: HAVING条件
	///   - args: HAVING条件の「?」に対応するパラメーター
	/// - Returns: 自分のinstance
	func groupBy(_ cols: [String], having: String, args: [Any?]) -> TableQuery {
		sqlGroupByCols = cols
		sqlHaving = having
		sqlHavingArgs = args
		return self
	}

	func limit(_ count: Int, offset: Int = 0) -> TableQuery {
		sqlLimitCount = count
		sqlLimitOffset = offset
		return self
	}
	
	func columns(columns: [String]) -> TableQuery {
		sqlColumns = columns
		return self
	}
	func columns(_ columns: String...) -> TableQuery {
		sqlColumns = columns
		return self
	}
	
	func keys(_ cols: String...) -> TableQuery {
		sqlKeyColumns = cols
		return self
	}
	func keys(columns cols: [String]) -> TableQuery {
		sqlKeyColumns = cols
		return self
	}

	//--- SELECT ---
	
	private func makeQuerySql(forCount: Bool = false) -> String {
		// SELECT
		var sql = "SELECT "
		if sqlDistnict {
			sql.append("DISTNICT ")
		}
		
		if forCount {
			sql.append("count(*)")
		}
		else if sqlColumns.isEmpty {
			sql.append("*")
		}
		else {
			var first = true
			for col in sqlColumns {
				if first { first = false } else {
					sql.append(",")
				}
				sql.append("`\(col)`")
			}
		}
		
		// FROM
		sql.append(" FROM \(tableName) ")
		
		// JOIN
		if !sqlJoin.isEmpty  {
			sql.append(" \(sqlJoin) ")
			
			var first = true
			for t in sqlJoinTables {
				if first { first = false } else {
					sql.append(",")
				}
				//sql.append("`\(t)`")
				sql.append(t)
			}
			
			sql.append("ON \(sqlJoinOn)")
		}
		
		// WHERE
		if !sqlWhere.isEmpty {
			sql.append(" WHERE \(sqlWhere)")
		}
		
		// GROUP BY [HAVING]
		if !sqlGroupByCols.isEmpty {
			sql.append(" GROUP BY ")
			var first = false
			for col in sqlGroupByCols {
				if first { first = false } else {
					sql.append(",")
				}
				sql.append(col)
			}
			
			if !sqlHaving.isEmpty {
				sql.append(" HAVING \(sqlHaving)")
			}
		}
		
		// ORDER BY
		if !sqlOrderBy.isEmpty {
			sql.append(" ORDER BY \(sqlOrderBy) ")
		}
		
		// LIMIT
		if sqlLimitCount > 0 {
			sql.append(" LIMIT \(sqlLimitOffset),\(sqlLimitCount)")
		}
		sql.append(";");
		
		return sql
	}
	
	func select(_ cols: String...) -> SQLiteCursor {
		return select(columns: cols)
	}
	func select(columns cols: [String]) -> SQLiteCursor {
		if !cols.isEmpty {
			let _ = self.columns(columns: cols)
		}
		return select()
	}
	
	func select() -> SQLiteCursor {
		let sql = makeQuerySql()
		let args = sqlJoinOnArgs + sqlWhereArgs + sqlHavingArgs
		
		return db.query(sql: sql, args: args)
	}
	
	func select<T: SQueryRow>(factory: ()->T, forEach: (T)->Void) {
		let cursor = select()
		defer {
			cursor.close()
		}
		while cursor.next() {
			let newRow = factory()
			newRow.loadFrom(cursor: cursor)
			forEach(newRow)
		}
	}

	func select<T: SQueryRow>(factory: ()->T, _ cols: String...) -> [T] {
		return select(factory: factory, columns: cols)
	}
	func select<T: SQueryRow>(factory: ()->T, columns cols: [String]) -> [T] {
		if !cols.isEmpty {
			let _ = self.columns(columns: cols)
		}
		
		var rows = [T].init()
		select(factory: factory) { row in rows.append(row) }
		return rows;
	}
	
	func selectOne<T: SQueryRow>(factory: ()->T, _ cols: String...) -> T? {
		return selectOne(factory: factory, columns: cols)
	}
	func selectOne<T: SQueryRow>(factory: ()->T, columns cols: [String]) -> T? {
		if !cols.isEmpty {
			let _ = self.columns(columns: cols)
		}
		
		let rows = limit(1).select(factory: factory, columns: cols)
		return rows.isEmpty ? nil : rows[0]
	}
	
	func count() -> Int? {
		let sql = makeQuerySql(forCount: true)
		return db.executeScalar(sql: sql, args: sqlWhereArgs)
	}
	
	//--- INSERT ---
	func values(_ row: SQueryRow) -> TableQuery {
		return values(row.toValues())
	}
	func values(_ data: Dictionary<String,Any>) -> TableQuery {
		sqlValues = data
		return self
	}
	
	func insert(values row: SQueryRow, except cols: [String] = []) -> Bool {
		return values(row).insert(except: cols)
	}
	func insert(values data: Dictionary<String,Any>, except cols: [String] = []) -> Bool {
		return values(data).insert(except: cols)
	}
	
	func insert(except cols: [String] = []) -> Bool {
		var sql = "INSERT INTO \(tableName) "
		
		var cols = ""
		var vals = ""
		var args = [Any?]()
		var first = true

		for (colName, value) in sqlValues {
			if cols.contains(colName) {
				continue
			}
			
			if first { first = false } else {
				cols.append(",")
				vals.append(",")
			}
			cols.append("`\(colName)`")
			vals.append("?")
			
			args.append(value)
		}
		
		sql.append("(\(cols)) VALUES (\(vals));")
		return db.execute(sql: sql, args: args)
	}
	
	//--- UPDATE ---
	func update(autoMakeWhere: Bool = true) -> Int {
		var sql = "UPDATE \(tableName) SET "
		var args = [Any?]()
		var first = true
		for (colName, value) in sqlValues {
			if first { first = false } else {
				sql.append(",")
			}
			
			sql.append("`\(colName)`=?")
			args.append(value)
		}
		
		if sqlWhere.isEmpty && autoMakeWhere {
			for key in sqlKeyColumns {
				if let _ = sqlValues.index(forKey: key) {
					let value = sqlValues[key]
					let _ = whereAnd("`\(key)`=?", value)
				}
			}
		}
		
		if !sqlWhere.isEmpty {
			sql.append(" WHERE \(sqlWhere)")
			args.append(contentsOf: sqlWhereArgs)
		}
		
		sql.append(";")
		if db.execute(sql: sql, args: args) {
			return db.getLastChangedRowCount()
		}
		
		return 0
	}
	
	//--- DELETE ---
	func delete() -> Int {
		var sql = "DELETE FROM \(tableName)"
		if !sqlWhere.isEmpty {
			sql.append(" WHERE \(sqlWhere)")
		}
		
		sql.append(";")
		if db.execute(sql: sql, args: sqlWhereArgs) {
			return db.getLastChangedRowCount()
		}
		return 0
	}

	//--- INSERT or UPDATE ---
	func insertOrUpdate(exceptInsert cols: [String] = []) -> Bool {
		return insert(except: cols) || update() > 0
	}
	
	func updateOrInsert(exceptInsert cols: [String] = []) -> Bool {
		return update() > 0 || insert(except: cols)
	}

}
