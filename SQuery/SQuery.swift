//
//  Squery.swift
//  Simple SQLite Query Library for Swift
//
//  - Version: 1.1
//  - Require Library: libsqlite3.tbd
//

import Foundation
import SQLite3

fileprivate let stdCalendar = Calendar(identifier: .gregorian)

public protocol SQueryRow {
	func load(from cursor: SQLiteCursor)
	func toValues() -> [String:Any?]
}

public enum SQLiteOpenMode {
	case readWriteCreate // rwc
	case readWrite // rw
	case readonly // ro
	case memory // memory
}

public enum SQLiteTransactionMode {
	case deferred
	case immediate
	case exclusive
}

public enum SQueryJoin {
	case inner
	case leftOuter
	case cross
}

public enum SQLiteColumnType: String {
	case text = "TEXT"
	case numeric = "NUMERIC"
	case integer = "INTEGER"
	case float = "REAL"
	case none = "NONE"
}

public class SQLiteError: Error {
	let code: Int32
	private let message: String
	
	init(code: Int32, message: String = "") {
		self.code = code
		self.message = message
	}
	
	public var localizedDescription: String {
		return self.message
	}
}

public class UpdateQueryResult {
	public var rowCount: Int = 0
	public var error: SQLiteError? = nil
	public var isSuccess: Bool {
		get { return error == nil }
	}
}

private let SQLITE_TRANSIENT = unsafeBitCast(OpaquePointer(bitPattern: -1), to: sqlite3_destructor_type.self)

private var enableDebugMode = true
public func setEnableSQueryDebug(_ flag: Bool = true) {
	enableDebugMode = flag
}

private func printLog(_ text: String, _ args: CVarArg...) {
	if enableDebugMode {
		NSLog("[SQuery] \(text)", args)
	}
}

private func toSqliteError(_ error: Error) -> SQLiteError {
	if let sqlErr = error as? SQLiteError {
		return sqlErr
	}
	else {
		return SQLiteError(code: SQLITE_ERROR, message: error.localizedDescription)
	}
}

/***********************************************************************************************************************
SQLite DBを操作するクラス。

DBファイルを開いて、クエリを実行できる。
*/
public class SQLiteConnection {
	private var db: OpaquePointer? = nil
	
	public required init(_ db: OpaquePointer) {
		self.db = db
	}
	
	public func close() {
		if let db = db {
			sqlite3_close(db)
		}
		db = nil
	}
	
	public var isClosed: Bool {
		return db == nil
	}
	
	public func getLastError() -> String? {
		if let db = db {
			let errMsgRaw = sqlite3_errmsg(db)
			return String(cString: errMsgRaw!)
		}
		return nil
	}
	
	public func getLastChangedRowCount() -> Int {
		if let db = db {
			return Int(sqlite3_changes(db))
		}
		return 0
	}
	
	private func prepare(sql: String, _ args: Any?...) -> (OpaquePointer?, SQLiteError?) {
		return prepare(sql: sql, args: args)
	}
	private func prepare(sql: String, args: [Any?]) -> (OpaquePointer?, SQLiteError?) {
		printLog("prepare sql: \(sql)")
		
		var stmt: OpaquePointer? = nil
		var err: SQLiteError? = nil
		
		let result = sqlite3_prepare_v2(db, sql, Int32(sql.utf8.count), &stmt, nil)
		if result == SQLITE_OK {
			bindAll(stmt, args: args)
		}
		else {
			err = SQLiteError(code: result, message: getLastError() ?? "")
		}
		return (stmt, err)
	}

	private func bindAll(_ stmt: OpaquePointer?, _ args: Any?...) {
		bindAll(stmt, args: args)
	}
	private func bindAll(_ stmt: OpaquePointer?, args: [Any?]) {
		guard let stmt = stmt else {
			return
		}
		
		var idx: Int32 = 0
		for arg in args {
			idx += 1 // 1 based index
			let result: Int32
			
			if arg == nil {
				result = sqlite3_bind_null(stmt, idx)
			}
			else {
				switch arg {
				case is Bool:
					let data = arg as! Bool
					result = sqlite3_bind_int(stmt, idx, data ? 1 : 0)
					
				case is Int8:
					result = sqlite3_bind_int(stmt, idx, Int32(arg as! Int8))
				case is Int16:
					result = sqlite3_bind_int(stmt, idx, Int32(arg as! Int16))
				case is Int32:
					result = sqlite3_bind_int(stmt, idx, arg as! Int32)
					
				case is Int:
					result = sqlite3_bind_int64(stmt, idx, Int64(arg as! Int))
				case is Int64:
					result = sqlite3_bind_int64(stmt, idx, arg as! Int64)
					
				case is Float:
					result = sqlite3_bind_double(stmt, idx, Double(arg as! Float))
				case is Double:
					result = sqlite3_bind_double(stmt, idx, arg as! Double)
					
				case is Date:
					let data = arg as! Date
					let timestamp = SQuery.toTimestamp(data)
					result = sqlite3_bind_int64(stmt, idx, timestamp)
					
				case is String:
					let data = arg as! String
					let length = Int32(data.lengthOfBytes(using: String.Encoding.utf8))
					result = sqlite3_bind_text(stmt, idx, data, length, SQLITE_TRANSIENT)
					
				case is [UInt8]:
					let data = arg as! [UInt8]
					result = sqlite3_bind_blob(stmt, idx, data, Int32(data.count), SQLITE_TRANSIENT)
					
				default:
					fatalError("[SQuery] data binding error, Not Support Data-Type >> index: \(idx), data: \(arg.debugDescription)")
				}
			}
			
			guard result == SQLITE_OK else {
				fatalError("[SQuery] data binding error >> index: \(idx), data: \(arg.debugDescription)")
			}
		}
	}
	
	public func query(sql: String, _ args: Any?...) -> SQLiteCursor {
		let (stmt, err) = prepare(sql: sql, args)
		return SQLiteCursor(stmt, error: err)
	}
	public func query(sql: String, args: [Any?]) -> SQLiteCursor {
		let (stmt, err) = prepare(sql: sql, args: args)
		return SQLiteCursor(stmt, error: err)
	}
	
	private func executeScalar(_ stmt: OpaquePointer) -> Int? {
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

	public func executeScalar(sql: String, _ args: Any?...) throws -> Int? {
		let (stmt, err) = prepare(sql: sql, args)
		if let error = err { throw error }
		return executeScalar(stmt!)
	}
	public func executeScalar(sql: String, args: [Any?]) throws -> Int? {
		let (stmt, err) = prepare(sql: sql, args: args)
		if let error = err { throw error }
		return executeScalar(stmt!)
	}
	
	private func excute(_ stmt: OpaquePointer)  -> SQLiteError? {
		let res = sqlite3_step(stmt)
		defer {
			sqlite3_finalize(stmt)
		}
		
		switch res {
		case SQLITE_OK, SQLITE_ROW, SQLITE_DONE: return nil
		default:
			return SQLiteError(code: res, message: getLastError() ?? "")
		}
	}
	
	public func execute(sql: String, _ args: Any?...) -> SQLiteError? {
		let (stmt, err) = prepare(sql: sql, args: args)
		return err ?? excute(stmt!)
	}
	public func execute(sql: String, args: [Any?])  -> SQLiteError? {
		let (stmt, err) = prepare(sql: sql, args: args)
		return err ?? excute(stmt!)
	}
	
	public func getUserVersion() -> Int {
		return (try? executeScalar(sql: "PRAGMA user_version;")) ?? 0
	}
	public func setUserVersion(_ ver: Int) -> Bool {
		return execute(sql: "PRAGMA user_version=\(ver);") == nil
	}

	//--- TRANSACTION ---
	// cf. https://www.sqlite.org/lang_transaction.html
	public func beginTransaction(_ mode: SQLiteTransactionMode = .deferred) -> Bool {
		var modeStr = ""
		switch mode {
		case .immediate:
			modeStr = "IMMEDIATE"
		case .exclusive:
			modeStr = "EXCLUSIVE"
		default:
			modeStr = "DEFERRED"
		}
		
		return execute(sql: "BEGIN \(modeStr) TRANSACTION;") == nil
	}
	
	public func endTransaction() -> Bool {
		return commit()
	}
	public func commit() -> Bool {
		return execute(sql: "COMMIT TRANSACTION;") == nil
	}
	
	public func setSavePoint(name: String) -> Bool {
		return execute(sql: "SAVEPOINT \(name);") == nil
	}
	
	public func releaseSavePoint(name: String) -> Bool {
		return execute(sql: "RELEASE SAVEPOINT \(name);") == nil
	}
	
	public func rollback() -> Bool {
		return execute(sql: "ROLLBACK TRANSACTION;") == nil
	}
	public func rollback(toSavePoint: String) -> Bool {
		return execute(sql: "ROLLBACK TO SAVEPOINT \(toSavePoint);") == nil
	}
}


/***********************************************************************************************************************
SQLiteのクエリの結果(row達)を探索する

使い方
---

rowを探索する
```
if let conn = SQuery(at:"sample.db").open() {
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
if let tblAcc = SQuery(at:"user.db").from("account") {
  defer { tblAcc.close() }
  if let cursor = tblAcc
    .whereAnd("joinDate >= ", 2018)
    .orderBy("joinDate")
    .columns("id","name","age","joinDate")
    .select()
  {
    defer { cursor.close() }
    while curosr.next() {
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
*/
public class SQLiteCursor {
	private var stmt: OpaquePointer? = nil
	
	/// エラーが無い場合に「true」
	public var isSuccess: Bool {
		get { return stmt != nil }
	}
	
	private var errorObj: SQLiteError? = nil
	/// エラーの内容
	public var error: SQLiteError? {
		get { return errorObj }
	}
	
	private var columnCountRaw: Int32 = 0
	/// 結果rowのcolumn数
	public var columnCount: Int {
		return Int(columnCountRaw)
	}
	
	private var columnNameMap = Dictionary<String,Int>()
	
	/// Cursorオブジェクトを作成する。
	/// 直接オブジェクトを作成する事は無く、SQLiteConnectionクラスから実行されたクエリの結果として作られる。
	///
	/// - Parameter stmt:
	///   `sqlite3_prepare_v2()`もしくはSQLiteConnectionクラスの`prepare()`の戻り値
	///   errorの場合は「nil」
	/// - Parameter error:
	///   エラーの内容、無い場合は「nil」
	public required init(_ stmt: OpaquePointer?, error: SQLiteError? = nil) {
		self.errorObj = error
		self.stmt = stmt
		guard isSuccess else { return }
		
		columnCountRaw = sqlite3_column_count(stmt)
		
		for i in 0..<columnCountRaw {
			let colName = getColumnNameRaw(i)
			columnNameMap[colName] = Int(i)
		}
	}
	
	/// 初期状態に戻る。
	/// rowをまた習得するには `next()` をコール。
	public func reset() {
		guard isSuccess else { return }
		sqlite3_reset(stmt)
	}
	
	/// 次のrowを習得する
	/// - Returns: rowがあったら**true**
	public func next() -> Bool {
		guard isSuccess else { return false }
		
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
	public func close() {
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
	public func getColumnIndex(name: String) -> Int? {
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
	public func getColumnName(_ col: Int) -> String? {
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
	public func forEachColumn(_ each: (_ cursor: SQLiteCursor, _ index: Int)->Void) {
		for i in 0..<columnCount {
			each(self, i)
		}
	}
	
	//--- get Datas ---
	
	private func getDataType(_ col: Int) -> Int32 {
		guard isSuccess else { return SQLITE_NULL }
		return sqlite3_column_type(stmt, Int32(col))
	}
	
	/// columnのデータが**nil**か確認
	///
	/// - Parameter col: columnのindex
	/// - Returns: **nil**の場合**true**
	public func isNull(_ col: Int) -> Bool {
		return getDataType(col) == SQLITE_NULL
	}
	
	/// columnから32bitのInt型データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:
	///   1) データが**NULL**の場合: nil
	///   2) それ以外: Int型の値
	public func getInt(_ col: Int) -> Int? {
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
	public func getInt64(_ col: Int) -> Int64? {
		return isNull(col)
			? nil
			: Int64(sqlite3_column_int64(stmt, Int32(col)))
	}
	
	/// columnからString型データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:
	///   1) データが**NULL**の場合: nil
	///   2) それ以外: 文字列（String型）
	public func getString(_ col: Int) -> String? {
		return isNull(col)
			? nil
			: String(cString: sqlite3_column_text(stmt, Int32(col)))
	}
	
	/// columnからDouble型データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:
	///   1) データが**NULL**の場合: nil
	///   2) それ以外: Double型の値
	public func getDouble(_ col: Int) -> Double? {
		return isNull(col)
			? nil
			: sqlite3_column_double(stmt, Int32(col))
	}
	
	/// columnからFloat型データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:
	///   1) データが**NULL**の場合: nil
	///   2) それ以外: Falot型の値
	public func getFloat(_ col: Int) -> Float? {
		if let value = getDouble(col) {
			return Float(value)
		}
		return nil
	}
	
	/// columnからBool型（true/false）データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:
	///   1) データが**NULL**の場合: nil
	///   2) それ以外: Bool型の値
	public func getBool(_ col: Int) -> Bool? {
		return isNull(col)
			? nil
			: getInt(col) != 0
	}
	
	/// columnからBinaryデータを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:
	///   1) データが**NULL**の場合: nil
	///   2) それ以外: Byte Array
	public func getBlob(_ col: Int) -> [UInt8]? {
		guard !isNull(col) else { return nil }
		if let data = sqlite3_column_blob(stmt, Int32(col)) {
			return data.load(as: [UInt8].self)
		}
		return nil
	}
	
	/// columnからBinaryデータのポインターを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:
	///   1) データが**NULL**の場合: nil
	///   2) それ以外: BLOBデータのポインター
	public func getBlobRaw(_ col: Int) -> UnsafeRawPointer {
		return sqlite3_column_blob(stmt, Int32(col))
	}
	
	/// 現在のrowが持っている全データをDictionary形式で返す
	///
	/// - Returns: 現在のrowの全データを `["column名":データ]` 形式で返す
	public func toDictionary() -> [String:Any?] {
		var result = [String:Any?]()
		forEachColumn { cur, i in
			if let name = cur.getColumnName(i) {
				let dataType = getDataType(i)
				var value: Any? = nil
				switch dataType {
				case SQLITE_INTEGER:
					value = cur.getInt64(i)
				case SQLITE_FLOAT:
					value = cur.getDouble(i)
				case SQLITE_BLOB:
					value = cur.getBlob(i)
				default:
					value = cur.getString(i)
					break
				}
				
				result[name] = value
			}
		}
		return result
	}
	
	/// Cursorが持っている全データをDictionaryの配列で返す
	///
	/// - Parameter closeCursor: 作業完了後すぐCursorをクローズする
	/// - Returns: 各rowのデータを`toDictionary()`でDictionary型で作成し、それらを配列でまとめる
	public func toDictionaryAll(closeCursor: Bool = false) -> [[String:Any?]] {
		var result = [[String:Any?]]()
		reset()
		while next() {
			result.append(toDictionary())
		}
		
		if closeCursor { close() }
		return result
	}
}


/***********************************************************************************************************************
SQLite DBをべ便利に扱う為のライブラリ

使い方
---
Open & Close
```
let dbConn = SQuery(at:"db_file_path").open()
dbConn?.close()
```

Tableを指定してクエリを作成する
```
let db = SQuery(at:"db_file_path")
// from() メソッドは自動でDBをopenする
let cursor = db.from("Table名")?.select()

// 使用後
cursor?.close()
db.close()
```

参照
---
```
class TableQuery
```
*/
public class SQuery {
	private var dataSource: String
	private var dbConn: SQLiteConnection? = nil
	
	public convenience init?(url: URL, mode: SQLiteOpenMode = .readWriteCreate) {
		do {
			let filepath = try String(contentsOf: url)
			self.init(at: filepath, mode: mode)
		}
		catch {
			return nil
		}
	}

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
	public required init(at dbfile: String, mode: SQLiteOpenMode = .readWriteCreate) {
		if !dbfile.hasPrefix("file:") {
			dataSource = "file:"
			
			var filePath: String
			if dbfile.hasPrefix("/") {
				filePath = dbfile
			}
			else {
				let path = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first!
				filePath = "\(path)/\(dbfile)"
			}
			
			if let encoded =
				filePath.addingPercentEncoding(withAllowedCharacters: NSCharacterSet.urlQueryAllowed)
			{
				dataSource.append(encoded)
			}
			else {
				dataSource.append(filePath)
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
		
		printLog("[SQuery] data source: \(dataSource)")
	}
	
	/// DBファイルを開く
	///
	/// このメソッドで直接DBファイルを開くよりは、`from()`メソッドを使うことをおすすめする。
	/// - Returns:
	///   1) DBファイルを開いて、SQLiteConnection オブジェクトとして返す
	///   2) 以前、開いたものがあったら、それを返す
	///   3) 失敗したら、nil
	public func open() -> SQLiteConnection? {
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
	public func close() {
		dbConn?.close()
		dbConn = nil
	}
	
	/// Tableを指定する
	///
	/// 以後、指定したTableに対してクエリを実行する事になる
	///
	/// 参照
	/// ---
	/// ```
	/// class TableQuery
	/// ```
	/// - Parameter table: Table名
	/// - Returns: クエリを作成できる**TableQuery**オブジェクト
	public func from(_ table: String) -> TableQuery? {
		if let db = open() {
			return TableQuery(db, table: table)
		}
		return nil
	}
	
	public func tableCreator(name: String) -> TableCreator? {
		if let db = open() {
			return TableCreator(db: db, name: name)
		}
		return nil
	}
	
	
	//--- Utils ---
	
	static let utcTimeZone = TimeZone(abbreviation: "UTC")!
	static let standardLocal = Locale(identifier: "en_US_POSIX")
	
	public static func newDateTimeFormat() -> DateFormatter {
		let fmt = DateFormatter()
		fmt.calendar = stdCalendar
		fmt.locale = standardLocal
		fmt.dateFormat = "yyyy-MM-dd HH:mm:ss"
		fmt.timeZone = utcTimeZone
		return fmt
	}
	public static func newDateFormat() -> DateFormatter {
		let fmt = DateFormatter()
		fmt.calendar = stdCalendar
		fmt.locale = standardLocal
		fmt.dateFormat = "yyyy-MM-dd"
		fmt.timeZone = utcTimeZone
		return fmt
	}
	public static func newTimeFormat() -> DateFormatter {
		let fmt = DateFormatter()
		fmt.calendar = stdCalendar
		fmt.locale = standardLocal
		fmt.dateFormat = "HH:mm:ss"
		fmt.timeZone = utcTimeZone
		return fmt
	}

	public static func toTimestamp(_ datetime: Date) -> Int64 {
		return Int64((datetime.timeIntervalSince1970 * 1000.0).rounded())
	}
	
	public static func toDate(timestamp: Int64) -> Date {
		return Date(timeIntervalSince1970: TimeInterval(timestamp))
	}
}

public class TableCreator {
	private let db: SQLiteConnection
	private let tableName: String
	
	private class ColumnDefine {
		let name: String
		
		init(_ name: String) {
			self.name = name
		}
		
		var type: SQLiteColumnType = .none
		var autoInc = false
		var pk = false
		var notNull = false
		var unique = false
	}
	
	private var columns = [ColumnDefine]()
	
	public init(db: SQLiteConnection, name: String) {
		self.tableName = name
		self.db = db
	}
	
	public func addAutoInc(_ name: String, notNull: Bool = false, unique: Bool = false) -> Self {
		let colDef = ColumnDefine(name)
		colDef.type = .integer
		colDef.autoInc = true
		colDef.pk = true
		colDef.unique = notNull
		colDef.notNull = unique
		columns.append(colDef)
		return self
	}
	
	public func addPrimaryKey(_ name: String, type: SQLiteColumnType, notNull: Bool = false, unique: Bool = false) -> Self {
		let colDef = ColumnDefine(name)
		colDef.type = type
		colDef.pk = true
		colDef.unique = unique
		colDef.notNull = notNull
		columns.append(colDef)
		return self
	}
	
	public func addColumn(_ name: String, type: SQLiteColumnType, notNull: Bool = false, unique: Bool = false) -> Self {
		let colDef = ColumnDefine(name)
		colDef.type = type
		colDef.notNull = notNull
		colDef.unique = unique
		columns.append(colDef)
		return self
	}
	
	public func create(ifNotExists: Bool = true) -> SQLiteError? {
		var sql = "CREATE TABLE "
		if (ifNotExists) {
			sql.append("IF NOT EXISTS ")
		}
		sql.append(tableName)
		
		var keys = [String]()
		for col in columns {
			if col.autoInc {
				keys.removeAll()
				keys.append(col.name)
				break
			}
			else if col.pk {
				keys.append(col.name)
			}
		}
		
		let isSinglePk = keys.count == 1
		var first = true
		sql.append(" (")
		for col in columns {
			if first { first = false } else { sql.append(",") }
			
			sql.append("\(col.name) \(col.type.rawValue)")
			
			if col.notNull {
				sql.append(" NOT NULL")
			}
			
			if col.autoInc || col.pk && isSinglePk {
				sql.append(" PRIMARY KEY")
				if col.autoInc {
					sql.append(" AUTOINCREMENT")
				}
			}
			
			if col.unique {
				sql.append(" UNIQUE")
			}
		}
		
		if !keys.isEmpty && !isSinglePk {
			sql.append(", PRIMARY KEY(")
			first = true
			for colName in keys {
				if first { first = false } else { sql.append(",") }
				sql.append(colName)
			}
			sql.append(")")
		}
		
		sql.append(");")
		return db.execute(sql: sql)
	}
	
	public func close() {
		db.close()
	}
}


/***********************************************************************************************************************
SQLite DBの一つのTableに対して、クエリ分を作成し、実行する

Rowデータのオブジェクト（例）
```
class Account: SQueryRow {
  var id = ""
  var pass = ""
  var name = ""
  var age = 0
  var joinDate: Date? = nil

  func load(from cursor: SQLiteCursor) {
    cursor.forEachColumn { cursor, index in
      let colName = cursor.getColumnName(index)
      switch colName {
      case "id":
        id = cursor.getString(index)
      case "pwd":
        pass = cursor.getString(index)
      case "name":
        name = cursor.getString(index)
      case "age":
        age = cursor.getInt(index)
      case "join_date":
        joinDate: Date? = joindateRaw != nil
          ? SQuery.newDateTimeFormat().date(from: joindateRaw)
          : nil
      }
    }
  }

  func toValues() -> Dictionary<String,Any> {
	return [
      "id": id, "pwd": pass, "name": name, "age": age,
      "join_date": SQuery.newDateTimeFormat().string(joinDate)
    ]
  }
}
```

SELECT
---
```
// SELECT id, name, age FROM account WHERE age < 18 ORDER BY age DESC
if let tableAcc = SQuery(at:"user.db").from("account") {
  defer { tableAcc.close()  }
  let rows = tableAcc
    .columns("id","name","age") //省略すると「all columns」
    .setWhere("age < ?", 18)
    .orderBy(age, desc: true)
    .select { Account() }

  for row in rows { ... }
}
```

SELECT One
---
```
// SELECT * account ORDER BY age DESC LIMIT 1
let oldest: Account = try? tableAcc
  .orderBy(age, desc: true)
  .selectOne { Account() }.0
```

COUNT
---
```
// SELECT count(*) FROM account WHERE age < 18
let under18cnt = tableAcc.setWhere("age<?",18).count()
```

INSERT
---
```
let data = Account()
data.id = "test"
data.pwd = "********"
data.age = 20
data.name = "Tester"

// INSERT INTO account (id,pwd,age,name)
// VALUES ("test","********",20,"Tester");
tableAcc.values(data).insert()
// 又は
tableAcc.insert(values: data)
```

UPDATE
---
```
// UPDATE account
// SET pwd="********", age=20, name="Tester"
// WHERE id="test";
tableAcc.keys("id").values(data).update()
// 又は
tableAcc.keys("id").update(set: data)
```

INSERT or UPDATE
---
```
tableAcc.keys("id").values(data).insertOrUpdate()
```

DELETE
---
```
// DELETE FROM account WHERE id = \(id)
tableAcc.setWhere("id=?",id).delete()
```

DROP
---
```
// DROP TABLE account
tableAcc.drop()
```
*/
public class TableQuery {
	private let db: SQLiteConnection
	public var connection: SQLiteConnection { return db }
	
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
	
	private var sqlValues = Dictionary<String,Any?>()
	
	/// DBからTableを指定て、instanceを作る
	///
	/// Examples
	/// ---
	/// ```
	/// if let dbConn = SQuery(at:"some.db").open() {
	/// 	let table = TableQuery(dbConn, "tableName")
	///		// ...
	/// }
	/// ```
	/// SQueryクラスの`from()`メソッドをおすすめ
	/// ```
	/// let table = SQuery(at:"some.db").from("tableName")
	/// ```
	///
	/// - Parameters:
	///   - db: 開いたDBオブジェクト
	///   - table: Table名
	public required init(_ db: SQLiteConnection, table: String) {
		self.db = db
		tableName = table
	}
	
	/// DBを閉じる
	///
	/// SQueryクラスの`close()`やSQLiteConnectionクラスの`close()`と同じ機能
	public func close() {
		db.close()
	}
	
	/// クエリの設定を初期化する
	///
	/// ただし、`keys()`の設定は残る。
	/// クエリを一度実行してから、また別の設定でクエリを実行すためには一度`reset()`する事をおすすめする。
	/// - Returns: 自分のinstance
	public func reset() -> Self {
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
	
	/// SELECT時に、重複したrow(行)は除外する設定
	///
	/// SQLの「SELECT DISTNIC」機能
	/// - Parameter flag:
	///   1) true = 重複したrow(行)を除外する
	///   2) false = 重複したrow(行)も残す
	/// - Returns: 自分のinstance
	public func distnict(_ flag: Bool = true) -> Self {
		sqlDistnict = flag
		return self
	}
	
	public func join(type joinType: SQueryJoin, tables: [String], on joinOn: String, _ args: Any?...) -> Self {
		return join(type: joinType, tables: tables, on: joinOn, args: args)
	}
	/// 参照
	/// ---
	/// ```
	/// func join(type joinType: SQueryJoin, tables: [String], on joinOn: String, _ args: Any?...) -> Self
	/// ```
	public func join(type joinType: SQueryJoin, tables: [String], on joinOn: String, args: [Any?]) -> Self {
		switch joinType {
		case .cross:
			sqlJoin = "CROSS JOIN"
		case .leftOuter:
			sqlJoin = "LEFT OUTER JOIN"
		default:
			sqlJoin = "INNER JOIN"
		}

		sqlJoinTables = tables
		sqlJoinOn = joinOn
		sqlJoinOnArgs = args
		
		return self
	}

	/// WHERE句を作成する
	///
	/// 検索条件を指定する。
	/// SQLiteの「?」パラメーターに対応。
	/// ```
	/// // SELECT count(*) FROM account WHERE id=\(id) AND pass=\(pwd)
	/// let cursor = SQuery(at:"user.db").from("account")?
	///   .setWhere("id=? AND pass=?", id, pwd)
	///   .select()
	///
	/// // UPDATE account SET pass=\(newPwd) WHERE id=\(id)
	/// if let table = SQuery(at:"user.db").from("account") {
	///   table.setWhere("id=?", id).update(set: ["pass":newPwd])
	///   table.close()
	/// }
	/// ```
	///
	/// - Parameters:
	///   - whereText: WHERE句に入る条件
	///   - args: 条件の中の「?」に対応するパラメータ達
	/// - Returns: 自分のinstance
	public func setWhere(_ whereText: String, _ args: Any?...) -> Self {
		return setWhere(whereText, args: args)
	}
	/// 参照
	/// ---
	/// ```
	/// func setWhere(_ whereText: String, args: Any?...) -> Self
	/// ```
	public func setWhere(_ whereText: String, args: [Any?]) -> Self {
		sqlWhereArgs.removeAll()
		
		sqlWhere = "(\(whereText))"
		for arg in args {
			sqlWhereArgs.append(arg)
		}
		
		return self
	}
	
	/// `setWhere()`と同じだが、現在のWHERE句にAND条件で追加する
	/// ```
	/// // SELECT count(*) FROM account WHERE (id=\(id)) AND (pass=\(pwd))
	/// let loginOk = SQuery(at:"user.db").from("account")?
	///   .setWhere("id=?", id)
	///   .whereAnd("pass=?", pwd)
	///   .count() == 1
	/// ```
	/// `setWhere()`を使わずに`whereAnd()`だけでWHERE句を作成することもできる
	/// ```
	/// let loginOk = SQuery(at:"user.db").from("account")?
	///   .whereAnd("id=?", id)
	///   .whereAnd("pass=?", pwd)
	///   .count() == 1
	/// ```
	///
	/// 参照
	/// ---
	/// ```
	/// func setWhere(_ whereText: String, args: [Any?]) -> Self
	/// ```
	/// - Parameters:
	///   - whereText: 追加する条件
	///   - args: 条件の中の「?」に対応するパラメータ達
	/// - Returns: 自分のinstance
	public func whereAnd(_ whereText: String, _ args: Any?...) -> Self {
		return whereAnd(whereText, args: args)
	}
	/// 参照
	/// ---
	/// ```
	/// func whereAnd(_ whereText: String, args: Any?...) -> Self
	/// ```
	public func whereAnd(_ whereText: String, args: [Any?]) -> Self {
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
	/// 結果をソートする
	/// ```
	/// // SELECT * from account ORDER BY joinDate DESC, name ASC
	/// let cursor = SQuery(at:"user.db").from("account")?
	///   .orderBy("joinDate", desc: true)
	///   .orderBy("name")
	///   .select()
	///
	/// ```
	/// - Parameters:
	///   - field: ソートするcolumn名
	///   - desc:
	///     1) true = 降順
	///     2) false = 昇順 (default)
	/// - Returns: 自分のinstance
	public func orderBy(_ field: String, desc: Bool = false) -> Self {
		if sqlOrderBy.count > 0 {
			sqlOrderBy.append(",")
		}
		
		sqlOrderBy.append("\(field)")
		if (desc) {
			sqlOrderBy.append(" DESC")
		}
		return self
	}
	
	/// ORDER BY句全体を作成する
	///
	/// 結果をソートする
	/// ```
	/// // SELECT * from account ORDER BY joinDate DESC, name ASC
	/// let cursor = SQuery(at:"user.db").from("account")?
	///   .setOrderBy("joinDate DESC, name ASC")
	///   .select()
	///
	/// ```
	/// - Parameter orderByRaw: 自分のinstance
	/// - Returns: 自分のinstance
	public func setOrderBy(_ orderByRaw: String) -> Self {
		sqlOrderBy = orderByRaw
		return self
	}
	
	/// HAVING条件なしのGROUP BY句を作成する
	/// 参照
	/// ---
	/// ```
	/// func groupBy(_ cols: [String], having: String, args: Any?...) -> Self
	/// ```
	///
	/// - Parameter cols: GROUP BYするcolumn達
	/// - Returns: 自分のinstance
	public func groupBy(_ cols: String...) -> Self {
		sqlGroupByCols = cols
		sqlHaving = ""
		sqlHavingArgs.removeAll()
		return self
	}
	/// GROUP BY句を作成する
	///
	/// 結果をcolumnでグループ化する
	///
	/// - Parameters:
	///   - cols: GROUP BYするcolumn達
	///   - having: HAVING条件
	///   - args: HAVING条件の「?」に対応するパラメーター
	/// - Returns: 自分のinstance
	public func groupBy(_ cols: [String], having: String, args: Any?...) -> Self {
		return groupBy(cols, having: having, args: args)
	}
	/// GROUP BY句を作成する
	/// 参照
	/// ---
	/// ```
	/// func groupBy(_ cols: [String], having: String, args: Any?...) -> Self
	/// ```
	public func groupBy(_ cols: [String], having: String, args: [Any?]) -> Self {
		sqlGroupByCols = cols
		sqlHaving = having
		sqlHavingArgs = args
		return self
	}

	/// LIMIT区を作成する
	///
	/// 結果として返されるrow(行)数を制限する
	/// ```
	/// // SELECT * FROM scroe ORDER BY point DESC LIMIT \(pageOffset),10
	/// let pageOffset = (pageNo-1)*10
	/// let cursor = SQuery(at:"user.db").from("score")?
	///   .orderBy("point", desc: true)
	///   .limit(10, offset: pageOffset)
	///   .select()
	/// ```
	/// - Parameters:
	///   - count: 最大の行数
	///   - offset: スタート位置(0 base)
	/// - Returns: 自分のinstance
	public func limit(_ count: Int, offset: Int = 0) -> Self {
		sqlLimitCount = count
		sqlLimitOffset = offset
		return self
	}
	
	/// 参照
	/// ---
	/// ```
	/// func columns(columns: String...) -> Self
	/// ```
	public func columns(columns: [String]) -> Self {
		sqlColumns = columns
		return self
	}
	/// SELECTで習得するcolumn達を指定する
	///
	/// - Parameter columns: column名（複数指定可）、省略すると「すべてのcolumn」
	/// - Returns: 自分のinstance
	public func columns(_ columns: String...) -> Self {
		sqlColumns = columns
		return self
	}
	
	/// Tableのキー(key)のcolumnを指定する
	///
	/// `update()`時、キーのcolumnは修正内容から自動で外される
	/// - Parameter cols: キーのcomunn達
	/// - Returns: 自分のinstance
	public func keys(_ cols: String...) -> Self {
		sqlKeyColumns = cols
		return self
	}
	/// 参照
	/// ---
	/// ```
	/// func keys(columns cols: String...) -> Self
	/// ```
	public func keys(columns cols: [String]) -> Self {
		sqlKeyColumns = cols
		return self
	}

	//--- SELECT ---
	
	/// SELECT用のクエリ文を作成する
	///
	/// 現在の設定値(WHERE, ORDER BY, LIMIT など)でSELECTクエリ文を作成する
	/// - Parameter forCount:
	///   1) true = `count()`用のクエリを作る　例) SELECT count(*) ...
	///   2) false = `select()`用のクエリを作る (default)
	/// - Returns: クエリ文
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
				sql.append(col)
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
			sql.append(" ORDER BY \(sqlOrderBy)")
		}
		
		// LIMIT
		if sqlLimitCount > 0 {
			sql.append(" LIMIT \(sqlLimitOffset),\(sqlLimitCount)")
		}
		sql.append(";");
		
		return sql
	}
	
	/// SELECTクエリを実行し、その結果をCurosrオブジェクトで返す
	///
	/// 参照
	/// ---
	/// - distnict()
	/// - columns()
	/// - join()
	/// - setWhere(), whereAnd()
	/// - groupBy(), setGroupBy()
	/// - orderBy(), setOrderBy()
	/// - limit()
	/// ```
	/// class SQLiteCursor
	/// ```
	///
	/// - Returns: クエリの結果(Curosr)
	public func select() -> SQLiteCursor {
		let sql = makeQuerySql()
		let args = sqlJoinOnArgs + sqlWhereArgs + sqlHavingArgs
		return db.query(sql: sql, args: args)
	}
	
	/// SELECTクエリを実行し、結果の各行(row)毎に処理を行う
	///
	/// cursorは自動でcloseされる
	///
	/// - Parameters:
	///   - factory: SQueryRow型のinstanceを生成するclouser
	///   - forEach: 各行(row)で行う処理(clouser)
	///   - each: 各行(row)のデータ、SQueryRow型
	/// - Returns:
	///   エラーが無い場合は「nil」を返す
	public func select<T: SQueryRow>(factory: ()->T, forEach: (_ each: T)->Void) -> SQLiteError? {
		let cursor = select()
		
		defer {
			cursor.close()
		}
		while cursor.next() {
			let newRow = factory()
			newRow.load(from: cursor)
			forEach(newRow)
		}
		
		return cursor.error
	}

	public func select<T: SQueryRow>(factory: ()->T) -> ([T], SQLiteError?) {
		var rows = [T]()
		let error = select(factory: factory) { row in rows.append(row) }
		return (rows, error);
	}
	
	public func selectOne<T: SQueryRow>(factory: ()->T) -> (T?, SQLiteError?) {
		let (rows, error) = limit(1).select(factory: factory)
		return (rows.first, error)
	}
	
	public func count() -> Int? {
		let sql = makeQuerySql(forCount: true)
		return try? db.executeScalar(sql: sql, args: sqlWhereArgs)
	}
	
	//--- INSERT ---
	public func values(_ row: SQueryRow) -> Self {
		return values(row.toValues())
	}
	public func values(_ data: [String:Any?]) -> Self {
		sqlValues = data
		return self
	}
	
	public func insert(values row: SQueryRow, except cols: [String] = []) -> UpdateQueryResult {
		return values(row).insert(except: cols)
	}
	
	public func insert(values data: [String:Any?], except cols: [String] = []) -> UpdateQueryResult {
		return values(data).insert(except: cols)
	}
	
	public func insert(except exceptCols: [String] = []) -> UpdateQueryResult {
		var sql = "INSERT INTO \(tableName) "
		
		var cols = ""
		var vals = ""
		var args = [Any?]()
		var first = true

		for (colName, value) in sqlValues {
			if exceptCols.contains(colName) {
				continue
			}
			
			if first { first = false } else {
				cols.append(",")
				vals.append(",")
			}
			cols.append("\(colName)")
			vals.append("?")
			
			args.append(value)
		}
		
		sql.append("(\(cols)) VALUES (\(vals));")
		
		let result = UpdateQueryResult()
		if let error = db.execute(sql: sql, args: args) {
			result.error = error
		}
		else {
			result.rowCount = 1
		}
		return result
	}
	
	//--- UPDATE ---
	public func update(autoMakeWhere: Bool = true) -> UpdateQueryResult {
		return update(set: sqlValues, autoMakeWhere: autoMakeWhere)
	}
	public func update(set values: SQueryRow, autoMakeWhere: Bool = true) -> UpdateQueryResult {
		return update(set: values.toValues(), autoMakeWhere: autoMakeWhere)
	}
	public func update(set values: [String:Any?], autoMakeWhere: Bool = true) -> UpdateQueryResult {
		var sql = "UPDATE \(tableName) SET "
		var args = [Any?]()
		var first = true
		for (colName, value) in values {
			if first { first = false } else {
				sql.append(",")
			}
			sql.append("`\(colName)`=?")
			args.append(value)
		}
		
		if sqlWhere.isEmpty && autoMakeWhere {
			for key in sqlKeyColumns {
				if let _ = values.index(forKey: key) {
					let value = values[key]
					let _ = whereAnd("`\(key)`=?", value ?? nil)
				}
			}
		}
		
		if !sqlWhere.isEmpty {
			sql.append(" WHERE \(sqlWhere)")
			args.append(contentsOf: sqlWhereArgs)
		}
		
		if enableDebugMode {
			var log = ""
			for a in args {
				log.append("\(String(describing: a)),")
			}
			printLog("params: \(log)")
		}
		
		sql.append(";")
		
		let result = UpdateQueryResult()
		if let error = db.execute(sql: sql, args: args) {
			result.error = error
		}
		else {
			result.rowCount = db.getLastChangedRowCount()
		}
		return result
	}
	
	//--- DELETE ---
	public func delete() -> UpdateQueryResult {
		var sql = "DELETE FROM \(tableName)"
		if !sqlWhere.isEmpty {
			sql.append(" WHERE \(sqlWhere)")
		}
		sql.append(";")
		
		let result = UpdateQueryResult()
		if let error = db.execute(sql: sql, args: sqlWhereArgs) {
			result.error = error
		}
		else {
			result.rowCount = db.getLastChangedRowCount()
		}
		return result
	}
	
	//--- DROP ---
	public func drop() -> Bool {
		return db.execute(sql: "DROP TABLE \(tableName);") == nil
	}

	//--- INSERT or UPDATE ---
	public func insertOrUpdate(exceptInsert cols: [String] = []) -> Bool {
		return insert(except: cols).isSuccess || update().rowCount > 0
	}
	
	public func updateOrInsert(exceptInsert cols: [String] = []) -> Bool {
		return update().rowCount > 0 || insert(except: cols).isSuccess
	}
}
