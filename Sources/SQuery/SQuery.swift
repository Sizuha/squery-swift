//
//  Squery.swift
//  Simple SQLite Query Library for Swift
//
//  - Version: 1.6.2
//

import Foundation
import SQLite3

fileprivate let stdCalendar = Calendar(identifier: .gregorian)

// MARK: Sql Nil
public class SqlNil: NSObject {
	public override var description: String { "sqlNil" }
	public override var debugDescription: String { "sqlNil" }
	public let asNil: Any? = nil
	
	public override func isEqual(_ object: Any?) -> Bool { object is SqlNil }
}
public let sqlNil = SqlNil()

// MARK: Sql Vlaue
public enum SqlValue {
	case integer(_ value: Int)
	case real(_value: Double)
	case text(_ value: String)
	case currentTime
	case currentDate
	case currentTimeStamp
	case null
	case raw(_ text: String)
	
	func toSqlString() -> String {
		switch self {
		case .integer(let value): return "\(value)"
		case .real(let value): return "\(value)"
		case .text(let value): return "'\(value)'"
		case .currentTime: return "CURRENT_TIME"
		case .currentDate: return "CURRENT_DATE"
		case .currentTimeStamp: return "CURRENT_TIMESTAMP"
		case .null: return "NULL"
		case .raw(let text): return text
		}
	}
}

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
	public let code: Int32
	public let message: String
	
	public init(code: Int32, message: String = "") {
		self.code = code
		self.message = message
	}
	
	public var localizedDescription: String { self.message }
}

fileprivate let SQLITE_TRANSIENT = unsafeBitCast(OpaquePointer(bitPattern: -1), to: sqlite3_destructor_type.self)

fileprivate var enableDebugMode = true

/// デバッグ用のログを出力するか、しないかを設定
public func setEnableSQueryDebug(_ flag: Bool = true) {
	enableDebugMode = flag
}

fileprivate func printLog(_ text: String, _ args: CVarArg...) {
    guard enableDebugMode else { return }
    print("[SQuery] \(text)", args)
}

// MARK: - QueryResult

public class QueryResult {
    public let error: SQLiteError?
    public var isSuccess: Bool {
        get { error == nil }
    }

    public init(error: SQLiteError? = nil) {
        self.error = error
    }
}

public class UpdateQueryResult: QueryResult {
    public let rowCount: Int
    
    public init(rowCount: Int, error: SQLiteError? = nil) {
        self.rowCount = rowCount
        super.init(error: error)
    }
}

public class SelectQueryResult<T: SQueryRow>: QueryResult {
    public let rows: [T]
    public var row: T? { rows.first }
    
    public init(rows: [T], error: SQLiteError? = nil) {
        self.rows = rows
        super.init(error: error)
    }
}

//MARK: - SQLiteConnection

/**
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
	
	public var isClosed: Bool { db == nil }
	
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
		prepare(sql: sql, args: args)
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
		guard let stmt = stmt, !args.isEmpty else {
			return
		}
		
		var idx: Int32 = 0
		for arg in args {
			idx += 1 // 1 based index
			let result: Int32
			
			if arg == nil || arg is SqlNil {
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
		let (stmt, err) = prepare(sql: sql, args: args)
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
		let (stmt, err) = prepare(sql: sql, args: args)
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
		defer { sqlite3_finalize(stmt) }
		
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
		(try? executeScalar(sql: "PRAGMA user_version;")) ?? 0
	}
	public func setUserVersion(_ ver: Int) -> Bool {
		execute(sql: "PRAGMA user_version=\(ver);") == nil
	}

	// MARK: - TRANSACTION
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
	
	public func endTransaction() -> Bool { commit() }
	public func commit() -> Bool {
		execute(sql: "COMMIT TRANSACTION;") == nil
	}
	
	public func setSavePoint(name: String) -> Bool {
		execute(sql: "SAVEPOINT \(name);") == nil
	}
	
	public func releaseSavePoint(name: String) -> Bool {
		execute(sql: "RELEASE SAVEPOINT \(name);") == nil
	}
	
	public func rollback() -> Bool {
		execute(sql: "ROLLBACK TRANSACTION;") == nil
	}
	public func rollback(toSavePoint: String) -> Bool {
		execute(sql: "ROLLBACK TO SAVEPOINT \(toSavePoint);") == nil
	}
}

//MARK: - SQLiteCursor

/**
 SQLiteのクエリの結果(row達)を探索する

 **使い方**
 
 rowを探索する
 ```
 guard let conn = SQuery(at:"sample.db").open() else { return }
 let cursor = conn.query("SELECT * FROM user")
 defer {
    // Cursorの仕様が終わったら必ずcloseする!
    cursor.close()
    conn.close()
 }

 while cursor.next() {
    // ...
 }
```

 rowからデータを習得
 ```
 guard let tblAcc = SQuery(at:"user.db").from("account") else { return }
 defer { tblAcc.close() }
 let cursor = tblAcc
    .whereAnd("joinDate >= ", 2018)
    .orderBy("joinDate")
    .columns("id","name","age","joinDate")
    .select()
  
 defer { cursor.close() }
 while curosr.next() {
    var id: String?
    var name: String?
    var age: Int?
    var joinDate: Date?
 
    cursor.forEachColumn { cur, i in
        let name = cur.getColumnName(i)
        switch name {
        case "id": id = cursor.getString(i)
        case "name": name = cursor.getString(i)
        case "age": age = cur.getint(i)
        case "joinDate":
            let joindateRaw = cursor.getString(i)
            joinDate = joindateRaw != nil
                ? SQuery.newDateTimeFormat.date(from: joindateRaw)
                : nil
        default: return
        }
    }
 
    // ...
 }
 ```
*/
public class SQLiteCursor {
	private var stmt: OpaquePointer? = nil
	
	/// エラーが無い場合に「true」
	public var isSuccess: Bool {
		get { stmt != nil }
	}
	
	private var errorObj: SQLiteError? = nil
	/// エラーの内容
	public var error: SQLiteError? {
		get { errorObj }
	}
	
	private var columnCountRaw: Int32 = 0
	/// 結果rowのcolumn数
	public var columnCount: Int { Int(columnCountRaw) }
	
	private var columnNameMap = Dictionary<String,Int>()
	
	/// Cursorオブジェクトを作成する。
    ///
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
        columnNameMap.reserveCapacity(Int(columnCountRaw));
		
		for i in 0..<columnCountRaw {
			let colName = getColumnNameRaw(i)
			columnNameMap[colName] = Int(i)
		}
	}
	
	/// 初期状態に戻る
    ///
	/// rowをまた取得するには `next()` をコール。
	public func reset() {
		guard isSuccess else { return }
		sqlite3_reset(stmt)
	}
	
	/// 次のrowを習得する
	/// - Returns: 返すrowが残っている場合：**true**
	public func next() -> Bool {
		guard isSuccess else { return false }
		
		let res = sqlite3_step(stmt)
		switch res {
		case SQLITE_ROW: return true
		case SQLITE_DONE: return false
		default: return false
		}
	}
	
	/// Coursorの作業を完全に終了する
    ///
	/// 作業が終わったCursorは必ず`close()`すること
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
	/// - Returns: column名が存在する場合: columnのindex、存在しない場合: nil
	public func getColumnIndex(name: String) -> Int? {
		columnNameMap[name]
	}
	
	private func getColumnNameRaw(_ col: Int32) -> String {
		String(cString: sqlite3_column_name(stmt, col))
	}
	
	/// columnのindexでcolumnの名前を習得
	///
	/// - Parameter col: columnのindex
	/// - Returns: indexが存在する場合**column名**、存在しない場合**nil**を返す
	public func getColumnName(_ col: Int) -> String? {
		for (colName, colIdx) in columnNameMap {
            if colIdx == col { return colName }
		}
		return nil
	}
	
	/// 各column毎に処理を行う
	///
	/// - Parameter each: 各column毎で呼ばれるClosure
	/// - Parameter cursor: Coursorオブジェクト（自身）
	/// - Parameter index: 現在のcolumnのindex
	public func forEachColumn(_ each: (_ cursor: SQLiteCursor, _ index: Int)->Void) {
		for i in 0..<columnCount { each(self, i) }
	}
	
	// MARK: - Cursorからデータ取得
	
	private func getDataType(_ col: Int) -> Int32 {
		guard isSuccess else { return SQLITE_NULL }
		return sqlite3_column_type(stmt, Int32(col))
	}
	
	/// columnのデータが**nil**か確認
	///
	/// - Parameter col: columnのindex
	/// - Returns: **nil**の場合**true**
	public func isNull(_ col: Int) -> Bool {
		getDataType(col) == SQLITE_NULL
	}
	
	/// columnから32bitのInt型データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:データが**NULL**の場合: nil、それ以外: Int型の値
	public func getInt(_ col: Int) -> Int? {
		isNull(col)
			? nil
			: Int(sqlite3_column_int(stmt, Int32(col)))
	}
	
	/// columnからInt64型データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:データが**NULL**の場合: nil、それ以外: Int64型の値
	public func getInt64(_ col: Int) -> Int64? {
		isNull(col)
			? nil
			: Int64(sqlite3_column_int64(stmt, Int32(col)))
	}
	
	/// columnからString型データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:データが**NULL**の場合: nil、それ以外: 文字列（String型）
	public func getString(_ col: Int) -> String? {
		isNull(col)
			? nil
			: String(cString: sqlite3_column_text(stmt, Int32(col)))
	}
	
	/// columnからDouble型データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:データが**NULL**の場合: nil、それ以外: Double型の値
	public func getDouble(_ col: Int) -> Double? {
		isNull(col)
			? nil
			: sqlite3_column_double(stmt, Int32(col))
	}
	
	/// columnからFloat型データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:データが**NULL**の場合: nil、それ以外: Falot型の値
	public func getFloat(_ col: Int) -> Float? {
		if let value = getDouble(col) {
			return Float(value)
		}
		return nil
	}
	
	/// columnからBool型（true/false）データを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns:データが**NULL**の場合: nil、それ以外: Bool型の値
	public func getBool(_ col: Int) -> Bool? {
		isNull(col)
			? nil
			: getInt(col) != 0
	}
	
	/// columnからBinaryデータを習得
	///
	/// - Parameter col: columnのindex
	/// - Returns: データが**NULL**の場合: nil、それ以外: Byte Array
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
	/// - Returns: データが**NULL**の場合: nil、それ以外: BLOBデータのポインター
	public func getBlobRaw(_ col: Int) -> UnsafeRawPointer {
		sqlite3_column_blob(stmt, Int32(col))
	}
	
	/// 現在のrowが持っている全データをDictionary形式で返す
	///
	/// - Returns: 現在のrowの全データを `["column名":データ]` 形式で返す
	public func toDictionary() -> [String:Any?] {
		var result = [String:Any?]()
		forEachColumn { cur, i in
            guard let name = cur.getColumnName(i) else { return }
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

//MARK: - SQuery

/**
SQLite DBをべ便利に扱う為のライブラリ

**使い方**
 
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
```
class TableQuery
```
*/
public class SQuery {
	private var dataSource: String
	private var dbConn: SQLiteConnection? = nil
	
	public convenience init?(url: URL, mode: SQLiteOpenMode = .readWriteCreate) {
        let filepath = url.path
        if mode != .readWriteCreate {
            guard FileManager.default.fileExists(atPath: filepath) else {
                return nil
            }
        }
        
        self.init(at: filepath, mode: mode)
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
		case .readonly: dataSource.append("ro")
		case .readWrite: dataSource.append("rw")
		case .memory: dataSource.append("memory")
		default: dataSource.append("rwc")
		}
		
		print("[SQuery] data source: \(dataSource)")
	}
	
    /// DBファイルを開く
    ///
    /// このメソッドで直接DBファイルを開くよりは、`from()`メソッドを使うことをおすすめする
    /// - Returns:
    /// (1) DBファイルを開いて、SQLiteConnection オブジェクトとして返す。
    /// (2) 以前、開いたものがあったら、それを返す。
    /// (3) 失敗したら、nil
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
	/// **参照**
	///
	/// ```
	/// class TableQuery
	/// ```
	/// - Parameter table: Table名
	/// - Returns: クエリを作成できる**TableQuery**オブジェクト
	public func from(_ table: String) -> TableQuery? {
        guard let db = open() else { return nil }
        return TableQuery(db, table: table)
	}
	
	public func tableCreator(name: String) -> TableCreator? {
		guard let db = open() else { return nil }
        return TableCreator(db: db, name: name)
	}
	
	public func createTable(_ scheme: TableScheme, ifNotExists: Bool = true) -> Bool {
		guard let db = open() else { return false }
		return TableCreator(db: db, scheme: scheme).create(ifNotExists: ifNotExists) == nil
	}
	
	public func getUserVersion() -> Int {
		guard let db = open() else { return 0 }
		defer { db.close() }
		return db.getUserVersion()
	}
	
	public func setUserVersion(_ version: Int) -> Bool {
		guard let db = open() else { return false }
		defer { db.close() }
		return db.setUserVersion(version)
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
		Int64((datetime.timeIntervalSince1970 * 1000.0).rounded())
	}
	
	public static func toDate(timestamp: Int64) -> Date {
		Date(timeIntervalSince1970: TimeInterval(timestamp))
	}
}

//MARK: - TableCreator

public class TableScheme {
	public let tableName: String
	public let columnSchemes: [ColumnScheme]
	
	public init(name: String, columns: [ColumnScheme]) {
		self.tableName = name
		self.columnSchemes = columns
	}
    
    public func getKeys() -> [String] {
        self.columnSchemes.filter({ $0.isKey }).map { $0.name }
    }
}

public enum ColumnScheme {
	case key(
		_ name: String,
		autoInc: Bool = false,
		type: SQLiteColumnType,
		notNull: Bool = true,
		unique: Bool = false)
	case column(
		_ name: String,
		type: SQLiteColumnType,
		notNull: Bool = false,
		unique: Bool = false,
		default: SqlValue? = nil)
	case def(
		_ name: String,
		type: SQLiteColumnType,
		_ constraint: [ColumnConstraint] = [])
    
    var isKey: Bool {
        switch self {
        case .key( _, _, _, _, _): return true
        case .column(_, _, _, _, _): return false
        case .def(_, _, let constraint):
            return constraint.contains {
                switch $0 {
                case .primaryKey(desc: _): return true
                default: return false
                }
            }
        }
    }
    
    var name: String {
        switch self {
        case .key(let name, _, _, _, _): return name
        case .column(let name, _, _, _, _): return name
        case .def(let name, _, _): return name
        }
    }
}

public enum ColumnConstraint {
	case primaryKey(desc: Bool = false)
	case autoInc
	case notNull
	case unique
	case check(_ expr: String)
	case `default`(_ val: SqlValue)
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
		var pk_desc = false
		var notNull = false
		var unique = false
		var check: String? = nil
		
		var defaultValue: SqlValue? = nil
	}
	
	private var columns = [ColumnDefine]()
	
	public init(db: SQLiteConnection, name: String) {
		self.tableName = name
		self.db = db
	}
	
	public convenience init(db: SQLiteConnection, scheme: TableScheme) {
		self.init(db: db, name: scheme.tableName)
		
		for s in scheme.columnSchemes {
			switch s {
			case .key(let name, let autoInc, let type, let notNull, let unique):
				if autoInc {
					_ = addAutoInc(name, notNull: true, unique: false)
				}
				else {
					_ = addPrimaryKey(name, type: type, notNull: notNull, unique: unique)
				}
			case .column(let name, let type, let notNull, let unique, let defVal):
				_ = addColumn(name, type: type, notNull: notNull, unique: unique, default: defVal)
			case .def(let name, let type, let constraint):
				_ = addColumn(name, type: type, constraint: constraint)
			}
		}
	}
	
	public func addAutoInc(_ name: String, notNull: Bool = false, unique: Bool = false) -> Self {
		let colDef = ColumnDefine(name)
		colDef.type = .integer
		colDef.autoInc = true
		colDef.pk = true
		colDef.pk_desc = false
		colDef.unique = notNull
		colDef.notNull = unique
		columns.append(colDef)
		return self
	}
	
	public func addPrimaryKey(
		_ name: String,
		type: SQLiteColumnType,
		desc: Bool = false,
		notNull: Bool = false,
		unique: Bool = false)
		-> Self
	{
		let colDef = ColumnDefine(name)
		colDef.type = type
		colDef.pk = true
		colDef.pk_desc = desc
		colDef.unique = unique
		colDef.notNull = notNull
		columns.append(colDef)
		return self
	}
	
	public func addColumn(
		_ name: String,
		type: SQLiteColumnType,
		notNull: Bool = false,
		unique: Bool = false,
		default defaultVal: SqlValue? = nil)
		-> Self
	{
		let colDef = ColumnDefine(name)
		colDef.type = type
		colDef.notNull = notNull
		colDef.unique = unique
		colDef.defaultValue = defaultVal
		columns.append(colDef)
		return self
	}
	
	public func addColumn(
		_ name: String,
		type: SQLiteColumnType,
		constraint: [ColumnConstraint] = [])
		-> Self
	{
		let colDef = ColumnDefine(name)
		colDef.type = type
	
		for def in constraint {
			switch def {
			case .primaryKey(let desc):
				colDef.pk = true
				colDef.pk_desc = desc
				
			case .autoInc:
				colDef.autoInc = true
				colDef.pk = true
				
			case .notNull:
				colDef.notNull = true
				
			case .unique:
				colDef.unique = true
									
			case .check(let expr):
				colDef.check = expr
				
			case .`default`(let val):
				colDef.defaultValue = val
			}
		}
		
		columns.append(colDef)
		return self
	}

	public func createSql(ifNotExists: Bool = true) -> String {
		var sql = "CREATE TABLE "
		if (ifNotExists) { sql.append("IF NOT EXISTS ") }
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
			
			if col.unique {
				sql.append(" UNIQUE")
			}
			
			if let check = col.check {
				if !check.isEmpty {
					sql.append(" CHECK (\(check))")
				}
			}
			
			if let defaultVal = col.defaultValue {
				sql.append(" DEFAULT \(defaultVal.toSqlString())")
			}
			
			if col.autoInc || col.pk && isSinglePk {
				sql.append(" PRIMARY KEY")
				if col.pk_desc {
					sql.append(" DESC")
				}
				
				if col.autoInc {
					sql.append(" AUTOINCREMENT")
				}
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
		return sql
	}
	
	public func create(ifNotExists: Bool = true) -> SQLiteError? {
		let sql = createSql(ifNotExists: ifNotExists)
		return db.execute(sql: sql)
	}
	
	public func close() {
		db.close()
	}
}


//MARK: - TableQuery

/**
SQLite DBの一つのTableに対して、クエリ分を作成し、実行する

**Rowデータのオブジェクト（例）**
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

**SELECT**
```
// SELECT id, name, age FROM account WHERE age < 18 ORDER BY age DESC
guard let tableAcc = SQuery(at:"user.db").from("account") else { return }
defer { tableAcc.close()  }
let rows = tableAcc
    .columns("id","name","age") //省略すると「all columns」
    .setWhere("age < ?", 18)
    .orderBy(age, desc: true)
    .select(as: Account()).rows

for row in rows { ... }
```

**SELECT One**
```
// SELECT * account ORDER BY age DESC LIMIT 1
let oldest: Account? = tableAcc
  .orderBy(age, desc: true)
  .selectOne(as: Account()).row
```

**COUNT**
```
// SELECT count(*) FROM account WHERE age < 18
let under18cnt = tableAcc.where("age<?",18).count()
```

**INSERT**
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

**UPDATE**
```
// UPDATE account
// SET pwd="********", age=20, name="Tester"
// WHERE id="test";
tableAcc.keys("id").values(data).update()
// 又は
tableAcc.keys("id").update(set: data)
```

**INSERT or UPDATE**
```
tableAcc.keys("id").values(data).insertOrUpdate()
```

**DELETE**
```
// DELETE FROM account WHERE id = \(id)
tableAcc.where("id=?",id).delete()
```

**DROP**
```
// DROP TABLE account
tableAcc.drop()
```
*/
public class TableQuery {
	private let db: SQLiteConnection
	public var connection: SQLiteConnection { return db }
	
	private let tableName: String
	
    // DISTINCT
	private var sqlDistinct = false
	
	private var sqlJoin = ""
	private var sqlJoinTables = [String]()
	private var sqlJoinOn = ""
	private var sqlJoinOnArgs = [Any?]()
	
	private var sqlWhere = ""
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
	/// **Examples**
	///
	/// ```
	/// guard let dbConn = SQuery(at:"some.db").open() else { return }
    /// defer { dbConn.close() }
	/// let table = TableQuery(dbConn, "tableName")
	///	// ...
	/// ```
	/// SQueryクラスの`from()`メソッドをおすすめ
	/// ```
	/// guard let table = SQuery(at:"some.db").from("tableName") else { return }
    /// defer { table.close() }
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
		sqlDistinct = false
		
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
	
    // MARK: DISTINCT
	/// SELECT時に、重複したrow(行)は除外する設定
	///
	/// SQLの「SELECT DISTINCT」機能
	/// - Parameters:
	///     - flag:
    ///         1) true = 重複したrow(行)を除外する
    ///         2) false = 重複したrow(行)も残す
	/// - Returns: 自分のinstance
	public func distinct(_ flag: Bool = true) -> Self {
		sqlDistinct = flag
		return self
	}
	
    // MARK: JOIN
    /// JOIN句を作成する
	public func join(type joinType: SQueryJoin, tables: [String], on joinOn: String, _ args: Any?...) -> Self {
		join(type: joinType, tables: tables, on: joinOn, args: args)
	}
	/// JOIN句を作成する
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

    // MARK: WHERE
    /// WHER句を作成する
    ///
    /// **参照**
    /// ```
    /// func `where`(_ whereText: String, args: Any?...) -> Self
    /// ```
	public func setWhere(_ whereText: String, _ args: Any?...) -> Self {
        set(where: whereText, args: args)
	}
    
    /// WHERE句を作成する
    ///
    /// 検索条件を指定する。
    /// SQLiteのData Bind（「?」パラメーター）にも対応。
    /// ```
    /// // SELECT count(*) FROM account WHERE id=\(id) AND pass=\(pwd)
    /// let cursor = SQuery(at:"user.db").from("account")?
    ///   .setWhere("id=? AND pass=?", id, pwd)
    ///   .select()
    ///
    /// // UPDATE account SET pass=\(newPwd) WHERE id=\(id)
    /// guard let table = SQuery(at:"user.db").from("account") else { return }
    /// table.Where("id=?", id).update(set: ["pass":newPwd])
    /// table.close()
    /// ```
    ///
    /// - Parameters:
    ///   - whereText: WHERE句に入る条件
    ///   - args: 条件の中の「?」に対応するパラメータ達
    /// - Returns: 自分のinstance
    public func `where`(_ whereText: String, _ args: Any?...) -> Self {
        set(where: whereText, args: args)
    }

    /// WHERE句を作成する
    ///
    /// **参照**
    /// ```
    /// func `where`(_ whereText: String, args: Any?...) -> Self
    /// ```
	public func set(where whereText: String, args: [Any?] = []) -> Self {
		sqlWhereArgs.removeAll()
        guard !whereText.isEmpty else {
            sqlWhere = ""
            return self
        }
        
		sqlWhere = "(\(whereText))"
		for arg in args {
			sqlWhereArgs.append(arg)
		}
		
		return self
	}
	
	/// `where()`と同じだが、現在のWHERE句にAND条件で追加する
	/// ```
	/// // SELECT count(*) FROM account WHERE (id=\(id)) AND (pass=\(pwd))
	/// let loginOk = SQuery(at:"user.db").from("account")?
	///   .where("id=?", id)
	///   .andWhere("pass=?", pwd)
	///   .count() == 1
	/// ```
	/// `where()`を使わずに`whereAnd()`だけでWHERE句を作成することもできる
	/// ```
	/// let loginOk = SQuery(at:"user.db").from("account")?
	///   .andWhere("id=?", id)
	///   .andWhere("pass=?", pwd)
	///   .count() == 1
	/// ```
	///
	/// **参照**
	/// ```
    /// func `where`(_ whereText: String, args: Any?...) -> Self
	/// ```
	/// - Parameters:
	///   - whereText: 追加する条件
	///   - args: 条件の中の「?」に対応するパラメータ達
	/// - Returns: 自分のinstance
	public func andWhere(_ whereText: String, _ args: Any?...) -> Self {
        and(where: whereText, args: args)
	}
	/// `where()`と同じだが、現在のWHERE句にAND条件で追加する
    ///
    /// **参照**
	/// ```
	/// func andWhere(_ whereText: String, args: Any?...) -> Self
	/// ```
	public func and(where whereText: String, args: [Any?] = []) -> Self {
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
	
    // MARK: ORDER BY
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
	///   .set(orderBy: "joinDate DESC, name ASC")
	///   .select()
	///
	/// ```
	/// - Parameter orderBy: 並べ条件
	/// - Returns: 自分のinstance
	public func set(orderBy orderByRaw: String) -> Self {
		sqlOrderBy = orderByRaw
		return self
	}
	
    // MARK: GROUP BY
	/// HAVING条件なしのGROUP BY句を作成する
    ///
	/// **参照**
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
		groupBy(cols, having: having, args: args)
	}
	/// GROUP BY句を作成する
    ///
	/// **参照*:
	/// ```
	/// func groupBy(_ cols: [String], having: String, args: Any?...) -> Self
	/// ```
	public func groupBy(_ cols: [String], having: String, args: [Any?]) -> Self {
		sqlGroupByCols = cols
		sqlHaving = having
		sqlHavingArgs = args
		return self
	}

    // MARK: LIMIT
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
	
    // MARK: COLUMNS
	/// SELECTで習得するcolumn達を指定する
    ///
    /// **参照**
	/// ```
	/// func columns(_ columns: String...) -> Self
	/// ```
	public func set(columns: [String]) -> Self {
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
	
    // MARK: KEYS
	/// Tableのキー(key)のcolumnを指定する
	///
	/// `update()`時、キーのcolumnは修正内容から自動で外される
	/// - Parameter cols: キーのcomunn達
	/// - Returns: 自分のinstance
	public func keys(_ cols: String...) -> Self {
		sqlKeyColumns = cols
		return self
	}
	/// Tableのキー(key)のcolumnを指定する
    ///
    /// **参照**
	/// ```
	/// func keys(columns cols: String...) -> Self
	/// ```
	public func keys(columns cols: [String]) -> Self {
		sqlKeyColumns = cols
		return self
	}

	// MARK: SELECT
	
	/// SELECT用のクエリ文を作成する
	///
	/// 現在の設定値(WHERE, ORDER BY, LIMIT など)でSELECTクエリ文を作成する
	/// - Parameters
    ///     - forCount
	///         1) true = `count()`用のクエリを作る。例) `SELECT count(*) FROM ...`
	///         2) false = `select()`用のクエリを作る (default)
	/// - Returns: クエリ文
	private func makeQuerySql(forCount: Bool = false) -> String {
		// SELECT
		var sql = "SELECT "
		if sqlDistinct {
			sql.append("DISTINCT ")
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
			var first = true
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
	/// **参照**
	/// - distinct()
	/// - columns()
	/// - join()
	/// - where(), andWhere()
	/// - groupBy()
	/// - orderBy(),
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
	public func select<T: SQueryRow>(factory: ()->T, forEach: (_ each: T)->Void) -> SQLiteError?
    {
		let cursor = select()
		defer { cursor.close() }
		while cursor.next() {
			let newRow = factory()
			newRow.load(from: cursor)
			forEach(newRow)
		}
		return cursor.error
	}
    
    /// SELECTクエリを実行し、結果の各行(row)毎に処理を行う
    ///
    /// cursorは自動でcloseされる
    ///
    /// - Parameters:
    ///   - as: SQueryRow型のinstance
    ///   - forEach: 各行(row)で行う処理(clouser)
    ///   - each: 各行(row)のデータ、SQueryRow型
    /// - Returns:
    ///   エラーが無い場合は「nil」を返す
    public func select<T: SQueryRow>(as factory: @autoclosure ()->T, forEach: (_ each: T)->Void) -> SQLiteError?
    {
        select(factory: factory, forEach: forEach)
    }

	public func select<T: SQueryRow>(factory: ()->T) -> SelectQueryResult<T> {
		var rows = [T]()
		let error = select(factory: factory) { rows.append($0) }
        return SelectQueryResult(rows: rows, error: error)
	}
    public func select<T: SQueryRow>(as factory: @autoclosure ()->T) -> SelectQueryResult<T> {
        select(factory: factory)
    }

	public func selectOne<T: SQueryRow>(factory: ()->T) -> SelectQueryResult<T> {
        limit(1).select(factory: factory)
	}
    public func selectOne<T: SQueryRow>(as factory: @autoclosure ()->T) -> SelectQueryResult<T> {
        limit(1).select(factory: factory)
    }

	public func count() -> Int? {
		let sql = makeQuerySql(forCount: true)
		return try? db.executeScalar(sql: sql, args: sqlWhereArgs)
	}
	
	//MARK: INSERT
	public func values(_ row: SQueryRow) -> Self {
		return values(row.toValues())
	}
	public func values(_ data: [String:Any?]) -> Self {
		sqlValues = data
		return self
	}
	
	public func insert(values row: SQueryRow, except cols: [String] = []) -> UpdateQueryResult {
		values(row).insert(except: cols)
	}
	
	public func insert(values data: [String:Any?], except cols: [String] = []) -> UpdateQueryResult {
		values(data).insert(except: cols)
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
		
		if let error = db.execute(sql: sql, args: args) {
            return UpdateQueryResult(rowCount: 0, error: error)
		}
		return UpdateQueryResult(rowCount: 1, error: nil)
	}
	
	//MARK: UPDATE
	public func update(autoMakeWhere: Bool = true, keyIsNotChanged: Bool = false) -> UpdateQueryResult {
		update(set: sqlValues, autoMakeWhere: autoMakeWhere, keyIsNotChanged: keyIsNotChanged)
	}
	public func update(
        set values: SQueryRow,
        autoMakeWhere: Bool = true,
        keyIsNotChanged: Bool = false
    ) -> UpdateQueryResult {
		update(set: values.toValues(), autoMakeWhere: autoMakeWhere, keyIsNotChanged: keyIsNotChanged)
	}
	public func update(
        set values: [String:Any?],
        autoMakeWhere: Bool = true,
        keyIsNotChanged: Bool = false
    ) -> UpdateQueryResult {
		var sql = "UPDATE \(tableName) SET "
		var args = [Any?]()
		var first = true
		for (colName, value) in values {
            guard !(keyIsNotChanged && sqlKeyColumns.contains(colName)) else {
                continue
            }
            
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
					let _ = andWhere("`\(key)`=?", value ?? nil)
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
		
		if let error = db.execute(sql: sql, args: args) {
            return UpdateQueryResult(rowCount: 0, error: error)
		}
        let rowCount = db.getLastChangedRowCount()
		return UpdateQueryResult(rowCount: rowCount, error: nil)
	}
	
	//MARK: DELETE
	public func delete() -> UpdateQueryResult {
		var sql = "DELETE FROM \(tableName)"
		if !sqlWhere.isEmpty {
			sql.append(" WHERE \(sqlWhere)")
		}
		sql.append(";")
		
		if let error = db.execute(sql: sql, args: sqlWhereArgs) {
            return UpdateQueryResult(rowCount: 0, error: error)
		}
        let rowCount = db.getLastChangedRowCount()
        return UpdateQueryResult(rowCount: rowCount, error: nil)
	}
	
	//MARK: DROP
	public func drop() -> Bool {
		db.execute(sql: "DROP TABLE \(tableName);") == nil
	}

	//MARK: INSERT or UPDATE
	public func insertOrUpdate(keyIsNotChanged: Bool = false, exceptInsert cols: [String] = []) -> Bool {
        let res_insert = insert(except: cols)
        if let error = res_insert.error {
            printLog(error.localizedDescription)
        }
        
        if res_insert.isSuccess {
            return true
        }

        let res_update = update(keyIsNotChanged: keyIsNotChanged)
        if let error = res_update.error {
            printLog(error.localizedDescription)
        }
        return res_update.rowCount > 0
    }
	
	public func updateOrInsert(keyIsNotChanged: Bool = false, exceptInsert cols: [String] = []) -> Bool {
        let res_update = update(keyIsNotChanged: keyIsNotChanged)
        if let error = res_update.error {
            printLog(error.localizedDescription)
        }
        
        if res_update.rowCount > 0 {
            return true
        }
        
        let res_insert = insert(except: cols)
        if let error = res_insert.error {
            printLog(error.localizedDescription)
        }
        return res_insert.isSuccess
	}
}


// MARK: - Extensions

public protocol SQueryRowEx: SQueryRow {
    static var tableScheme: TableScheme { get }
}

public protocol SQueryRowEx2: SQueryRowEx {
    init()
}

public extension SQLiteCursor {
    func getDate(_ colIdx: Int, format: DateFormatter) -> Date? {
        guard let raw = getString(colIdx) else { return nil }
        return format.date(from: raw)
    }
}

public extension SQuery {
    func from(_ tableClass: SQueryRowEx.Type) -> TableQuery? {
        let scheme = tableClass.tableScheme
        guard let table = self.from(scheme.tableName) else { return nil }
        
        _ = table.keys(columns: scheme.getKeys())
        return table
    }
    
    func create(tables: [SQueryRowEx.Type]) {
        for table in tables {
            _ = createTable(table.tableScheme)
        }
    }
}

public extension TableQuery {
    func select<T: SQueryRowEx2>(type: T.Type, forEach: (_ each: T)->Void) -> SQLiteError?
    {
        select(as: T.init(), forEach: forEach)
    }
    
    func select<T: SQueryRowEx2>(type: T.Type) -> SelectQueryResult<T> {
        select(as: T.init())
    }
    
    func selectOne<T: SQueryRowEx2>(type: T.Type) -> SelectQueryResult<T> {
        limit(1).select(as: T.init())
    }
}
