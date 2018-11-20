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

enum SQueryError: Error {
	case prepareFailed
	case noResult
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

Code
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
	/// - seealso: next()
	func reset() {
		sqlite3_reset(stmt)
	}
	
	/// 次のrowを習得する
	/// - returns: rowがあったらtrue
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
	
	func close() {
		if let stmt = stmt {
			sqlite3_finalize(stmt)
		}
		stmt = nil
	}
	
	func getColumnIndex(name: String) -> Int? {
		return columnNameMap[name]
	}
	
	private func getColumnNameRaw(_ col: Int32) -> String {
		return String(cString: sqlite3_column_name(stmt, col))
	}
	
	func getColumnName(_ col: Int) -> String? {
		for (colName, colIdx) in columnNameMap {
			if colIdx == col {
				return colName
			}
		}
		return nil
	}
	
	func forEachColumn(_ each: (SQLiteCursor,Int)->Void) {
		for i in 0..<columnCount {
			each(self, i)
		}
	}
	
	//--- get Datas ---

	func isNull(_ col: Int) -> Bool {
		let dataType = sqlite3_column_type(stmt, Int32(col))
		return dataType == SQLITE_NULL
	}
	
	func getInt(_ col: Int) -> Int? {
		return isNull(col)
			? nil
			: Int(sqlite3_column_int(stmt, Int32(col)))
	}
	
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

class SQuery {
	private var filepath: String
	
	required init(_ dbfile: String) {
		filepath = dbfile
	}
	
	internal func getError(_ db: OpaquePointer) -> String {
		let errMsgRaw = sqlite3_errmsg(db)
		return String(cString: errMsgRaw!)
	}
	
	func open() -> SQLiteConnection? {
		var db: OpaquePointer? = nil

		let res = sqlite3_open(filepath, &db)
		let conn = SQLiteConnection(db!)
		
		if res != SQLITE_OK {
			printLog("Can't open DB -> %@", conn.getLastError()!)
			conn.close()
			return nil;
		}
		return conn
	}
	
	func close(_ db: OpaquePointer) {
		sqlite3_close(db)
	}
	
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

class TableQuery {
	private let db: SQLiteConnection
	private let name: String
	
	private var sqlDistnict = false
	
	private var sqlWhere = String()
	private var sqlWhereArgs = [Any?]()
	
	private var sqlOrderBy = ""
	private var sqlGroupBy = ""
	
	private var sqlLimitCount = 0
	private var sqlLimitOffset = 0
	
	private var sqlColumns = [String]()
	private var sqlKeyColumns = [String]()
	
	private var sqlValues = Dictionary<String,Any>()
	
	required init(_ db: SQLiteConnection, table: String) {
		self.db = db
		name = table
	}
	
	func reset() -> TableQuery {
		sqlDistnict = false
		sqlWhere = ""
		sqlWhereArgs.removeAll()
		sqlOrderBy = ""
		sqlGroupBy = ""
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
	
	func setWhere(_ whereText: String, _ args: Any?...) -> TableQuery {
		sqlWhereArgs.removeAll()
		
		sqlWhere = "(\(whereText))"
		for arg in args {
			sqlWhereArgs.append(arg)
		}
		
		return self
	}
	
	func whereAnd(_ whereText: String, _ args: Any?...) -> TableQuery {
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
	func setOrderBy(_ orderByRaw: String) {
		sqlOrderBy = orderByRaw
	}
	
	func groupBy(_ groupByText: String) -> TableQuery {
		self.sqlGroupBy = groupByText
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
		sql.append(" FROM \(name) ")
		
		// JOIN
		// not yet
		
		// WHERE
		if !sqlWhere.isEmpty {
			sql.append("WHERE \(sqlWhere) ")
		}
		
		// ORDER BY
		if !sqlOrderBy.isEmpty {
			sql.append("ORDER BY \(sqlOrderBy) ")
		}
		
		// GROUP BY
		if !sqlGroupBy.isEmpty {
			sql.append("GROUP BY \(sqlGroupBy) ")
		}
		
		// LIMIT
		if sqlLimitCount > 0 {
			sql.append("LIMIT \(sqlLimitOffset),\(sqlLimitCount)")
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
		return db.query(sql: sql, args: sqlWhereArgs)
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
	
	//--- DELETE ---
	func delete() -> Int {
		var sql = "DELETE FROM \(name)"
		if !sqlWhere.isEmpty {
			sql.append(" WHERE \(sqlWhere)")
		}
		sql.append(";")
		if db.execute(sql: sql, args: sqlWhereArgs) {
			return db.getLastChangedRowCount()
		}
		return 0
	}
	
	//--- INSERT ---
	func values(_ row: SQueryRow) -> TableQuery {
		return values(row.toValues())
	}
	func values(_ data: Dictionary<String,Any>) -> TableQuery {
		sqlValues = data
		return self
	}
	
	func insert(values row: SQueryRow, except cols: String...) -> Bool {
		return values(row).insert(except: cols)
	}
	func insert(values data: Dictionary<String,Any>, except cols: String...) -> Bool {
		return values(data).insert(except: cols)
	}
	
	func insert(except cols: String...) -> Bool {
		return insert(except: cols)
	}
	func insert(except cols: [String] = []) -> Bool {
		var sql = "INSERT INTO \(name) "
		
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
		var sql = "UPDATE \(name) SET "
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
	
	//--- INSERT & UPDATE ---
	func insertOrUpdate(except cols: String...) -> Bool {
		return insert(except: cols) || update() > 0
	}
	
	func updateOrInsert(except cols: String...) -> Bool {
		return update() > 0 || insert(except: cols)
	}

}
