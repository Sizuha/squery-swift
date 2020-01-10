//
//  SQueryEx.swift
//  Simple SQLite Query Library for Swift
//
//  - Version: 1.2
//  - Require Library: libsqlite3.tbd
//

import Foundation

public protocol SQueryRowEx: SQueryRow {
	static var tableName: String { get }
	static var tableScheme: TableScheme { get }
}

public extension SQLiteCursor {
	func getDate(_ colIdx: Int, format: DateFormatter) -> Date? {
		guard let raw = getString(colIdx) else { return nil }
		return format.date(from: raw)
	}
}

public extension SQuery {
	func from(_ tableClass: SQueryRowEx.Type) -> TableQuery? {
		return self.from(tableClass.tableName)
	}
	
	func create(tables: [SQueryRowEx.Type]) {
		for table in tables {
			_ = createTable(table.tableScheme)
		}
	}
}

public extension TableQuery {
	func and(_ whereText: String, args: [Any?]) -> Self {
		return whereAnd(whereText, args)
	}
	func and(_ whereText: String, _ args: Any?...) -> Self {
		return whereAnd(whereText, args: args)
	}
}
