//
//  SQueryEx.swift
//  Simple SQLite Query Library for Swift
//
//  - Version: 1.2
//  - Require Library: libsqlite3.tbd
//

import Foundation

protocol DbTableCreatable {
	static func createTable(db: SQuery)
}

protocol SQueryRowEx: SQueryRow, DbTableCreatable {
	static var tableName: String { get }
}

extension SQLiteCursor {
	func getDate(_ colIdx: Int, format: DateFormatter) -> Date? {
		guard let raw = getString(colIdx) else { return nil }
		return format.date(from: raw)
	}
}

extension SQuery {
	func from(_ tableClass: SQueryRowEx.Type) -> TableQuery? {
		return self.from(tableClass.tableName)
	}
	
	func create(tables: [DbTableCreatable.Type]) {
		for table in tables {
			table.createTable(db: self)
		}
	}
}

extension TableQuery {
	public func and(_ whereText: String, args: [Any?]) -> Self {
		return whereAnd(whereText, args)
	}
	public func and(_ whereText: String, _ args: Any?...) -> Self {
		return whereAnd(whereText, args: args)
	}
}
