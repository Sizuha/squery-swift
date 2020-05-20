//
//  SQueryEx.swift
//  Simple SQLite Query Library for Swift
//
//  - Version: 1.4.8
//  - Require Library: libsqlite3.tbd
//

import Foundation

public protocol SQueryRowEx: SQueryRow {
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
