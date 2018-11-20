//
//  squeryTests.swift
//  squeryTests
//
//  Created by IL KYOUNG HWANG on 2018/11/08.
//  Copyright © 2018年 Sizuha's Atelier. All rights reserved.
//

import XCTest
@testable import squery

class squeryTests: XCTestCase {
	
	private var db: SQuery? = nil
	
	class User: SQueryRow {
		var name = ""
		var age = 0
		
		func loadFrom(cursor: SQLiteCursor) {
			name = cursor.getString(0) ?? ""
			age = cursor.getInt(1) ?? 0
		}
		
		func toValues() -> Dictionary<String, Any?> {
			return Dictionary<String, Any?>()
		}
	}

    override func setUp() {
        db = SQuery("sample.db")
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testExample() {
        let rows = db?.from(table: "user")?
			.select(factory: { User() }, "name", "age")
		
		
    }

    func testPerformanceExample() {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
        }
    }

}
