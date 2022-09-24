import XCTest
@testable import rabbitmq_nio

final class rabbitmq_nioTests: XCTestCase {
    func testExample() throws {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(rabbitmq_nio().text, "Hello, World!")
    }
}
