import XCTest
import NIOPosix
import AMQPClient
@testable import AMQPClient

@available(macOS 12.0, *)
final class AMQPClientTest: XCTestCase {
    func testCanOpenChannelAndShutdown() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        let connection: AMQPConnection

        do {
            connection = try await AMQPConnection.connect(use: eventLoopGroup.next(), from: .init(connection: .plain, server: .init()))
        } catch {
            XCTAssert(error is AMQPConnectionError)
            throw error
        }

        let channel1 = try await connection.openChannel()
        XCTAssertNotNil(channel1)
        XCTAssertEqual(channel1.ID, 1)

        let channel2 = try await connection.openChannel()
        XCTAssertNotNil(channel2)
        XCTAssertEqual(channel2.ID, 2)

        let channel3 = try await connection.openChannel()
        XCTAssertNotNil(channel3)
        XCTAssertEqual(channel3.ID, 3)

        let channel4 = try await connection.openChannel()
        XCTAssertNotNil(channel4)
        XCTAssertEqual(channel4.ID, 4)

        try await connection.close()
    }
}
