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

        let channel1 = try await connection.openChannel(id: 1)

        XCTAssertNotNil(channel1)

        let channel2 = try await connection.openChannel(id: 2)
        XCTAssertNotNil(channel2)

        let channel3 = try await connection.openChannel()
        XCTAssertNotNil(channel3)

        let channel4 = try await connection.openChannel()
        XCTAssertNotNil(channel4)


        try await connection.close()
    }

    func testfailOnBadChannel() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

       let connection = try await AMQPConnection.connect(use: eventLoopGroup.next(), from: .init(connection: .plain, server: .init()))
        do {
            let _ = try await connection.openChannel(id: 0)
            XCTFail()
        } catch {
            XCTAssert(error is AMQPConnectionError)
        }

        do {
            try await connection.close()
        } catch  {
           XCTAssert(error is AMQPConnectionError)
        }
    }
}
