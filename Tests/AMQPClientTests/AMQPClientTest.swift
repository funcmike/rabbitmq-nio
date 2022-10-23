import XCTest
import AMQPClient
@testable import AMQPClient

final class AMQPClientTest: XCTestCase {
    var client = AMQPClient(eventLoopGroupProvider: .createNew, config: .plain(.init()))

    override func tearDown() async throws {
        try await self.client.shutdown()
    }

    func testCanOpenChannelAndClose() async throws {
        guard case .connection(let connection) = try await client.connect(), case .connected = connection else {
            return  XCTFail()
        }

        let channel1 = try await client.openChannel(id: 1)
        XCTAssertNotNil(channel1)

        let channel2 = try await client.openChannel(id: 2)
        XCTAssertNotNil(channel2)

        guard case .connection(let connection) = try await client.close(), case .closed = connection else {
            return XCTFail()
        }
    }

    func testfailOnBadChannel() async throws {
        guard case .connection(let connection) = try await client.connect(), case .connected = connection else {
            return XCTFail()
        }

        do {
            let _ = try await client.openChannel(id: 0)
            XCTFail()
        } catch {
            XCTAssert(error is AMQPClientError)
        }


        do {
            let _ = try await client.close()
            XCTFail()
        } catch  {
           XCTAssert(error is AMQPClientError)
        }
    }
}
