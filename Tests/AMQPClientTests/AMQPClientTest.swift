import XCTest
import AMQPClient
@testable import AMQPClient

final class AMQPClientTest: XCTestCase {
    var client = AMQPClient(eventLoopGroupProvider: .createNew, config: .plain(.init()))

    override func tearDown() async throws {
        try await self.client.shutdown()
    }

    func testCanOpenChannelAndClose() async throws {
        try await client.connect()

        let channel1 = try await client.openChannel(id: 1)
        XCTAssertNotNil(channel1)

        let channel2 = try await client.openChannel(id: 2)
        XCTAssertNotNil(channel2)

        try await client.close()
    }

    func testfailOnBadChannel() async throws {
        try await client.connect()

        do {
            let _ = try await client.openChannel(id: 0)
            XCTFail()
        } catch {
            XCTAssert(error is AMQPClientError)
        }


        do {
            try await client.close()
            XCTFail()
        } catch  {
           XCTAssert(error is AMQPClientError)
        }
    }
}
