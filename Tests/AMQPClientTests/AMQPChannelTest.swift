import XCTest
import AMQPClient
import AMQPProtocol
import NIO

@testable import AMQPClient

final class AMQPChannelTest: XCTestCase {
    var client = AMQPClient(eventLoopGroupProvider: .createNew, config: .plain(.init()))

    override func setUp() async throws {
        let _ = try await client.connect()
    }

    override func tearDown() async throws {
        try await self.client.shutdown()
    }

    func testCanCloseChannel() async throws {
        let channel = try await client.openChannel(id: 1)

        guard case .channel(let channel) = try await channel.close(), case .closed = channel else {
            return  XCTFail()
        }
    }

    func testQueue() async throws {
        let channel = try await client.openChannel(id: 1)

        guard case .channel(let ch) = try await channel.queueDeclare(name: "test", durable: false), case .queue(let queue) = ch, case .declared = queue else {
            return  XCTFail()            
        }

        guard case .channel(let ch) = try await channel.queueBind(queue: "test", exchange: "amq.topic", routingKey: "test"), case .queue(let queue) = ch, case .binded = queue else {
            return  XCTFail()            
        }

        guard case .channel(let ch) = try await channel.queueUnbind(queue: "test", exchange: "amq.topic", routingKey: "test"), case .queue(let queue) = ch, case .unbinded = queue else {
            return  XCTFail()            
        }

        guard case .channel(let ch) = try await channel.queuePurge(name: "test"), case .queue(let queue) = ch, case .purged = queue else {
            return  XCTFail()            
        }

        guard case .channel(let ch) = try await channel.queueDelete(name: "test"), case .queue(let queue) = ch, case .deleted = queue else {
            return  XCTFail()            
        }

        let _ = try await channel.close()
    }

    func testExchange() async throws {
        let channel = try await client.openChannel(id: 1)

        guard case .channel(let ch) = try await channel.exchangeDeclare(name: "test1", type: "topic"), case .exchange(let exchange) = ch, case .declared = exchange else {
            return  XCTFail()            
        }

        guard case .channel(let ch) = try await channel.exchangeDeclare(name: "test2", type: "topic"), case .exchange(let exchange) = ch, case .declared = exchange else {
            return  XCTFail()            
        }

        guard case .channel(let ch) = try await channel.exchangeBind(destination: "test1", source: "test2", routingKey: "test"), case .exchange(let exchange) = ch, case .binded = exchange else {
            return  XCTFail() 
        }

        guard case .channel(let ch) = try await channel.exchangeUnbind(destination: "test1", source: "test2", routingKey: "test"), case .exchange(let exchange) = ch, case .unbinded = exchange else {
            return  XCTFail() 
        }

        guard case .channel(let ch) = try await channel.exchangeDelete(name: "test1"), case .exchange(let exchange) = ch, case .deleted = exchange else {
            return  XCTFail()            
        }

        guard case .channel(let ch) = try await channel.exchangeDelete(name: "test2"), case .exchange(let exchange) = ch, case .deleted = exchange else {
            return  XCTFail()            
        }

        let _ = try await channel.close()
    }

    func testBasicPublish() async throws {
        let channel = try await client.openChannel(id: 1)

        let _ = try await channel.queueDeclare(name: "test", durable: true)

        let body = ByteBufferAllocator().buffer(string: "{}")

        try await channel.basicPublish(from: body, exchange: "", routingKey: "test")

        let _ = try await channel.queueDelete(name: "test")

        let _ = try await channel.close()
    }

    func testBasicGet() async throws {
        let channel = try await client.openChannel(id: 1)

        let _ = try await channel.queueDeclare(name: "test", durable: true)

        let body = ByteBufferAllocator().buffer(string: "{}")

        let properties: Properties = .init(
            contentType: "application/json",
            contentEncoding: "UTF-8",
            headers: ["test": .longString("test")],
            deliveryMode: 1,
            priority: 1,
            correlationID: "correlationID",
            replyTo: "replyTo",
            expiration: "60000",
            messageID: "messageID",
            timestamp: 100,
            type: "type",
            userID: "guest",
            appID: "appID"
        )

        try await channel.basicPublish(from: body, exchange: "", routingKey: "test", properties: properties)

        guard let msg = try await channel.basicGet(queue: "test") else {
            return  XCTFail()
        }

        XCTAssertEqual(msg.messageCount, 0)
        XCTAssertEqual(msg.message.body.getString(at: 0, length: msg.message.body.readableBytes), "{}")
        XCTAssertEqual(properties, msg.message.properties)

        let _ = try await channel.queueDelete(name: "test")

        let _ = try await channel.close()
    }
}
