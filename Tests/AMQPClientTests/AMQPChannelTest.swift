import XCTest
import AMQPClient
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
}
