
@testable import AMQPClient
import AMQPProtocol
import NIO
import NIOPosix
import XCTest

final class AMQPChannelLargePayloadsTest: XCTestCase {
    let testExchange = "lage_payloads"
    var connection: AMQPConnection!
    var channel: AMQPChannel!

    override func setUp() async throws {
        connection = try await AMQPConnection.connect(use: MultiThreadedEventLoopGroup.singleton.next(), from: .init(connection: .plain, server: .init()))
        channel = try await connection.openChannel()
        try await channel.exchangeDeclare(name: testExchange, type: "fanout")
    }

    override func tearDown() async throws {
        try await channel?.close()
        try await connection?.close()
    }

    func testPublishLargePayloads() async throws {
        try await channel.basicPublish(from: ByteBuffer(megaBytes: 50), exchange: testExchange, routingKey: "")
        try await channel.basicPublish(from: ByteBuffer(megaBytes: 10), exchange: testExchange, routingKey: "")
    }

    func testGetLargePayloads() async throws {
        let queueName = "lp-test-get"
        let payload1 = ByteBuffer(megaBytes: 20)
        let payload2 = ByteBuffer(megaBytes: 30, repeating: UInt8(ascii: "b"))

        try await channel.queueDeclare(name: queueName, exclusive: true)
        try await channel.queueBind(queue: queueName, exchange: testExchange)

        try await channel.basicPublish(from: payload1, exchange: testExchange, routingKey: "")
        try await channel.basicPublish(from: payload2, exchange: testExchange, routingKey: "")
        let getResponse1 = try await channel.basicGet(queue: queueName, noAck: true)
        let getResponse2 = try await channel.basicGet(queue: queueName, noAck: true)

        XCTAssertEqual(getResponse1?.message.body, payload1)
        XCTAssertEqual(getResponse1?.messageCount, 1)
        XCTAssertEqual(getResponse2?.message.body, payload2)
        XCTAssertEqual(getResponse2?.messageCount, 0)
    }

    func testConsumeLargePayloads() async throws {
        let queueName = "lp-test-consume"
        let payload1 = ByteBuffer(megaBytes: 20, repeating: UInt8(ascii: "a"))
        let payload2 = ByteBuffer(megaBytes: 10, repeating: UInt8(ascii: "b"))

        try await channel.queueDeclare(name: queueName, exclusive: true)
        try await channel.queueBind(queue: queueName, exchange: testExchange)

        var messages = try await channel.basicConsume(queue: queueName, noAck: true).prefix(2).makeAsyncIterator()

        try await channel.basicPublish(from: payload1, exchange: testExchange, routingKey: "")
        try await channel.basicPublish(from: payload2, exchange: testExchange, routingKey: "")

        let received1 = try await messages.next()
        let received2 = try await messages.next()
        let received3 = try await messages.next()

        XCTAssertEqual(received1?.body, payload1)
        XCTAssertEqual(received2?.body, payload2)
        XCTAssertNil(received3)
    }

    func testReturnLargePayloads() async throws {
        let payload1 = ByteBuffer(megaBytes: 20, repeating: UInt8(ascii: "a"))
        let payload2 = ByteBuffer(megaBytes: 10, repeating: UInt8(ascii: "b"))

        try await channel.exchangeDeclare(name: "no-binding", type: "fanout")

        var returns = try await channel.returnConsume(named: "test").prefix(2).makeAsyncIterator()
        

        try await channel.basicPublish(from: payload1, exchange: "no-binding", routingKey: "", mandatory: true)
        try await channel.basicPublish(from: payload2, exchange: "no-binding", routingKey: "", mandatory: true)

        let received1 = try await returns.next()
        let received2 = try await returns.next()
        let received3 = try await returns.next()

        XCTAssertEqual(received1?.body, payload1)
        XCTAssertEqual(received2?.body, payload2)
        XCTAssertNil(received3)
    }
}

extension ByteBuffer {
    init(megaBytes: Int, repeating: UInt8 = .init(ascii: "a")) {
        self.init(repeating: repeating, count: megaBytes * 1024 * 1024)
    }
}
