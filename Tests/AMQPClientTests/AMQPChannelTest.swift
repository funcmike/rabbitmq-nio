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

        let deliveryTag = try await channel.basicPublish(from: body, exchange: "", routingKey: "test")

        XCTAssertEqual(deliveryTag, 0)

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

        let _ = try await channel.basicPublish(from: body, exchange: "", routingKey: "test", properties: properties)

        guard let msg = try await channel.basicGet(queue: "test") else {
            return  XCTFail()
        }

        XCTAssertEqual(msg.messageCount, 0)
        XCTAssertEqual(msg.message.body.getString(at: 0, length: msg.message.body.readableBytes), "{}")
        XCTAssertEqual(properties, msg.message.properties)

        let _ = try await channel.queueDelete(name: "test")

        let _ = try await channel.close()
    }

    func testBasicTx() async throws {
        let channel = try await client.openChannel(id: 1)

        guard case .channel(let ch) = try await channel.txSelect(), case .tx(let exchange) = ch, case .selected = exchange else {
            return  XCTFail() 
        }

        guard case .channel(let ch) = try await channel.txCommit(), case .tx(let exchange) = ch, case .committed = exchange else {
            return  XCTFail() 
        }

        guard case .channel(let ch) = try await channel.txRollback(), case .tx(let exchange) = ch, case .rollbacked = exchange else {
            return  XCTFail() 
        }

        let _ = try await channel.close()
    }

    func testConfirm() async throws {
        let channel = try await client.openChannel(id: 1)

        guard case .channel(let ch) = try await channel.confirmSelect(), case .confirm(let confirm) = ch, case .selected = confirm else {
            return  XCTFail() 
        }

        guard case .channel(let ch) = try await channel.confirmSelect(), case .confirm(let confirm) = ch, case .alreadySelected = confirm else {
            return  XCTFail() 
        }

        let _ = try await channel.close()
    }

    func testFlow() async throws {
        let channel = try await client.openChannel(id: 1)

        guard case .channel(let ch) = try await channel.flow(active: true), case .flowed(let active) = ch, active else {
            return  XCTFail() 
        }

        let _ = try await channel.close()
    }

    func testBasicQos() async throws {
        let channel = try await client.openChannel(id: 1)

        guard case .channel(let ch) = try await channel.basicQos(count: 100, global: true), case .basic(let basic) = ch, case.qosOk = basic else {
            return  XCTFail() 
        }

        guard case .channel(let ch) = try await channel.basicQos(count: 100, global: false), case .basic(let basic) = ch, case.qosOk = basic else {
            return  XCTFail() 
        }

        let _ = try await channel.close()
    }

    func testConsumeConfirms() async throws {
        let channel = try await client.openChannel(id: 1)

        let _ = try await channel.queueDeclare(name: "test", durable: true)

        let body = ByteBufferAllocator().buffer(string: "{}")

        for _ in  1...6 {
            let _ = try await channel.basicPublish(from: body, exchange: "", routingKey: "test", properties: .init())
        }

        do {
            guard let msg = try await channel.basicGet(queue: "test") else {
                return  XCTFail()
            }
    
            try await channel.basicAck(deliveryTag: msg.message.deliveryTag)

            guard let msg = try await channel.basicGet(queue: "test") else {
                return  XCTFail()
            }

            try await channel.basicAck(message: msg.message)
        }


        do {
            guard let msg = try await channel.basicGet(queue: "test") else {
                return  XCTFail()
            }
            
            try await channel.basicNack(deliveryTag: msg.message.deliveryTag)
    
            guard let msg = try await channel.basicGet(queue: "test") else {
                return  XCTFail()
            }

            try await channel.basicNack(message: msg.message)
        }

        do {
            guard let msg = try await channel.basicGet(queue: "test") else {
                return XCTFail()
            }

            try await channel.basicReject(deliveryTag: msg.message.deliveryTag)

            guard let msg = try await channel.basicGet(queue: "test") else {
                return  XCTFail()
            }

            try await channel.basicReject(message: msg.message)
        }

        guard case .channel(let ch) = try await channel.basicRecover(requeue: true), case .basic(let basic) = ch, case .recovered = basic else {
            return XCTFail() 
        }

        let _ = try await channel.queueDelete(name: "test")

        let _ = try await channel.close()
    }

    func testPublishConsume() async throws {
        let channel = try await client.openChannel(id: 1)

        let _ = try await channel.queueDeclare(name: "test_publish", durable: true)

        let body = ByteBufferAllocator().buffer(string: "{}")

        let _ = try await channel.confirmSelect()
        
        Task {
            for i in  1...100 {
                let deliveryTag = try await channel.basicPublish(from: body, exchange: "", routingKey: "test", properties: .init())
                
                XCTAssertEqual(UInt64(i), deliveryTag)
            }
        }

        let consumer = try await channel.publishConsume(named: "test")

        var count = 0
        for await msg in consumer {
            guard case .success = msg else {
                return XCTFail()
            }
            count += 1
            if count == 2 { break }
        }

        let _ = try await channel.queueDelete(name: "test_publish")

        let _ = try await channel.close()
    }

    func testBasicConsume() async throws {
        let channel = try await client.openChannel(id: 1)

        let _ = try await channel.queueDeclare(name: "test_consume", durable: true)

        let body = ByteBufferAllocator().buffer(string: "{}")
        
        for _ in  1...100 {
            let _ = try await channel.basicPublish(from: body, exchange: "", routingKey: "test_consume", properties: .init())
        }

        let consumer = try await channel.basicConsume(queue: "test_consume")

        var count = 0
        for await msg in consumer {
            guard case .success = msg else {
                return XCTFail()
            }
            count += 1
            if count == 100 { break }
        }

        guard case .channel(let ch) = try await channel.basicCancel(consumerTag: consumer.name), case .basic(let basic) = ch, case .canceled = basic else {
            return XCTFail() 
        }

        let _ = try await channel.queueDelete(name: "test_consume")
        let _ = try await channel.close()
    }
}
