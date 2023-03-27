import XCTest
import NIOPosix
import AMQPClient
import AMQPProtocol
import NIO

@testable import AMQPClient

@available(macOS 12.0, *)
final class AMQPChannelTest: XCTestCase {
    var connection: AMQPConnection!

    override func setUp() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        connection = try await AMQPConnection.connect(use: eventLoopGroup.next(), from: .init(connection: .plain, server: .init()))
    }

    override func tearDown() async throws {
        try await self.connection?.close()
    }

    func testCanCloseChannel() async throws {
        let channel = try await connection.openChannel()

        try await channel.close()
    }

    func testQueue() async throws {
        let channel = try await connection.openChannel()

        try await channel.queueDeclare(name: "test", durable: false)

        try await channel.queueBind(queue: "test", exchange: "amq.topic", routingKey: "test")

        try await channel.queueUnbind(queue: "test", exchange: "amq.topic", routingKey: "test")

        try await channel.queuePurge(name: "test")

        try await channel.queueDelete(name: "test")

        try await channel.close()
    }

    func testExchange() async throws {
        let channel = try await connection.openChannel()

        try await channel.exchangeDeclare(name: "test1", type: "topic")

        try await channel.exchangeDeclare(name: "test2", type: "topic")

        try await channel.exchangeBind(destination: "test1", source: "test2", routingKey: "test")

        try await channel.exchangeUnbind(destination: "test1", source: "test2", routingKey: "test")

        try await channel.exchangeDelete(name: "test1")

        try await channel.exchangeDelete(name: "test2")

        try await channel.close()
    }

    func testBasicPublish() async throws {
        let channel = try await connection.openChannel()

        try await channel.queueDeclare(name: "test", durable: true)

        let body = ByteBufferAllocator().buffer(string: "{}")

        let result = try await channel.basicPublish(from: body, exchange: "", routingKey: "test")

        XCTAssertEqual(result.deliveryTag, 0)

        try await channel.queueDelete(name: "test")

        try await channel.close()
    }

    func testBasicGet() async throws {
        let channel = try await connection.openChannel()

        try await channel.queueDeclare(name: "test", durable: true)

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

        try await channel.queueDelete(name: "test")

        try await channel.close()
    }

    func testBasicTx() async throws {
        let channel = try await connection.openChannel()

        try await channel.txSelect()

        try await channel.txCommit()

        try await channel.txRollback()

        try await channel.close()
    }

    func testConfirm() async throws {
        let channel = try await connection.openChannel()

        try await channel.confirmSelect()

        try await channel.confirmSelect()

        try await channel.close()
    }

    func testFlow() async throws {
        let channel = try await connection.openChannel()

        try await channel.flow(active: true)

        try await channel.close()
    }

    func testBasicQos() async throws {
        let channel = try await connection.openChannel()

        try await channel.basicQos(count: 100, global: true)

        try await channel.basicQos(count: 100, global: false)

        try await channel.close()
    }

    func testConsumeConfirms() async throws {
        let channel = try await connection.openChannel()

        try await channel.queueDeclare(name: "test", durable: true)

        let body = ByteBufferAllocator().buffer(string: "{}")

        for _ in  1...6 {
            try await channel.basicPublish(from: body, exchange: "", routingKey: "test", properties: .init())
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

        try await channel.basicRecover(requeue: true)

        try await channel.queueDelete(name: "test")

        try await channel.close()
    }

    func testPublishConsume() async throws {
        let channel = try await connection.openChannel()

        try await channel.queueDeclare(name: "test_publish", durable: true)

        let body = ByteBufferAllocator().buffer(string: "{}")

        try await channel.confirmSelect()
        
        let publish = Task {
            for i in  1...100 {
                let result = try await channel.basicPublish(from: body, exchange: "", routingKey: "test", properties: .init())
                
                XCTAssertEqual(UInt64(i), result.deliveryTag)
            }
        }

        let consumer = try await channel.publishConsume(named: "test")

        var count = 0
        for try await _ in consumer {
            count += 1
            if count == 2 { break }
        }

        let _ = await publish.result

        try await channel.queueDelete(name: "test_publish")

        try await channel.close()
    }
    
    func testBasicConsumeAutoCancel() async throws {
        let channel = try await connection.openChannel()

        try await channel.queueDeclare(name: "test_consume", durable: true)

        let body = ByteBufferAllocator().buffer(string: "{}")
        
        for _ in  1...100 {
            try await channel.basicPublish(from: body, exchange: "", routingKey: "test_consume", properties: .init())
        }
        
        let consumerTag: String
        
        do {
            let consumer = try await channel.basicConsume(queue: "test_consume", noAck: true)
            consumerTag = consumer.name

            var count = 0
            for try await _ in consumer {
                count += 1
                if count == 100 {
                    break
                }
            }
            
            XCTAssertEqual(count, 100)
        }
        
        sleep(2) // wait for deinit

        do {
            try await channel.basicCancel(consumerTag: consumerTag)
            XCTFail()
        } catch let error as AMQPConnectionError  {
            guard case .consumerAlreadyCancelled = error else {
                return XCTFail()
            }
        }

        try await channel.queueDelete(name: "test_consume")
        try await channel.close()
    }

    func testBasicConsumeManualCancel() async throws {
        let channel = try await connection.openChannel()

        try await channel.queueDeclare(name: "test_consume", durable: true)

        let body = ByteBufferAllocator().buffer(string: "{}")
        
        for _ in  1...100 {
            try await channel.basicPublish(from: body, exchange: "", routingKey: "test_consume", properties: .init())
        }
        
        do {
            let consumer = try await channel.basicConsume(queue: "test_consume", noAck: true)

            var count = 0
            for try await _ in consumer {
                count += 1
                if count == 100 {
                    try await channel.basicCancel(consumerTag: consumer.name)
                }
            }
            
            XCTAssertEqual(count, 100)
        }

        try await channel.queueDelete(name: "test_consume")
        try await channel.close()
    }
    
    func testOpenChannelsConcurrencly() async throws {
        async let first = connection.openChannel()
        async let second = connection.openChannel()
        
        try await first.close()
        try await second.close()
    }
    
    func testConcurrentOperationsOnChannel() async throws {
        for run in 0...1000 {
            let queueName = "temp_queue_\(run)"
            
            let channel = try await connection.openChannel()
            try await channel.queueDeclare(name: queueName, durable: false, exclusive: true)
            
            //just a few parallel operations
            async let o1 = channel.basicConsume(queue: queueName)
            async let o2 = channel.basicPublish(from: ByteBuffer(string: "baz"), exchange: "", routingKey: queueName)
            async let o3 = channel.basicConsume(queue: queueName)
            async let o4 = channel.basicPublish(from: ByteBuffer(string: "baz"), exchange: "", routingKey: queueName)
            let tag = try await o1
            async let o5: () = channel.basicCancel(consumerTag: tag.name)
            
            _ = try await (o2, o3, o4, o5)
        }
    }
}
