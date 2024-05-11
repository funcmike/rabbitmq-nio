# RabbitMQNIO
[<img src="https://img.shields.io/badge/platform-macOS | Linux-brightgreen.svg" alt="Platform macOS | Linux" />](https://swift.org)
[<img src="https://img.shields.io/badge/swift-5.7-brightgreen.svg" alt="Swift 5.7" />](https://swift.org)


A Swift implementation of AMQP 0.9.1 protocol: decoder + encoder (AMQPProtocol) and non-blocking client (AMQPClient).

Heavy inspired by projects: amq-protocol (https://github.com/cloudamqp/amq-protocol.cr) and amqp-client (https://github.com/cloudamqp/amqp-client.cr).

Swift-NIO related code is based on other NIO projects like:
* https://github.com/vapor/postgres-nio
* https://github.com/swift-server-community/mqtt-nio
* https://gitlab.com/swift-server-community/RediStack

## State of the project

**!!! WARNING !!!** <br>
This project is in alpha stage and still under development - API can change in the near future before 1.0 release. <br>
Please do extensive tests of Your use case before using it on production! <br>
Nevertheless, current client operations are tested and appears to be stable so do not be afraid to use it. <br>
Please report bugs or missing features. <br>
**!!! WARNING !!!**

AMQPProtocol library currently should cover all of AMQP 0.9.1 specification.

AMQPClient library's architecture using NIO Channels is already done and all of AMQP operations (without WebSockets) should be supported.
Current work is focused on testing, finding bugs, API stabilization and code refactoring / polishing (based on Swift Server Side Community feedback).

## Basic usage

Create a connection and connect to the AMQP broker using connection string.
```swift
let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

var connection: AMQPConnection

do {
    connection = try await AMQPConnection.connect(use: eventLoopGroup.next(), from: .init(url: "amqp://guest:guest@localhost:5672/%2f"))

    print("Succesfully connected")
} catch {
    print("Error while connecting", error)
}
```

Create a connection and connect to the AMQP broker using configuration object.
```swift
let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

var connection: AMQPConnection

do {
    connection = try await AMQPConnection.connect(use: eventLoopGroup.next(), from: .init(connection: .plain, server: .init()))

    print("Succesfully connected")
} catch {
    print("Error while connecting", error)
}
```

Open a channel.
```swift
var channel: AMQPChannel
 
do {
    channel = try await connection.openChannel()

    print("Succesfully opened a channel")
} catch {
    print("Error while opening a channel", error)
}
```

Declare a queue.
```swift
do {
    try await channel.queueDeclare(name: "test", durable: false)

    print("Succesfully created queue")
} catch {
    print("Error while creating queue", error)
}
```

Publish a message to queue.
```swift
do {
    let deliveryTag = try await channel.basicPublish(
        from: ByteBuffer(string: "{}"),
        exchange: "",
        routingKey: "test"
    )

    print("Succesfully publish a message")
} catch {
    print("Error while publishing a message", error)
}
```

Consume a single message.
```swift
do {
    guard let msg = try await channel.basicGet(queue: "test") else {
        print("No message currently available")
        return
    }

    print("Succesfully consumed a message", msg)
} catch {
    print("Error while consuming a message", error)
}
```

Set a QOS limit to prevent memory overflow of consumer.
```swift
try await channel.basicQos(count: 1000)
```

Consume a multiple message as AsyncThrowingStream.
```swift
do {
    let consumer = try await channel.basicConsume(queue: "test")

    for try await msg in consumer {
        print("Succesfully consumed a message", msg)
        break
    }
} catch {
    print("Delivery failure", error)
}
```

Consumer will be automatically cancelled on deinitialization.
Can be also manually cancelled. 
```swift
try await channel.basicCancel(consumerTag: consumer.name)
```

Close a channel, connection.
```swift
do {
    try await channel.close()
    try await connection.close()

    print("Succesfully closed", msg)
} catch {
    print("Error while closing", error)
}
```

## Connection recovery patterns.

Handling broker closing channel or connection disconnects.
Connection to AMQP broker is sustained by answering to heartbeat messages, however on network problem or broker restart connection can be broken.
Broker can also close channel or connection on bad command or other error.
Currently RabbitMQNIO do not support any connection nor channel recovery / re-connect mechanism so clients have to handle it manually.
After connection is interrupted all of channels created by it and connection itself must be re-created manually.

Example recovery patterns.

Checking channel and connection state (safe channel pattern - wrap standard connection and channel in a class and reuse channel before for ex. any produce operation).
```swift
@available(macOS 12.0, *)
class SimpleSafeConnection {
    private let eventLoop: EventLoop
    private let config: AMQPConnectionConfiguration

    private var channel: AMQPChannel?
    private var connection: AMQPConnection?

    init(eventLoop: EventLoop, config: AMQPConnectionConfiguration) {
        self.eventLoop = eventLoop
        self.config = config
    }
    
    func reuseChannel() async throws -> AMQPChannel {
        guard let channel = self.channel, channel.isOpen else {
            if self.connection == nil || self.connection!.isConnected {
                self.connection = try await AMQPConnection.connect(use: self.eventLoop, from: self.config)
            }

            self.channel = try await connection!.openChannel()
            return self.channel!
        }
        return channel
    }
}

let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
let connection = SimpleSafeConnection(eventLoop: eventLoopGroup.next(), config: .init(connection: .plain, server: .init()))

while(true) {
    let deliveryTag = try await connection.reuseChannel().basicPublish(
        from: ByteBuffer(string: "{}"),
        exchange: "",
        routingKey: "test"
    )
}
```

Handling channel and connection close errors (simple retry pattern - re-create channel or connection when errors occurs).
```swift
let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
var connection =  try await AMQPConnection.connect(use: eventLoopGroup.next(), from: .init(connection: .plain, server: .init()))
var channel = try await connection.openChannel()

for _ in 0..<3 {
    do {
        let deliveryTag = try await channel.basicPublish(
            from: ByteBuffer(string: "{}"),
            exchange: "",
            routingKey: "test"
        )
        break
    } catch AMQPConnectionError.channelClosed {
        do {
            channel = try await connection.openChannel()
        } catch AMQPConnectionError.connectionClosed {
            connection = try await AMQPConnection.connect(use: eventLoopGroup.next(), from: .init(connection: .plain, server: .init()))
            channel = try await connection.openChannel()
        }
    } catch {
        print("Unknown problem", error)
    }
}
```

Above recovery patterns can be mixed together.
```swift
let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
let connection = SimpleSafeConnection(eventLoop: eventLoopGroup.next(), config: .init(connection: .plain, server: .init()))
var channel = try await connection.reuseChannel()

for _ in 0..<3 {
    do {
        let deliveryTag = try await channel.basicPublish(
            from: ByteBuffer(string: "{}"),
            exchange: "",
            routingKey: "test"
        )
        break
    } catch AMQPConnectionError.channelClosed {
        channel = try await connection.reuseChannel()
    } catch {
        print("Unknown problem", error)
    }
}
```
