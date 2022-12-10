# RabbitMQNIO

A Swift implementation of AMQP 0.9.1 protocol: decoder + encoder (AMQPProtocol) and non-blocking client (AMQPClient).

Heavy inspired by projects: amq-protocol (https://github.com/cloudamqp/amq-protocol.cr) and amqp-client (https://github.com/cloudamqp/amqp-client.cr).

Swift-NIO related code is based on other NIO projects like:
* https://github.com/vapor/postgres-nio
* https://github.com/swift-server-community/mqtt-nio
* https://gitlab.com/swift-server-community/RediStack

## State of the project

**!!! WARNING !!!** <br>
This project is in very early stage and still under heavy development so everything can change in the near future. <br>
Please don't use it on production until first release! <br>
**!!! WARNING !!!**

AMQPProtocol library currently should cover all of AMQP 0.9.1 specification.

AMQPClient library's basic architecture using NIO Channels is already done and all of AMQP operations (without WebSockets) should be supported.
Current work is focused on testing, fixing bugs, documentation and benchmarking.

## Basic usage
Create a client and connect to the AMQP broker.
```swift
let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

var connection: AMQPConnection

do {
    connection = try await  AMQPConnection.connect(use: eventLoopGroup.next(), from: .plain(.init()))

    print("Succesfully connected")
} catch {
    print("Error while connecting", error)
}
```

Open a channel.
```swift
do {
    let channel = try await connection.openChannel()

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

Consume a multiple message as AsynStream.
```swift
let consumer = try await channel.basicConsume(queue: "test")

for await msg in consumer {
    guard case .success(let delivery) = msg else {
        print("Delivery failure", msg)
        return
    }

    print("Succesfully consumed a message", delivery)
    break
}

try await channel.basicCancel(consumerTag: consumer.name)
```

Close a channel, connection.
```swift
do {
    let _ = try await channel.close()
    try await connection.close()

    print("Succesfully closed", msg)
} catch {
    print("Error while closing", error)
}
```