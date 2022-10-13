//===----------------------------------------------------------------------===//
//
// This source file is part of the RabbitMQNIO project
//
// Copyright (c) 2022 Krzysztof Majk
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIO
import NIOPosix
import Foundation

@main
public struct rabbitmq_nio {
    public private(set) var text = "Hello, World!"

    public  static func main() async {
        print(rabbitmq_nio().text)
        await setupEventloop(arguments: CommandLine.arguments)
    }
}

func setupEventloop(arguments: [String]) async {
    let arg1 = arguments.dropFirst().first
    let arg2 = arguments.dropFirst(2).first


    let connectTarget: (host: String, port: Int)?
    switch (arg1, arg1.flatMap(Int.init), arg2.flatMap(Int.init)) {
    case (.some(let h), _ , .some(let p)):
        /* we got two arguments, let's interpret that as host and port */
        connectTarget = (host: h, port: p)
    default:
        connectTarget = nil
    }

    let client: AMQPClient

    if let target = connectTarget {
        client = AMQPClient(eventLoopGroupProvider: .createNew, config: .plain(Configuration.Server(host: target.0, port: target.1, user: "vxos", password: "vxos")))
    } else {
        client = AMQPClient(eventLoopGroupProvider: .createNew, config: .plain(Configuration.Server(user: "vxos", password: "vxos")))
    }

    defer {
        client.shutdown({error  in return ()})
    }

    let connected = try! await client.connect()
    print(connected)


    let channelResult = try! await client.openChannel(id: 1)

    let queueDeclare1 = try! await channelResult.queueDeclare(name: "delete", durable: true)
    print(queueDeclare1)

    let queueDelete1 = try! await channelResult.queueDelete(name: "delete")
    print(queueDelete1)

    let queueDeclare2 = try! await channelResult.queueDeclare(name: "test", durable: true)
    print(queueDeclare2)

    let queueBind = try! await channelResult.queueBind(queue: "test", exchange: "amq.topic", routingKey: "test")
    print(queueBind)

    let queueUnbind = try! await channelResult.queueUnbind(queue: "test", exchange: "amq.topic", routingKey: "test")
    print(queueUnbind)

    let queuePurge = try! await channelResult.queuePurge(name: "test")
    print(queuePurge)

    let test  = [UInt8](arrayLiteral: 65, 77, 81, 80, 0, 0, 9, 1)

    let startProduce = Date()

    for _ in 1 ... 100000  {
        try! await channelResult.basicPublish(body: test, exchange: "", routingKey: "test")
    }

    let stopProduce = Date()

    print(100000.0/startProduce.distance(to: stopProduce))


    let start = Date()
    for _ in 1 ... 100000  {
        do
        {
            let message = try await channelResult.basicGet(queue: "test")
            //print("got messge", message as Any)
        } catch {
            print("error", error)
        }
    }

    let stop = Date()

    print(100000.0/start.distance(to: stop))
    
    try! client.closeFuture()?.wait()
    print("Client closed")
}
