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
import AMQPClient
@main
public struct TestCLI {
    public private(set) var text = "Hello, World!"

    public  static func main() async {
        print(TestCLI().text)
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

    let echangeDeclare1 = try! await channelResult.exchangeDeclare(name: "test1", type: "topic")
    print(echangeDeclare1)

    let echangeDeclare2 = try! await channelResult.exchangeDeclare(name: "test2", type: "topic")
    print(echangeDeclare2)

    let exchangeBind = try! await channelResult.exchangeBind(destination: "test1", source: "test2", routingKey: "test")
    print(exchangeBind)

    let exchangeUnbind = try! await channelResult.exchangeUnbind(destination: "test1", source: "test2", routingKey: "test")
    print(exchangeUnbind)

    let exchangeDelete1 = try! await channelResult.exchangeDelete(name: "test1")
    print(exchangeDelete1)

    let exchangeDelete2 = try! await channelResult.exchangeDelete(name: "test2")
    print(exchangeDelete2)

    let recover = try! await channelResult.basicRecover(requeue: true)
    print(recover)

    let test  = [UInt8](arrayLiteral: 65, 77, 81, 80, 0, 0, 9, 1)

    let startProduce = Date()

    for _ in 1 ... 1000  {
        try! await channelResult.basicPublish(body: test, exchange: "", routingKey: "test")
    }

    let stopProduce = Date()
    print(1000.0/startProduce.distance(to: stopProduce))


    //let start = Date()
    // for _ in 1 ... 10 + 2  {
    //     do
    //     {
    //         let _ = try await channelResult.basicGet(queue: "test")
    //         //print("got messge", message as Any)
    //     } catch {
    //         print("error", error)
    //     }
    // }
    // let stop = Date()
    // print(10.0/start.distance(to: stop))
    // let basicConsume = try! await channelResult.basicConsume(queue: "test", listener: { result in 
    //     print(result)
    // })

    // print(basicConsume)

    // let flow1 = try! await channelResult.flow(active: false)
    // print(flow1)

    // let flow2 = try! await channelResult.flow(active: true)
    // print(flow2)


    let start = Date()

    var i = 0
    let consumer = try! await channelResult.basicConsume(queue: "test")
    for await _ in consumer {
        //print(result)
        i += 1
        if i == 1000 {
            let stop = Date()
            print("finished", 1000.0/start.distance(to: stop))
            let cancel = try! await channelResult.cancel(consumerTag: consumer.name)
            print(cancel)
        }
    }



    // let confirmSelect1 = try! await channelResult.confirmSelect()
    // print(confirmSelect1)

    // let confirmSelect2 = try! await channelResult.confirmSelect()
    // print(confirmSelect2)

    // let basicQos = try! await channelResult.basicQos(count: 1000)
    // print(basicQos)

    // let txSelect = try! await channelResult.txSelect()
    // print(txSelect)

    // let txCommit = try! await channelResult.txCommit()
    // print(txCommit)

    // let txRollback = try! await channelResult.txRollback()
    // print(txRollback)
    
    // let channelClose = try! await channelResult.close()
    // print(channelClose)

    // let clientClose = try! await client.close()
    // print(clientClose)
    
    let flow = try! await channelResult.flow(active: true)
    print(flow)

    try! client.closeFuture?.wait()
    print("Client closed")
}
