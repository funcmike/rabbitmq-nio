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
import AMQPProtocol

public extension AMQPChannel {
    func close(reason: String = "", code: UInt16 = 200) async throws -> AMQPResponse {
        return try await self.close(reason: reason, code: code).get()
    }

    func basicGet(queue: String, noAck: Bool = true) async throws -> AMQPMessage.Get? {
        return try await self.basicGet(queue: queue, noAck: noAck).get()
    }

    func basicPublish(body: ByteBuffer, exchange: String, routingKey: String, mandatory: Bool = false,  immediate: Bool = false, properties: Properties = Properties()) async throws {
        return try await self.basicPublish(body: body, exchange: exchange, routingKey: routingKey, mandatory: mandatory, immediate: immediate, properties: properties).get()
    }

    func basicPublish(body: [UInt8], exchange: String, routingKey: String, mandatory: Bool = false,  immediate: Bool = false, properties: Properties = Properties()) async throws  {
        return try await self.basicPublish(body: body, exchange: exchange, routingKey: routingKey, mandatory: mandatory, immediate: immediate, properties: properties).get()
    }

    func queueDeclare(name: String, passive: Bool = false, durable: Bool = false, exclusive: Bool = false, autoDelete: Bool = false, args arguments: Table =  Table()) async throws -> AMQPResponse {
        return try await self.queueDeclare(name: name, passive: passive, durable: durable, exclusive: exclusive, autoDelete: autoDelete, args: arguments).get()
    }

    func queueDelete(name: String, ifUnused: Bool = false, ifEmpty: Bool = false) async throws -> AMQPResponse {
        return try await self.queueDelete(name: name, ifUnused: ifUnused, ifEmpty: ifEmpty).get()
    }

    func queuePurge(name: String) async throws -> AMQPResponse {
        return try await self.queuePurge(name: name).get()
    }

    func queueBind(queue: String, exchange: String, routingKey: String, args arguments: Table =  Table()) async throws -> AMQPResponse {
        return try await self.queueBind(queue: queue, exchange: exchange, routingKey: routingKey, args: arguments).get()
    }

    func queueUnbind(queue: String, exchange: String, routingKey: String, args arguments: Table =  Table()) async throws -> AMQPResponse {
        return try await self.queueUnbind(queue: queue, exchange: exchange, routingKey: routingKey, args: arguments).get()
    }

    func exchangeDeclare(name: String, type: String, passive: Bool = false, durable: Bool = true, autoDelete: Bool = false,  internal: Bool = false, args arguments: Table = Table()) async throws -> AMQPResponse {
        return try await self.exchangeDeclare(name: name, type: type, passive: passive, durable: durable, autoDelete: autoDelete,  internal: `internal`, args: arguments).get()
    }

    func exchangeDelete(name: String, ifUnused: Bool = false) async throws -> AMQPResponse {
        return try await self.exchangeDelete(name: name, ifUnused: ifUnused).get()
    }

    func exchangeBind(destination: String, source: String, routingKey: String, args arguments: Table = Table()) async throws -> AMQPResponse {
        return try await self.exchangeBind(destination: destination, source: source, routingKey: routingKey, args: arguments).get()
    }

    func exchangeUnbind(destination: String, source: String, routingKey: String, args arguments: Table = Table()) async throws -> AMQPResponse {
        return try await self.exchangeUnbind(destination: destination, source: source, routingKey: routingKey, args: arguments).get()
    }

    func basicRecover(requeue: Bool) async throws -> AMQPResponse {
        return try await self.basicRecover(requeue: requeue).get()
    }

    func confirmSelect() async throws -> AMQPResponse {
        return try await self.confirmSelect().get()
    }

    func txSelect() async throws -> AMQPResponse {
        return try await self.txSelect().get()
    }

    func txCommit() async throws -> AMQPResponse {
        return try await self.txCommit().get()
    }

    func txRollback() async throws -> AMQPResponse {
        return try await self.txRollback().get()
    }

    func basicQos(count: UInt16, global: Bool = false) async throws -> AMQPResponse {
        return try await self.basicQos(count: count, global: global).get()
    }

    func basicAck(deliveryTag: UInt64, multiple: Bool = false) async throws {
        return try await self.basicAck(deliveryTag: deliveryTag, multiple: multiple).get()
    }
    
    func basicAck(message: AMQPMessage.Delivery,  multiple: Bool = false) async throws {
        return try await self.basicAck(message: message, multiple: multiple).get()
    }

    func basicNack(deliveryTag: UInt64, multiple: Bool = false, requeue: Bool = false) async throws {
        return try await self.basicNack(deliveryTag: deliveryTag, multiple: multiple, requeue: requeue).get()
    }

    func basicNack(message: AMQPMessage.Delivery, multiple: Bool = false, requeue: Bool = false) async throws  {
        return try await self.basicNack(message: message, multiple: multiple, requeue: requeue).get()
    }

    func basicReject(deliveryTag: UInt64, requeue: Bool = false) async throws {
        return try await self.basicReject(deliveryTag: deliveryTag, requeue: requeue).get()
    }

    func basicReject(message: AMQPMessage.Delivery, requeue: Bool = false) async throws {
        return try await self.basicReject(message: message, requeue: requeue).get()
    }

    func basicConsume(queue: String, consumerTag: String = "", noAck: Bool = false, exclusive: Bool = false, args arguments: Table = Table(), listener: @escaping (Result<AMQPMessage.Delivery, Error>) -> Void) async throws -> AMQPResponse {
        return try await self.basicConsume(queue: queue, consumerTag: consumerTag, noAck: noAck, exclusive:exclusive, args: arguments, listener: listener).get()
    }

    func basicConsume(queue: String, consumerTag: String = "", noAck: Bool = false, exclusive: Bool = false, args arguments: Table = Table()) async throws -> AMQPListener {
        return try await self.basicConsume(queue: queue, consumerTag: consumerTag, noAck: noAck, exclusive: exclusive, args: arguments)
            .flatMapThrowing { response in
                    guard case .channel(let channel) = response, case .basic(let basic) = channel, case .consumed(let tag) = basic else {
                        throw ClientError.invalidResponse(response)
                    }

                    return .init(self, consumerTag: tag)
                }.get()  
    }

    func flow(active: Bool) async throws -> AMQPResponse { 
        return try await self.flow(active: active).get()
    }
}

public class AMQPListener: AsyncSequence {
    public typealias AsyncIterator = AsyncStream<Element>.AsyncIterator
    public typealias Element = Result<AMQPMessage.Delivery, Error>

    let channel: AMQPChannel
    let stream: AsyncStream<Element>
    let consumerTag: String

    init(_ channel: AMQPChannel, consumerTag: String) {
        self.channel = channel
        self.consumerTag = consumerTag    
        self.stream = AsyncStream { cont in
            do {
                try channel.addConsumeListener(consumerTag: consumerTag) { result in
                    cont.yield(result)
                }
            } catch {
                cont.finish()
                return
            }

            channel.addCloseListener(consumerTag: consumerTag) { _ in
                cont.finish()
            }
        }
    }

    deinit {
        self.channel.removeConsumeListener(consumerTag: self.consumerTag)
        self.channel.removeCloseListener(consumerTag: self.consumerTag)
    }

    public __consuming func makeAsyncIterator() -> AsyncStream<Element>.AsyncIterator {
        return self.stream.makeAsyncIterator()
    }
}
