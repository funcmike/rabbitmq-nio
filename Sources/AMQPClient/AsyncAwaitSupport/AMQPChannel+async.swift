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

    func basicPublish(from body: ByteBuffer, exchange: String, routingKey: String, mandatory: Bool = false,  immediate: Bool = false, properties: Properties = Properties()) async throws -> UInt64 {
        return try await self.basicPublish(from: body, exchange: exchange, routingKey: routingKey, mandatory: mandatory, immediate: immediate, properties: properties).get()
    }


    func publishConsume(named name: String) async throws -> AMQPListener<AMQPResponse.Channel.Basic.PublishConfirm> {
        return .init(self, named: name)
    }

    func returnConsume(named name: String) async throws -> AMQPListener<AMQPResponse.Channel.Message.Return> {
        return .init(self, named: name)
    }

    func basicGet(queue: String, noAck: Bool = false) async throws -> AMQPResponse.Channel.Message.Get? {
        return try await self.basicGet(queue: queue, noAck: noAck).get()
    }

    func basicConsume(queue: String, consumerTag: String = "", noAck: Bool = false, exclusive: Bool = false, args arguments: Table = Table(), listener: @escaping (Result<AMQPResponse.Channel.Message.Delivery, Error>) -> Void) async throws -> AMQPResponse.Channel.Basic.ConsumeOk {
        return try await self.basicConsume(queue: queue, consumerTag: consumerTag, noAck: noAck, exclusive:exclusive, args: arguments, listener: listener).get()
    }

    func basicConsume(queue: String, consumerTag: String = "", noAck: Bool = false, exclusive: Bool = false, args arguments: Table = Table()) async throws -> AMQPListener<AMQPResponse.Channel.Message.Delivery> {
        return try await self.basicConsume(queue: queue, consumerTag: consumerTag, noAck: noAck, exclusive: exclusive, args: arguments)
            .flatMapThrowing { response in
                return .init(self, named: response.consumerTag)
            }.get()
    }

    func cancel(consumerTag: String) async throws -> AMQPResponse { 
        return try await self.cancel(consumerTag: consumerTag).get()
    }

    func basicAck(deliveryTag: UInt64, multiple: Bool = false) async throws {
        return try await self.basicAck(deliveryTag: deliveryTag, multiple: multiple).get()
    }

    func basicAck(message: AMQPResponse.Channel.Message.Delivery,  multiple: Bool = false) async throws {
        return try await self.basicAck(message: message, multiple: multiple).get()
    }

    func basicNack(deliveryTag: UInt64, multiple: Bool = false, requeue: Bool = false) async throws {
        return try await self.basicNack(deliveryTag: deliveryTag, multiple: multiple, requeue: requeue).get()
    }

    func basicNack(message: AMQPResponse.Channel.Message.Delivery, multiple: Bool = false, requeue: Bool = false) async throws  {
        return try await self.basicNack(message: message, multiple: multiple, requeue: requeue).get()
    }

    func basicReject(deliveryTag: UInt64, requeue: Bool = false) async throws {
        return try await self.basicReject(deliveryTag: deliveryTag, requeue: requeue).get()
    }

    func basicReject(message: AMQPResponse.Channel.Message.Delivery, requeue: Bool = false) async throws {
        return try await self.basicReject(message: message, requeue: requeue).get()
    }

    func basicRecover(requeue: Bool) async throws -> AMQPResponse {
        return try await self.basicRecover(requeue: requeue).get()
    }

    func basicQos(count: UInt16, global: Bool = false) async throws -> AMQPResponse {
        return try await self.basicQos(count: count, global: global).get()
    }

    func flow(active: Bool) async throws -> AMQPResponse { 
        return try await self.flow(active: active).get()
    }

    func flowConsume(named name: String) async throws -> AMQPListener<Bool> {
        return .init(self, named: name)
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

    func exchangeDeclare(name: String, type: String, passive: Bool = false, durable: Bool = false, autoDelete: Bool = false,  internal: Bool = false, args arguments: Table = Table()) async throws -> AMQPResponse {
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
}

public final class AMQPListener<Value>: AsyncSequence {
    public typealias AsyncIterator = AsyncStream<Element>.AsyncIterator
    public typealias Element = Result<Value, Error>

    let channel: AMQPChannel
    let stream: AsyncStream<Element>
    public let name: String

    init(_ channel: AMQPChannel, named name: String) {
        self.channel = channel
        self.name = name
        self.stream = AsyncStream { cont in
            do {
                try channel.addListener(type: Value.self, named: name) { result in
                    guard case .success = result else {
                        cont.yield(result)
                        cont.finish()
                        return
                    }

                    cont.yield(result)
                }
            } catch {
                cont.finish()
                return
            }

            channel.addCloseListener(named: name) { _ in
                cont.finish()
            }
        }
    }

    deinit {
        self.channel.removeListener(type: Value.self, named: self.name)
        self.channel.removeCloseListener(named: self.name)
    }

    public __consuming func makeAsyncIterator() -> AsyncStream<Element>.AsyncIterator {
        return self.stream.makeAsyncIterator()
    }
}
