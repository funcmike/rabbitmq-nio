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

    func exchangeDeclare(name: String, type: String, passive: Bool = false, durable: Bool = true, autoDelete: Bool = false,  internal: Bool = false, noWait: Bool = false, args arguments: Table = Table()) async throws -> AMQPResponse {
        return try await self.exchangeDeclare(name: name, type: type, passive: passive, durable: durable, autoDelete: autoDelete,  internal: `internal`, noWait: noWait, args: arguments).get()
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

    func close(reason: String = "", code: UInt16 = 200) async throws -> AMQPResponse {
        return try await self.close(reason: reason, code: code).get()
    }
}
