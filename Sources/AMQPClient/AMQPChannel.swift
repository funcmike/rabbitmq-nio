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
import NIOConcurrencyHelpers
import AMQPProtocol

public final class AMQPChannel {
    public let channelID: Frame.ChannelID
    private var eventLoopGroup: EventLoopGroup

    private var lock = NIOLock()
    private var _connection: AMQPConnection?
    var connection: AMQPConnection? {
        get {
            self.lock.withLock {
                _connection
            }
        }
        set {
            self.lock.withLock {
                _connection = newValue
            }
        }
    }

    init(channelID: Frame.ChannelID, eventLoopGroup: EventLoopGroup, connection: AMQPConnection, channelCloseFuture: EventLoopFuture<Void>) {
        self.channelID = channelID
        self.eventLoopGroup = eventLoopGroup
        self.connection = connection

        connection.closeFuture().whenComplete { result in
            if self.connection === connection {
                self.connection = nil
            }
        }

        channelCloseFuture.whenComplete { result in
                if self.connection === connection {
                self.connection = nil
            }
        }
    }

    public func basicGet(queue: String, noAck: Bool = true) -> EventLoopFuture<AMQPMessage.Get?> {
        guard let connection = self.connection else { return self.eventLoopGroup.next().makeFailedFuture(ClientError.connectionClosed()) }

        return connection.sendFrame(frame: .method(self.channelID, .basic(.get(.init(reserved1: 0, queue: queue, noAck: noAck)))), immediate: true)
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .message(let message) = channel, case .get(let get) = message else {
                    throw ClientError.invalidResponse(response)
                }
                return get
            }
    }


    public func basicPublish(body: ByteBuffer, exchange: String, routingKey: String, mandatory: Bool = false,  immediate: Bool = false, properties: Properties = Properties()) -> EventLoopFuture<Void> {
        guard let body = body.getBytes(at: 0, length: body.readableBytes) else { return self.eventLoopGroup.next().makeFailedFuture(ClientError.invalidBody) }

        return self.basicPublish(body: body, exchange: exchange, routingKey: routingKey, mandatory: mandatory,  immediate: immediate, properties: properties)
    }

    public func basicPublish(body: [UInt8], exchange: String, routingKey: String, mandatory: Bool = false,  immediate: Bool = false, properties: Properties = Properties()) -> EventLoopFuture<Void> {
        guard let connection = self.connection else { return self.eventLoopGroup.next().makeFailedFuture(ClientError.connectionClosed()) }

        let publish = Frame.method(self.channelID, .basic(.publish(.init(reserved1: 0, exchange: exchange, routingKey: routingKey, mandatory: mandatory, immediate: immediate))))
        let header = Frame.header(self.channelID, .init(classID: 60, weight: 0, bodySize: UInt64(body.count), properties: properties))
        let body = Frame.body(self.channelID, body: body)

        return connection.sendFrames(frames: [publish, header, body], immediate: true)
    }

    public func queueDeclare(name: String, passive: Bool = false, durable: Bool = false, exclusive: Bool = false, autoDelete: Bool = false, args arguments: Table =  Table()) -> EventLoopFuture<AMQPResponse>  {
        guard let connection = self.connection else { return self.eventLoopGroup.next().makeFailedFuture(ClientError.connectionClosed()) }

        return connection.sendFrame(frame: .method(self.channelID, .queue(.declare(.init(reserved1: 0, queueName: name, passive: passive, durable: durable, exclusive: exclusive, autoDelete: autoDelete, noWait: false, arguments: arguments)))), immediate: true)
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .queue(let queue) = channel, case .declared(let queueName, let messageCount, let consumerCount) = queue else {
                    throw ClientError.invalidResponse(response)
                }
                return .channel(.queue(.declared(queueName: queueName, messageCount: messageCount, consumerCount: consumerCount)))
            }        
    }

    public func queueBind(queue: String, exchange: String, routingKey: String, args arguments: Table = Table()) -> EventLoopFuture<AMQPResponse> {
        guard let connection = self.connection else { return self.eventLoopGroup.next().makeFailedFuture(ClientError.connectionClosed()) }

        return connection.sendFrame(frame: .method(self.channelID, .queue(.bind(.init(reserved1: 0, queueName: queue, exchangeName: exchange, routingKey: routingKey, noWait: false, arguments: arguments)))), immediate: true)
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .queue(let queue) = channel, case .binded = queue else {
                    throw ClientError.invalidResponse(response)
                }
                return .channel(.queue(.binded))
            }         
    }

    public func close(reason: String = "", code: UInt16 = 200) -> EventLoopFuture<Void> {
        guard let connection = self.connection else { return self.eventLoopGroup.next().makeFailedFuture(ClientError.connectionClosed()) }

        return connection.sendFrame(frame: .method(self.channelID, .channel(.close(.init(replyCode: code, replyText: reason, classID: 0, methodID: 0)))))
        .flatMapThrowing { response in
            guard case .channel(let channel) = response, case .closed = channel else {
                throw ClientError.invalidResponse(response)
            }
            ()
        }
    }

    public func ack(deliveryTag: UInt64, multiple: Bool = false) {
        return TODO("implement ack")
    }

    public func ack(message: AMQPMessage.Delivery,  multiple: Bool = false) {
        self.ack(deliveryTag: message.deliveryTag, multiple: multiple)
    }

    public func nack(deliveryTag: UInt64, multiple: Bool = false, requeue: Bool = false) {
        return TODO("implement nack")
    }

    public func nack(message: AMQPMessage.Delivery, multiple: Bool = false, requeue: Bool = false) {
        self.nack(deliveryTag: message.deliveryTag, multiple: multiple, requeue: requeue)
    }

    public func reject(deliveryTag: UInt64, requeue: Bool = false) {
        return TODO("implement reject")
    }

    public func reject(message: AMQPMessage.Delivery, requeue: Bool = false) {
        self.reject(deliveryTag: message.deliveryTag, requeue: requeue)
    }
}
