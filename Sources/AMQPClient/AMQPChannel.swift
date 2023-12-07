//===----------------------------------------------------------------------===//
//
// This source file is part of the RabbitMQNIO project
//
// Copyright (c) 2023 RabbitMQNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import AMQPProtocol
import Atomics
import NIOCore

public final class AMQPChannel: Sendable {
    public let ID: Frame.ChannelID
    public let eventLoop: EventLoop
    public var closeFuture: NIOCore.EventLoopFuture<Void> {
        return channel.closeFuture
    }

    public var isOpen: Bool {
        return channel.isOpen
    }

    typealias ChannelHandler = AMQPChannelHandler
    private let channel: AMQPChannelHandler

    private let isConfirmMode = ManagedAtomic(false)
    private let isTxMode = ManagedAtomic(false)
    private let deliveryTag = ManagedAtomic(UInt64(1))
    private let frameMax: UInt32

    init(channelID: Frame.ChannelID, eventLoop: EventLoop, channel: AMQPChannelHandler, frameMax: UInt32) {
        ID = channelID
        self.eventLoop = eventLoop
        self.channel = channel
        self.frameMax = frameMax
    }

    /// Close the channel
    /// - Parameters:
    ///     - reason: Any message - might be logged by the server.
    ///     - code: Any number - might be logged by the server.
    /// - Returns: EventLoopFuture waiting for close response.
    @discardableResult
    public func close(reason: String = "", code: UInt16 = 200) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.channel(.close(.init(replyCode: code, replyText: reason, classID: 0, methodID: 0)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case .closed = channel else {
                    throw AMQPConnectionError.invalidResponse(response)
                }

                return ()
            }
    }

    /// Publish a ByteBuffer message to exchange or queue.
    /// - Parameters:
    ///     - body: Message payload that can be read from ByteBuffer.
    ///     - exchange: Name of exchange on which the message is published. Can be empty.
    ///     - routingKey: Name of routingKey that will be attached to the message.
    ///         An exchange looks at the routingKey while deciding how the message has to be routed.
    ///         When exchange parameter is empty routingKey is used as queueName.
    ///     - mandatory: When a published message cannot be routed to any queue and mendatory is true, the message will be returned to publisher.
    ///         Returned message must be handled with returnListner or returnConsumer.
    ///         When a published message cannot be routed to any queue and mendatory is false, the message is discarded or republished to an alternate exchange, if any.
    ///     - immediate: When matching queue has a least one or more consumers and immediate is set to true, message is delivered to them immediately.
    ///         When mathing queue has zero active consumers and immediate is set to true, message is returned to publisher.
    ///         When mathing queue has zero active consumers and immediate is set to false, message will be delivered to the queue.
    ///     - properties: Additional message properties (check amqp documentation).
    /// - Returns: EventLoopFuture with deliveryTag waiting for message write to the broker.
    ///     DeliveryTag is 0 when channel is not in confirm mode.
    ///     DeliveryTag is > 0 (monotonically increasing) when channel is in confirm mode.
    @discardableResult
    public func basicPublish(
        from body: ByteBuffer,
        exchange: String,
        routingKey: String,
        mandatory: Bool = false,
        immediate: Bool = false,
        properties: Properties = Properties()
    ) -> EventLoopFuture<AMQPResponse.Channel.Basic.Published> {
        let basic = Frame.Method.basic(.publish(.init(reserved1: 0, exchange: exchange, routingKey: routingKey, mandatory: mandatory, immediate: immediate)))
        let classID = basic.kind.rawValue
        let publish = Frame.Payload.method(basic)

        let header = Frame.Payload.header(.init(classID: classID, weight: 0, bodySize: UInt64(body.readableBytes), properties: properties))

        let payloads: [Frame.Payload]

        if body.readableBytes <= frameMax {
            payloads = [publish, header, .body(body)]
        } else {
            var parts = [publish, header]
            var buffer = body

            while(buffer.readableBytes > 0) {
                guard let bytes = buffer.readBytes(length: frameMax < buffer.readableBytes ? Int(frameMax) : buffer.readableBytes) else {
                    preconditionFailure("invalid bytes read")
                }
                parts.append(.body(.init(bytes: bytes)))
            }

            payloads = parts
        }

        let result: EventLoopFuture<Void> = channel.send(payloads: payloads)
        return result.map {
            if self.isConfirmMode.load(ordering: .relaxed) {
                let count = self.deliveryTag.loadThenWrappingIncrement(ordering: .sequentiallyConsistent)
                return .init(deliveryTag: count)
            } else {
                return .init(deliveryTag: 0)
            }
        }
    }

    /// Get a single message from a queue.
    /// - Parameters:
    ///     - queue: Name of the queue.
    ///     - noAck: Controls whether message will be acked or nacked automatically (true) or manually (false).
    /// - Returns: EventLoopFuture with optional message when queue is not empty.
    public func basicGet(queue: String, noAck: Bool = false) -> EventLoopFuture<AMQPResponse.Channel.Message.Get?> {
        return channel.send(payload: .method(.basic(.get(.init(reserved1: 0, queue: queue, noAck: noAck)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .message(message) = channel, case let .get(get) = message else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return get
            }
    }

    /// Consume messages from a queue by sending them to registered consume listeners.
    /// - Parameters:
    ///     - queue: Name of the queue.
    ///     - consumerTag: Name of the consumer if empty will be generated by the broker.
    ///     - noAck: Controls whether message will be acked or nacked automatically (true) or manually (false).
    ///     - exclusive: Flag ensures that queue can only be consumed by single consumer.
    ///     - args: Additional arguments (check rabbitmq documentation).
    ///     - listener: Callback when Delivery arrives - automatically registered.
    /// - Returns: EventLoopFuture with response confirming that broker has accepted consume request.
    @discardableResult
    public func basicConsume(
        queue: String,
        consumerTag: String = "",
        noAck: Bool = false,
        exclusive: Bool = false,
        args arguments: Table = Table(),
        listener: @escaping @Sendable (Result<AMQPResponse.Channel.Message.Delivery, Error>
        ) -> Void
    ) -> EventLoopFuture<AMQPResponse.Channel.Basic.ConsumeOk> {
        return basicConsume(queue: queue, consumerTag: consumerTag, noAck: noAck, exclusive: exclusive, args: arguments)
            .flatMapThrowing { response in
                try self.addListener(type: ChannelHandler.Delivery.self, named: response.consumerTag, listener: listener)
                return response
            }
    }

    func basicConsume(
        queue: String,
        consumerTag: String = "",
        noAck: Bool = false,
        exclusive: Bool = false,
        args arguments: Table = Table()
    ) -> EventLoopFuture<AMQPResponse.Channel.Basic.ConsumeOk> {
        return channel.send(payload: .method(.basic(.consume(.init(reserved1: 0,
                                                                   queue: queue,
                                                                   consumerTag: consumerTag,
                                                                   noLocal: false,
                                                                   noAck: noAck,
                                                                   exclusive: exclusive,
                                                                   noWait: false,
                                                                   arguments: arguments)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .basic(basic) = channel, case let .consumeOk(consumeOk) = basic else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return consumeOk
            }
    }

    /// Cancel sending messages from server to consumer.
    /// - Parameters:
    ///     - consumerTag: Identifer of the consumer.
    /// - Returns: EventLoopFuture waiting for cancel response.
    public func basicCancel(consumerTag: String) -> EventLoopFuture<Void> {
        let found: Bool

        do {
            found = try channel.existsConsumeListener(named: consumerTag)
        } catch {
            return eventLoop.makeFailedFuture(error)
        }

        guard found else { return eventLoop.makeFailedFuture(AMQPConnectionError.consumerAlreadyCancelled) }

        return channel.send(payload: .method(.basic(.cancel(.init(consumerTag: consumerTag, noWait: false)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .basic(basic) = channel, case .canceled = basic else {
                    throw AMQPConnectionError.invalidResponse(response)
                }

                return ()
            }
    }

    func basicCancelNoWait(consumerTag: String) throws {
        guard try channel.existsConsumeListener(named: consumerTag) else { throw AMQPConnectionError.consumerAlreadyCancelled }

        try channel.sendNoWait(payload: .method(.basic(.cancel(.init(consumerTag: consumerTag, noWait: true)))))
    }

    /// Acknowledge a message.
    /// - Parameters:
    ///     - deliveryTag: Number (identifier) of the message..
    ///     - multiple: Controls whether only this message is acked (false) or additionally all other up to it (true).
    /// - Returns: EventLoopFuture that will be resolved when ack is sent.
    public func basicAck(deliveryTag: UInt64, multiple: Bool = false) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.basic(.ack(deliveryTag: deliveryTag, multiple: multiple))))
    }

    /// Acknowledge a message.
    /// - Parameters:
    ///     - message: Received message.
    ///     - multiple: Controls whether only this message is acked (false) or additionally all other up to it (true).
    /// - Returns: EventLoopFuture that will be resolved when ack is sent.
    public func basicAck(message: AMQPResponse.Channel.Message.Delivery, multiple: Bool = false) -> EventLoopFuture<Void> {
        return basicAck(deliveryTag: message.deliveryTag, multiple: multiple)
    }

    /// Reject a message.
    /// - Parameters:
    ///     - deliveryTag: Number (identifier) of the message.
    ///     - multiple: Controls whether only this message is rejected (false) or additionally all other up to it (true).
    ///     - requeue: Controls whether to requeue message after reject.
    /// - Returns: EventLoopFuture that will be resolved when nack is sent.
    public func basicNack(deliveryTag: UInt64, multiple: Bool = false, requeue: Bool = false) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.basic(.nack(.init(deliveryTag: deliveryTag, multiple: multiple, requeue: requeue)))))
    }

    /// Reject a message.
    /// - Parameters:
    ///     - message: Received Message.
    ///     - multiple: Controls whether only this message is rejected (false) or additionally all other up to it (true).
    ///     - requeue: Controls whether to requeue message after reject.
    /// - Returns: EventLoopFuture that will be resolved when nack is sent.
    public func basicNack(message: AMQPResponse.Channel.Message.Delivery, multiple: Bool = false, requeue: Bool = false) -> EventLoopFuture<Void> {
        return basicNack(deliveryTag: message.deliveryTag, multiple: multiple, requeue: requeue)
    }

    /// Reject a message.
    /// - Parameters:
    ///     - deliveryTag: Number ((identifier) of the message.
    ///     - requeue: Controls whether to requeue message after reject.
    /// - Returns: EventLoopFuture that will be resolved when reject is sent.
    public func basicReject(deliveryTag: UInt64, requeue: Bool = false) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.basic(.reject(deliveryTag: deliveryTag, requeue: requeue))))
    }

    /// Reject a message.
    /// - Parameters:
    ///     - message: Received Message.
    ///     - requeue: Controls whether to requeue message after reject.
    /// - Returns: EventLoopFuture that will be resolved when reject is sent.
    public func basicReject(message: AMQPResponse.Channel.Message.Delivery, requeue: Bool = false) -> EventLoopFuture<Void> {
        return basicReject(deliveryTag: message.deliveryTag, requeue: requeue)
    }

    /// Tell the broker what to do with all unacknowledge messages.
    /// Unacknowledged messages retrieved by `basicGet` are requeued regardless.
    /// - Parameters:
    ///     - requeue: Controls whether to requeue all messages after rejecting them.
    /// - Returns: EventLoopFuture waiting for recover response.
    public func basicRecover(requeue: Bool) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.basic(.recover(requeue: requeue))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .basic(basic) = channel, case .recovered = basic else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Set a prefetch limit when consuming messages.
    /// No more messages will be delivered to the consumer until one or more message have been acknowledged or rejected.
    /// - Parameters:
    ///     - count: Size of the limit.
    ///     - global: Whether the limit will be shared across all consumers on the channel.
    /// - Returns: EventLoopFuture waiting for qos response.
    public func basicQos(count: UInt16, global: Bool = false) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.basic(.qos(prefetchSize: 0, prefetchCount: count, global: global))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .basic(basic) = channel, case .qosOk = basic else {
                    throw AMQPConnectionError.invalidResponse(response)
                }

                return ()
            }
    }

    /// Send a flow message to broker to start or stop sending message to consumers.
    /// Warning: Not supported by all brokers.
    /// - Parameters:
    ///     - active: Flow enabled or disabled.
    /// - Returns: EventLoopFuture with response confirming that broker has accepted a flow request.
    @discardableResult
    public func flow(active: Bool) -> EventLoopFuture<AMQPResponse.Channel.Flowed> {
        return channel.send(payload: .method(.channel(.flow(active: active))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .flowed(flowed) = channel else {
                    throw AMQPConnectionError.invalidResponse(response)
                }

                return flowed
            }
    }

    /// Declare a queue.
    /// - Parameters:
    ///     - name: Name of the queue.
    ///     - passive: If enabled broker will raise exception if queue already exists.
    ///     - durable: If enabled creates a queue stored on disk otherwise transient.
    ///     - exclusive: If enabled queue will be deleted when the channel is closed.
    ///     - auto_delete: If enabled queue will be deleted when the last consumer has stopped consuming.
    ///     - arguments: Additional arguments (check rabbitmq documentation).
    /// - Returns: EventLoopFuture with response confirming that broker has accepted a request.
    @discardableResult
    public func queueDeclare(
        name: String,
        passive: Bool = false,
        durable: Bool = false,
        exclusive: Bool = false,
        autoDelete: Bool = false,
        args arguments: Table = Table()
    ) -> EventLoopFuture<AMQPResponse.Channel.Queue.Declared> {
        return channel.send(payload: .method(.queue(.declare(.init(reserved1: 0,
                                                                   queueName: name,
                                                                   passive: passive,
                                                                   durable: durable,
                                                                   exclusive: exclusive,
                                                                   autoDelete: autoDelete,
                                                                   noWait: false,
                                                                   arguments: arguments)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .queue(queue) = channel, case let .declared(declared) = queue else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return declared
            }
    }

    /// Delete a queue.
    /// - Parameters:
    ///     - name: Name of the queue.
    ///     - ifUnused: If enabled queue will be deleted only when there is no consumers subscribed to it.
    ///     - ifEmpty: If enabled queue will be deleted only when it's empty.
    /// - Returns: EventLoopFuture with response confirming that broker has accepted a delete request.
    @discardableResult
    public func queueDelete(name: String, ifUnused: Bool = false, ifEmpty: Bool = false) -> EventLoopFuture<AMQPResponse.Channel.Queue.Deleted> {
        return channel.send(payload: .method(.queue(.delete(.init(reserved1: 0, queueName: name, ifUnused: ifUnused, ifEmpty: ifEmpty, noWait: false)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .queue(queue) = channel, case let .deleted(deleted) = queue else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return deleted
            }
    }

    /// Delete all message from a queue.
    /// - Parameters:
    ///     - name: Name of the queue.
    /// - Returns: EventLoopFuture with response confirming that broker has accepted a delete request.
    @discardableResult
    public func queuePurge(name: String) -> EventLoopFuture<AMQPResponse.Channel.Queue.Purged> {
        return channel.send(payload: .method(.queue(.purge(.init(reserved1: 0, queueName: name, noWait: false)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .queue(queue) = channel, case let .purged(purged) = queue else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return purged
            }
    }

    /// Bind a queue to an exchange.
    /// - Parameters:
    ///     - queue: Name of the queue.
    ///     - exchange: Name of the exchange.
    ///     - routingKey: Bind only to messages matching routingKey.
    ///     - arguments: Bind only to message matching given options.
    /// - Returns: EventLoopFuture waiting for bind response.
    public func queueBind(queue: String, exchange: String, routingKey: String = "", args arguments: Table = Table()) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.queue(.bind(.init(reserved1: 0,
                                                                queueName: queue,
                                                                exchangeName: exchange,
                                                                routingKey: routingKey,
                                                                noWait: false,
                                                                arguments: arguments)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .queue(queue) = channel, case .binded = queue else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Unbind a queue from an exchange.
    /// - Parameters:
    ///     - queue: Name of the queue.
    ///     - exchange: Name of the exchange.
    ///     - routingKey: Unbind only from messages matching routingKey.
    ///     - arguments: Unbind only from messages matching given options.
    /// - Returns: EventLoopFuturewaiting for bind response unbind response.
    public func queueUnbind(queue: String, exchange: String, routingKey: String = "", args arguments: Table = Table()) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.queue(.unbind(.init(reserved1: 0,
                                                                  queueName: queue,
                                                                  exchangeName: exchange,
                                                                  routingKey: routingKey,
                                                                  arguments: arguments)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .queue(queue) = channel, case .unbinded = queue else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Declare an exchange.
    /// - Parameters:
    ///     - name: Name of the exchange.
    ///     - passive: If enabled broker will raise exception if exchange already exists.
    ///     - durable: If enabled creates a exchange stored on disk otherwise transient.
    ///     - auto_delete: If enabled exchange will be deleted when the last consumer has stopped consuming.
    ///     - internal: Whether the exchange cannot be directly published to client.
    ///     - arguments: Additional arguments (check rabbitmq documentation).
    /// - Returns: EventLoopFuture waiting for declare response.
    public func exchangeDeclare(
        name: String,
        type: String,
        passive: Bool = false,
        durable: Bool = false,
        autoDelete: Bool = false,
        internal: Bool = false,
        args arguments: Table = Table()
    ) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.exchange(.declare(.init(reserved1: 0,
                                                                      exchangeName: name,
                                                                      exchangeType: type,
                                                                      passive: passive,
                                                                      durable: durable,
                                                                      autoDelete: autoDelete,
                                                                      internal: `internal`,
                                                                      noWait: false,
                                                                      arguments: arguments)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .exchange(exchange) = channel, case .declared = exchange else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Delete an exchange.
    /// - Parameters:
    ///     - name: Name of the queue.
    ///     - ifUnused: If enabled exchange will be deleted only when it's not used.
    /// - Returns: EventLoopFuture waiting for delete response.
    public func exchangeDelete(name: String, ifUnused: Bool = false) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.exchange(.delete(.init(reserved1: 0, exchangeName: name, ifUnused: ifUnused, noWait: false)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .exchange(exchange) = channel, case .deleted = exchange else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Bind an exchange to another exchange.
    /// - Parameters:
    ///     - destination: Output exchange.
    ///     - source: Input exchange.
    ///     - routingKey: Bind only to messages matching routingKey.
    ///     - arguments: Bind only to messages matching given options.
    /// - Returns: EventLoopFuture waiting for bind response.
    public func exchangeBind(destination: String, source: String, routingKey: String, args arguments: Table = Table()) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.exchange(.bind(.init(reserved1: 0,
                                                                   destination: destination,
                                                                   source: source,
                                                                   routingKey: routingKey,
                                                                   noWait: false,
                                                                   arguments: arguments)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .exchange(exchange) = channel, case .binded = exchange else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Unbind an exchange from another exchange.
    /// - Parameters:
    ///     - destination: Output exchange.
    ///     - source: Input exchange.
    ///     - routingKey: Unbind only from messages matching routingKey.
    ///     - arguments: Unbind only from messages matching given options.
    /// - Returns: EventLoopFuture waiting for unbind response.
    public func exchangeUnbind(destination: String, source: String, routingKey: String, args arguments: Table = Table()) -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.exchange(.unbind(.init(reserved1: 0,
                                                                     destination: destination,
                                                                     source: source,
                                                                     routingKey: routingKey,
                                                                     noWait: false,
                                                                     arguments: arguments)))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .exchange(exchange) = channel, case .unbinded = exchange else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Set channel in publish confirm mode, each published message will be acked or nacked.
    /// - Returns: EventLoopFuture waiting for confirm select response.
    public func confirmSelect() -> EventLoopFuture<Void> {
        guard !isConfirmMode.load(ordering: .relaxed) else {
            return eventLoop.makeSucceededFuture(())
        }

        return channel.send(payload: .method(.confirm(.select(noWait: false))))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .confirm(confirm) = channel, case .selected = confirm else {
                    throw AMQPConnectionError.invalidResponse(response)
                }

                self.isConfirmMode.store(true, ordering: .relaxed)

                return ()
            }
    }

    /// Set channel in transaction mode.
    /// - Returns: EventLoopFuture waiting for tx select response.
    public func txSelect() -> EventLoopFuture<Void> {
        guard !isTxMode.load(ordering: .relaxed) else {
            return eventLoop.makeSucceededFuture(())
        }

        return channel.send(payload: .method(.tx(.select)))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .tx(tx) = channel, case .selected = tx else {
                    throw AMQPConnectionError.invalidResponse(response)
                }

                self.isTxMode.store(true, ordering: .relaxed)

                return ()
            }
    }

    /// Commit a transaction.
    /// - Returns: EventLoopFuture waiting for commit response.
    public func txCommit() -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.tx(.commit)))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .tx(tx) = channel, case .committed = tx else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Rollback a transaction.
    /// - Returns: EventLoopFuture waiting for rollback response.
    public func txRollback() -> EventLoopFuture<Void> {
        return channel.send(payload: .method(.tx(.rollback)))
            .flatMapThrowing { response in
                guard case let .channel(channel) = response, case let .tx(tx) = channel, case .rollbacked = tx else {
                    throw AMQPConnectionError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Add channel close listener.
    /// - Parameters:
    ///     - name: Identifier of listener.
    ///     - listener: Callback when channel is closed.
    public func addCloseListener(named name: String, listener: @escaping @Sendable (Result<Void, Error>) -> Void) throws {
        return try addListener(type: ChannelHandler.Close.self, named: name, listener: listener)
    }

    /// Remove channel close listener.
    /// - Parameters:
    ///     - name: Identifier of listener.
    public func removeCloseListener(named name: String) {
        return removeListener(type: ChannelHandler.Close.self, named: name)
    }

    /// Add publish confirmation message listener.
    /// When channel is in confirm mode broker sends whether published message was accepted.
    /// - Parameters:
    ///     - name: Identifier of listener.
    ///     - listener: Callback when publish confirmation message is received.
    public func addPublishListener(
        named name: String,
        listener: @escaping @Sendable (Result<AMQPResponse.Channel.Basic.PublishConfirm, Error>) -> Void
    ) throws {
        guard isConfirmMode.load(ordering: .relaxed) else {
            throw AMQPConnectionError.channelNotInConfirmMode
        }

        return try addListener(type: ChannelHandler.Publish.self, named: name, listener: listener)
    }

    /// Remove publish confirm message listener.
    /// - Parameters:
    ///     - name: Identifier of consumer.
    public func removePublishListener(named name: String) {
        return removeListener(type: ChannelHandler.Publish.self, named: name)
    }

    /// Add return message listener.
    /// When broker cannot route message to any queue it sends a return message.
    /// - Parameters:
    ///     - name: Identifier of listener.
    ///     - listener: Callback when return message is received.
    public func addReturnListener(named name: String, listener: @escaping @Sendable (Result<AMQPResponse.Channel.Message.Return, Error>) -> Void) throws {
        return try addListener(type: ChannelHandler.Return.self, named: name, listener: listener)
    }

    /// Remove return message listener.
    /// - Parameters:
    ///     - name: Identifier of listener.
    public func removeReturnListener(named name: String) {
        return removeListener(type: ChannelHandler.Return.self, named: name)
    }

    /// Add flow message listener.
    /// When broker cannot keep up with amount of published messages it sends a flow (false) message.
    /// When broker is again ready to handle new messages it sends a flow (true) message.
    /// - Parameters:
    ///     - name: Identifier of listener.
    ///     - listener: Callback when flow message is received.
    public func addFlowListener(named name: String, listener: @escaping @Sendable (Result<Bool, Error>) -> Void) throws {
        return try addListener(type: ChannelHandler.Flow.self, named: name, listener: listener)
    }

    /// Remove flow listener.
    /// - Parameters:
    ///     - name: Identifier of listener.
    public func removeFlowListener(named name: String) {
        return removeListener(type: ChannelHandler.Flow.self, named: name)
    }

    func addListener<Value>(type: Value.Type, named name: String, listener: @escaping @Sendable (Result<Value, Error>) -> Void) throws {
        return try channel.addListener(type: type, named: name, listener: listener)
    }

    func removeListener<Value>(type: Value.Type, named name: String) {
        return channel.removeListener(type: type, named: name)
    }

    deinit {
        if self.channel.isOpen {
            assertionFailure("close() was not called before deinit!")
        }
    }
}

// can be removed the once Sendable is properly added to swift-atomics
extension ManagedAtomic: @unchecked Sendable where Value: Sendable {}
