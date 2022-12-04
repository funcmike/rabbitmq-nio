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

import NIOCore
import NIOConcurrencyHelpers
import Atomics
import AMQPProtocol

public final class AMQPChannel {
    // Channel identificator.
    public let ID: Frame.ChannelID
    public var eventLoop: EventLoop

    private var lock = NIOLock()

    private var _channel: AMQPChannelHandler<AMQPFrameHandler>?
    private var channel: AMQPChannelHandler<AMQPFrameHandler>? {
        get {
            self.lock.withLock {
                _channel
            }
        }
        set {
            self.lock.withLock {
                _channel = newValue
            }
        }
    }

    private var closeListeners = AMQPListeners<Void>()

    private let isConfirmMode = ManagedAtomic(false)
    private let isTxMode = ManagedAtomic(false)
    private let deliveryTag = ManagedAtomic(UInt64(1))

    init(channelID: Frame.ChannelID, eventLoop: EventLoop, channel: AMQPChannelHandler<AMQPFrameHandler>) {
        self.ID = channelID
        self.eventLoop = eventLoop
        self.channel  = channel

        channel.closeFuture.whenComplete { result in
            self.channel = nil
            
            self.closeListeners.notify(result)
        }
    }

    /// Close the channel
    /// - Parameters:
    ///     - reason: any message - might be logged by the server.
    ///     - code: any number - might be logged by the server.
    /// - Returns: EventLoopFuture waiting for close response.
    @discardableResult
    public func close(reason: String = "", code: UInt16 = 200) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.channel(.close(.init(replyCode: code, replyText: reason, classID: 0, methodID: 0)))))
            .flatMapThrowing { response in
                guard case .channel(let channel) = response, case .closed = channel else {
                    throw AMQPClientError.invalidResponse(response)
                }

                return ()
            }
    }

    /// Publish a ByteBuffer message to exchange or queue.
    /// - Parameters:
    ///     - body: Message payload that can be read from ByteBuffer.
    ///     - exchange: Name of exchange on which the message is published. Can be empty.
    ///     - routingKey: Name of routingKey that will be attached to the message.
    ///                   An exchange looks at the routingKey while deciding how the message has to be routed.
    ///                   When exchange parameter is empty routingKey is used as queueName.
    ///     - mandatory: When a published message cannot be routed to any queue and mendatory is true, the message will be returned to publisher.
    //                      Returned message must be handled with returnListner or returnConsumer.
    ///                  When a published message cannot be routed to any queue and mendatory is false, the message is discarded or republished to an alternate exchange, if any.
    ///     - immediate: When matching queue has a least one or more consumers and immediate is set to true, message is delivered to them immediately.
    ///                  When mathing queue has zero active consumers and immediate is set to true, message is returned to publisher.
    ///                  When mathing queue has zero active consumers and immediate is set to false, message will be delivered to the queue.
    ///     - properties: Additional Message properties.
    /// - Returns: EventLoopFuture with deliveryTag waiting for message write to the server.
    ///     DeliveryTag is 0 when channel is not in confirm mode.
    ///     DeliveryTag is > 0 (monotonically increasing) when channel is in confirm mode.
    @discardableResult
    public func basicPublish(from body: ByteBuffer, exchange: String, routingKey: String, mandatory: Bool = false,  immediate: Bool = false, properties: Properties = Properties()) -> EventLoopFuture<AMQPResponse.Channel.Basic.Published> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        let basic = Frame.Method.basic(.publish(.init(reserved1: 0, exchange: exchange, routingKey: routingKey, mandatory: mandatory, immediate: immediate)))
        let classID = basic.kind.rawValue
        let publish = Frame.Payload.method(basic)

        let header =  Frame.Payload.header(.init(classID: classID, weight: 0, bodySize: UInt64(body.readableBytes), properties: properties))

        let result: EventLoopFuture<Void> = channel.send(payloads: [publish, header, .body(body)])
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
    ///     - queue: name of the queue.
    ///     - noAck: controls whether message will be acked or nacked automatically (true) or manually (false)
    /// - Returns: EventLoopFuture with optional Message when queue is not empty.
    public func basicGet(queue: String, noAck: Bool = false) -> EventLoopFuture<AMQPResponse.Channel.Message.Get?> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.basic(.get(.init(reserved1: 0, queue: queue, noAck: noAck)))))
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .message(let message) = channel, case .get(let get) = message else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return get
            }
    }

    // Consumes message from a queue by sending them to registered consumeListeners.
    /// - Parameters:
    ///     - queue: name of the queue.
    ///     - consumerTag: name of consumer if emtpy will generated by broker.
    ///     - noAck: controls whether message will be acked or nacked automatically (true) or manually (false).
    ///     - exclusive: flag ensures that only a single consumer receives messages from the queue at the time.
    ///     - arguments: Additional arguments (check rabbitmq documentation).
    /// - Returns: EventLoopFuture with consumerTag confirming that broker has accepted a new consumer.
    public func basicConsume(queue: String, consumerTag: String = "", noAck: Bool = false, exclusive: Bool = false, args arguments: Table = Table()) -> EventLoopFuture<AMQPResponse.Channel.Basic.ConsumeOk> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.basic(.consume(.init(
                reserved1: 0, queue: queue, consumerTag: consumerTag, noLocal: false, noAck: noAck, exclusive: exclusive, noWait: false, arguments: arguments)))))
                .flatMapThrowing { response in
                guard case .channel(let channel) = response, case .basic(let basic) = channel, case .consumeOk(let consumeOk) = basic else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return consumeOk
            }
    }

    // Consume messages from a queue by sending them to registered consume listeners.
    /// - Parameters:
    ///     - queue: name of the queue.
    ///     - consumerTag: name of the consumer if empty will generated by broker.
    ///     - noAck: controls whether message will be acked or nacked automatically (true) or manually (false).
    ///     - exclusive: flag ensures that only a single consumer receives messages from the queue at the time.
    ///     - args: Additional arguments (check rabbitmq documentation).
    ///     - listener: callback when Delivery arrives - automatically registered.
    /// - Returns: EventLoopFuture with response confirming that broker has accepted a request.
    public func basicConsume(queue: String, consumerTag: String = "", noAck: Bool = false, exclusive: Bool = false, args arguments: Table = Table(), listener: @escaping (Result<AMQPResponse.Channel.Message.Delivery, Error>) -> Void) -> EventLoopFuture<AMQPResponse.Channel.Basic.ConsumeOk> {
        let response: EventLoopFuture<AMQPResponse.Channel.Basic.ConsumeOk> = self.basicConsume(queue: queue, consumerTag: consumerTag, noAck: noAck, exclusive: exclusive, args: arguments)
        return response
            .flatMapThrowing { response in
                try self.addConsumeListener(consumerTag: response.consumerTag, listener: listener)
                return response
            }
    }

    /// Cancel sending messages from server to consumer.
    /// - Parameters:
    ///     - consumerTag: name of the consumer.
    /// - Returns: EventLoopFuture waiting for cancel response.
    public func basicCancel(consumerTag: String) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.basic(.cancel(.init(consumerTag: consumerTag, noWait: false)))))
            .flatMapThrowing { response in
                guard case .channel(let channel) = response, case .basic(let basic) = channel, case .canceled = basic else {
                    throw AMQPClientError.invalidResponse(response)
                }

                return ()
            }
    }

    /// Acknowledge a message.
    /// - Parameters:
    ///     - deliveryTag: number of the message.
    ///     - multiple: controls whether only this message is acked (false) or additionally all other up to it (true).
    /// - Returns: EventLoopFuture that is resolved when ack is sent.
    public func basicAck(deliveryTag: UInt64, multiple: Bool = false) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.basic(.ack(deliveryTag: deliveryTag, multiple: multiple))))
    }

    /// Acknowledge a message.
    /// - Parameters:
    ///     - message: received Message.
    ///     - multiple: controls whether only this message is acked (false) or additionally all other up to it (true).
    /// - Returns: EventLoopFuture that is resolved when ack is sent.
    public func basicAck(message: AMQPResponse.Channel.Message.Delivery,  multiple: Bool = false) -> EventLoopFuture<Void> {
        return self.basicAck(deliveryTag: message.deliveryTag, multiple: multiple)
    }

    /// Reject a message.
    /// - Parameters:
    ///     - deliveryTag: number of the message.
    ///     - multiple: controls whether only this message is rejected (false) or additionally all other up to it (true).
    ///     - requeue: controls whether to requeue message after reject.
    /// - Returns: EventLoopFuture that is resolved when nack is sent.
    public func basicNack(deliveryTag: UInt64, multiple: Bool = false, requeue: Bool = false) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.basic(.nack(.init(deliveryTag: deliveryTag, multiple: multiple, requeue: requeue)))))
    }

    /// Reject a message.
    /// - Parameters:
    ///     - message: received Message.
    ///     - multiple: controls whether only this message is rejected (false) or additionally all other up to it (true).
    ///     - requeue: controls whether to requeue message after reject.
    /// - Returns: EventLoopFuture that is resolved when nack is sent.
    public func basicNack(message: AMQPResponse.Channel.Message.Delivery, multiple: Bool = false, requeue: Bool = false) -> EventLoopFuture<Void> {
        return self.basicNack(deliveryTag: message.deliveryTag, multiple: multiple, requeue: requeue)
    }

    /// Reject a message.
    /// - Parameters:
    ///     - deliveryTag: number of the message.
    ///     - requeue: controls whether to requeue message after reject.
    /// - Returns: EventLoopFuture that is resolved when nack is sent.
    public func basicReject(deliveryTag: UInt64, requeue: Bool = false) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.basic(.reject(deliveryTag: deliveryTag, requeue: requeue))))
    }

    /// Reject a message.
    /// - Parameters:
    ///     - message: received Message.
    ///     - requeue: controls whether to requeue message after reject.
    /// - Returns: EventLoopFuture that is resolved when nack is sent.
    public func basicReject(message: AMQPResponse.Channel.Message.Delivery, requeue: Bool = false) -> EventLoopFuture<Void> {
        return self.basicReject(deliveryTag: message.deliveryTag, requeue: requeue)
    }


    /// Tell the broker what to do with all unacknowledge messages.
    /// Unacknowledged messages retrived by `basicGet` are requeued regardless.
    /// - Parameters:
    ///     - requeue: controls whether to requeue all messages after rejecting them.
    /// - Returns: EventLoopFuture waiting for recover response.
    public func basicRecover(requeue: Bool) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.basic(.recover(requeue: requeue))))
            .flatMapThrowing { response in
                guard case .channel(let channel) = response, case .basic(let basic) = channel, case .recovered = basic else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Set a prefetch limit when consuming messages.
    /// No more messages will be delivered to the consumer until one or more message have been acknowledged or rejected.
    /// - Parameters:
    ///     - count: size of the limit.
    ///     - global: whether the limit will be shared across all consumers on the channel.
    /// - Returns: EventLoopFuture waiting for qos response.
    public func basicQos(count: UInt16, global: Bool = false) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.basic(.qos(prefetchSize: 0, prefetchCount: count, global: global))))
            .flatMapThrowing { response in
                guard case .channel(let channel) = response, case .basic(let basic) = channel, case .qosOk = basic else {
                    throw AMQPClientError.invalidResponse(response)
                }

                return ()
            }
    }

    /// Send a flow message to broker to start or stop sending message to consumers.
    /// Not supported by all brokers.
    /// - Parameters:
    ///     - active: flow enabled or disabled.
    /// - Returns: EventLoopFuture with response confirming that broker has accepted a request.
    @discardableResult
    public func flow(active: Bool) -> EventLoopFuture<AMQPResponse.Channel.Flowed> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.channel(.flow(active: active))))
            .flatMapThrowing { response in
                guard case .channel(let channel) = response, case .flowed(let flowed) = channel else {
                    throw AMQPClientError.invalidResponse(response)
                }

                return flowed
            }
    }

    /// Declare a queue.
    /// - Parameters:
    ///     - name: Name of the queue.
    ///     - passive: If enabled broker will raise exception if queue already exists.
    ///     - durable: if enabled creates a queue stored on disk otherwise transient.
    ///     - exclusive: if enabled queue will be deleted when the channel is closed.
    ///     - auto_delete: if enabled queue will be deleted when the last consumer has stopped consuming.
    ///     - arguments: Additional arguments (check rabbitmq documentation).
    /// - Returns: EventLoopFuture with response confirming that broker has accepted a request.
    @discardableResult
    public func queueDeclare(name: String, passive: Bool = false, durable: Bool = false, exclusive: Bool = false, autoDelete: Bool = false, args arguments: Table =  Table()) -> EventLoopFuture<AMQPResponse.Channel.Queue.Declared>  {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.queue(.declare(.init(
                reserved1: 0, queueName: name, passive: passive, durable: durable, exclusive: exclusive, autoDelete: autoDelete, noWait: false, arguments: arguments)))))
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .queue(let queue) = channel, case .declared(let declared) = queue else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return declared
            }
    }

    /// Deletes a queue.
    /// - Parameters:
    ///     - name: Name of the queue.
    ///     - ifUnused: If enabled queue will be deleted only when there is no consumers subscribed to it.
    ///     - ifEmpty: if enabled queue will be deleted only when it's empty.
    /// - Returns: EventLoopFuture with response confirming that broker has accepted a request.
    @discardableResult
    public func queueDelete(name: String, ifUnused: Bool = false, ifEmpty: Bool = false) -> EventLoopFuture<AMQPResponse.Channel.Queue.Deleted> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.queue(.delete(.init(reserved1: 0, queueName: name, ifUnused: ifUnused, ifEmpty: ifEmpty, noWait: false)))))
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .queue(let queue) = channel, case .deleted(let deleted) = queue else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return deleted
            }
    }

    /// Deletes all message from a queue.
    /// - Parameters:
    ///     - name: Name of the queue.
    /// - Returns: EventLoopFuture with response confirming that broker has accepted a request.
    @discardableResult
    public func queuePurge(name: String) -> EventLoopFuture<AMQPResponse.Channel.Queue.Purged> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.queue(.purge(.init(reserved1: 0, queueName: name, noWait: false)))))
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .queue(let queue) = channel, case .purged(let purged) = queue else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return purged
            }
    }

    /// Bind queue to an exchange.
    /// - Parameters:
    ///     - queue: Name of the queue.
    ///     - exchange: Name of the exchange.
    ///     - routingKey: Bind only to messages matching routingKey.
    ///     - arguments: Bind only to message matching given options.
    /// - Returns: EventLoopFuture waiting for bind response.
    public func queueBind(queue: String, exchange: String, routingKey: String = "", args arguments: Table = Table()) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.queue(.bind(.init(
                reserved1: 0, queueName: queue, exchangeName: exchange, routingKey: routingKey, noWait: false, arguments: arguments)))))
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .queue(let queue) = channel, case .binded = queue else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Unbind queue from an exchange.
    /// - Parameters:
    ///     - queue: Name of the queue.
    ///     - exchange: Name of the exchange.
    ///     - routingKey: Unbind only from messages matching routingKey.
    ///     - arguments: Unbind only from messages matching given options.
    /// - Returns: EventLoopFuturewaiting for bind response unbind response.
    public func queueUnbind(queue: String, exchange: String, routingKey: String = "", args arguments: Table = Table()) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.queue(.unbind(.init(
                reserved1: 0, queueName: queue, exchangeName: exchange, routingKey: routingKey, arguments: arguments)))))
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .queue(let queue) = channel, case .unbinded = queue else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Declare a exchange.
    /// - Parameters:
    ///     - name: Name of the exchange.
    ///     - passive: If enabled broker will raise exception if exchange already exists.
    ///     - durable: if enabled creates a exchange stored on disk otherwise transient.
    ///     - auto_delete: if enabled exchange will be deleted when the last consumer has stopped consuming.
    ///     - internal: Whether the exchange cannot be directly published to client.
    ///     - arguments: Additional arguments (check rabbitmq documentation).
    /// - Returns: EventLoopFuture waiting for declare response.
    public func exchangeDeclare(name: String, type: String, passive: Bool = false, durable: Bool = false, autoDelete: Bool = false,  internal: Bool = false, args arguments: Table = Table()) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.exchange(.declare(.init(
                reserved1: 0, exchangeName: name, exchangeType: type, passive: passive, durable: durable, autoDelete: autoDelete, internal: `internal`, noWait: false, arguments: arguments)))))
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .exchange(let exchange) = channel, case .declared = exchange else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Deletes a exchange.
    /// - Parameters:
    ///     - name: Name of the queue.
    ///     - ifUnused: if enabled exchange will be deleted only when it's not used.
    /// - Returns: EventLoopFuture waiting for delete response.
    public func exchangeDelete(name: String, ifUnused: Bool = false) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.exchange(.delete(.init(reserved1: 0, exchangeName: name, ifUnused: ifUnused, noWait: false)))))
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .exchange(let exchange) = channel, case .deleted = exchange else {
                    throw AMQPClientError.invalidResponse(response)
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
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.exchange(.bind(.init(
                reserved1: 0, destination: destination, source: source, routingKey: routingKey, noWait: false, arguments: arguments)))))
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .exchange(let exchange) = channel, case .binded = exchange else {
                    throw AMQPClientError.invalidResponse(response)
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
    /// - Returns: EventLoopFuture waiting for bind response.
    public func exchangeUnbind(destination: String, source: String, routingKey: String, args arguments: Table = Table()) -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.exchange(.unbind(.init(
                reserved1: 0, destination: destination, source: source, routingKey: routingKey, noWait: false, arguments: arguments)))))
            .flatMapThrowing { response in 
                guard case .channel(let channel) = response, case .exchange(let exchange) = channel, case .unbinded = exchange else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Sets the channel in publish confirm mode, each published message will be acked or nacked.
    /// - Returns: EventLoopFuture waiting for confirm select response.
    public func confirmSelect() -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        guard !self.isConfirmMode.load(ordering: .relaxed) else {
            return self.eventLoop.makeSucceededFuture(())
        }

        return channel.send(payload: .method(.confirm(.select(noWait: false))))
            .flatMapThrowing { response in
                guard case .channel(let channel) = response, case .confirm(let confirm) = channel, case .selected = confirm else {
                    throw AMQPClientError.invalidResponse(response)
                }

                self.isConfirmMode.store(true, ordering: .relaxed)

                return ()
            }
    }

    /// Set the Channel in transaction mode.
    /// - Returns: EventLoopFuture waiting for tx select response.
    public func txSelect() -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        guard !self.isTxMode.load(ordering: .relaxed) else {
            return self.eventLoop.makeSucceededFuture(())
        }

        return channel.send(payload: .method(.tx(.select)))
            .flatMapThrowing { response in
                guard case .channel(let channel) = response, case .tx(let tx) = channel, case .selected = tx else {
                    throw AMQPClientError.invalidResponse(response)
                }

                self.isTxMode.store(true, ordering: .relaxed)

                return ()
            }
    }

    /// Commit a transaction.
    /// - Returns: EventLoopFuture waiting for commit response.
    public func txCommit() -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.tx(.commit)))
            .flatMapThrowing { response in
                guard case .channel(let channel) = response, case .tx(let tx) = channel, case .committed = tx else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Rollback a transaction.
    /// - Returns: EventLoopFuture waiting for rollback response.
    public func txRollback() -> EventLoopFuture<Void> {
        guard let channel = self.channel else { return self.eventLoop.makeFailedFuture(AMQPClientError.channelClosed()) }

        return channel.send(payload: .method(.tx(.rollback)))
            .flatMapThrowing { response in
                guard case .channel(let channel) = response, case .tx(let tx) = channel, case .rollbacked = tx else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return ()
            }
    }

    /// Add close listener.
    /// - Parameters:
    ///     - name: listener identifier.
    ///     - listener: callback when channel is closed.
    public func addCloseListener(named name: String, listener: @escaping (Result<Void, Error>) -> Void)  {
        return self.closeListeners.addListener(named: name, listener: listener)
    }

    /// Remove close listener.
    /// - Parameters:
    ///     - name: listener identifier.
    public func removeCloseListener(named name: String)  {
        return self.closeListeners.removeListener(named: name)
    }


    /// Add publish listener.
    /// When channel is in confirm mode broker sends whether published message was accepted.
    /// - Parameters:
    ///     - name: identifier of listner.
    ///     - listener: callback when publish confirmation message is received.
    public func addPublishListener(named name: String,  listener: @escaping (Result<AMQPResponse.Channel.Basic.PublishConfirm, Error>) -> Void) throws {
        guard let channel = self.channel else { throw AMQPClientError.channelClosed() }

        guard self.isConfirmMode.load(ordering: .relaxed) else {
            throw AMQPClientError.channelNotInConfirmMode
        }

        return channel.addPublishListener(named: name, listener: listener)
    }

    /// Remove publish listener.
    /// - Parameters:
    ///     - name: identifier of consumer.
    public func removePublishListener(named name: String)  {
        guard let channel = self.channel else { return }

        return channel.removePublishListener(named: name)
    }

    /// Add consume listener.
    /// When basic consume has started broker sends delivery messages to consumer.
    /// - Parameters:
    ///     - consumerTag: name of consumer.
    ///     - listener: callback when delivery message is received.
    public func addConsumeListener(consumerTag: String, listener: @escaping (Result<AMQPResponse.Channel.Message.Delivery, Error>) -> Void) throws {
        guard let channel = self.channel  else { throw AMQPClientError.channelClosed() }

        return channel.addConsumeListener(named: consumerTag, listener: listener)   
    }

    /// Remove consume listener.
    /// - Parameters:
    ///     - consumerTag: name of consumer.
    public func removeConsumeListener(consumerTag: String) {
        guard let channel = self.channel else { return }

        return channel.removeConsumeListener(named: consumerTag)   
    }

    /// Add return listener.
    /// When broker cannot route message to any queue it sends a return message.
    /// - Parameters:
    ///     - name: identifier of listner.
    ///     - listener: callback when return message is received.
    public func addReturnListener(named name: String,  listener: @escaping (Result<AMQPResponse.Channel.Message.Return, Error>) -> Void) throws {
        guard let channel = self.channel else { throw AMQPClientError.channelClosed() }

        return channel.addReturnListener(named: name, listener: listener)
    }

    /// Remove return message listener.
    /// - Parameters:
    ///     - name: number of listner.
    public func removeReturnListener(named name: String)  {
        guard let channel = self.channel else { return }

        return channel.removeReturnListener(named: name)   
    }

    /// Add flow listener.
    /// When broker cannot keep up with amount of published messages it sends a flow(false) message.
    /// When broker is again ready to handle new messages it sends a flow(true) message.
    /// - Parameters:
    ///     - name: identifier of listner.
    ///     - listener: callback when flow signal is received.
    public func addFlowListener(named name: String,  listener: @escaping (Result<Bool, Error>) -> Void) throws {
        guard let channel = self.channel else { throw AMQPClientError.channelClosed() }

        return channel.addFlowListener(named: name, listener: listener)
    }

    /// Remove flow listener.
    /// - Parameters:
    ///     - name: listener identifier.
    public func removeFlowListener(named name: String)  {
        guard let channel = self.channel else { return }

        return channel.removeFlowListener(named: name)   
    }

    func addListener<Value>(type: Value.Type, named name: String, listener: @escaping (Result<Value, Error>) -> Void) throws {
        switch listener {
            case let l as (Result<AMQPResponse.Channel.Message.Delivery, Error>) -> Void:
                return try addConsumeListener(consumerTag: name, listener: l)
            case let l as (Result<AMQPResponse.Channel.Basic.PublishConfirm, Error>) -> Void:
                return try addPublishListener(named: name, listener: l)
            case let l as (Result<AMQPResponse.Channel.Message.Return, Error>) -> Void:
                return try addReturnListener(named: name, listener: l)
            case let l as (Result<Bool, Error>) -> Void:
                return try addFlowListener(named: name, listener: l)
            default:
                preconditionUnexpectedListenerType(type)
        }
    }

    func removeListener<Value>(type: Value.Type, named name: String) {
        switch type {
            case is AMQPResponse.Channel.Message.Delivery.Type:
                return removeConsumeListener(consumerTag: name)
            case is AMQPResponse.Channel.Basic.PublishConfirm.Type:
                return removePublishListener(named: name)
            case is AMQPResponse.Channel.Message.Return.Type:
                return removeReturnListener(named: name)
            case is Bool.Type:
                return removeFlowListener(named: name)
            default:
                preconditionUnexpectedListenerType(type)
        }
    }
}
