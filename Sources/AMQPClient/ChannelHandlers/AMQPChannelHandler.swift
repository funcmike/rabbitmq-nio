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

import Collections
import NIOCore
import NIOConcurrencyHelpers
import AMQPProtocol

internal final class AMQPChannelHandler<Parent: AMPQChannelHandlerParent> {
    internal enum ConnectionState {
        case open
        case shuttingDown
        case closed
    }

    private var _state = ConnectionState.open
    private var state: ConnectionState {
        get { return _lock.withLock { self._state } }
        set(newValue) { _lock.withLockVoid { self._state = newValue } }
    }

    public var isOpen: Bool {
        return self.state == .open
    }

    private let parent: Parent
    private let channelID: Frame.ChannelID
    private let eventLoop: EventLoop
    private var responseQueue: Deque<EventLoopPromise<AMQPResponse>>
    private var nextMessage: (frame: Frame.Method.Basic, properties: Properties?)?

    private let closePromise: NIOCore.EventLoopPromise<Void>
    var closeFuture: NIOCore.EventLoopFuture<Void> {
        get { self.closePromise.futureResult }
    }
    
    private let _lock = NIOLock()

    private var consumeListeners = AMQPListeners<AMQPResponse.Channel.Message.Delivery>()
    private var flowListeners = AMQPListeners<Bool>()
    private var returnListeners = AMQPListeners<AMQPResponse.Channel.Message.Return>()
    private var publishListeners = AMQPListeners<AMQPResponse.Channel.Basic.PublishConfirm>()
    private var closeListeners = AMQPListeners<Void>()

    init(parent: Parent, channelID: Frame.ChannelID, eventLoop: EventLoop,  initialQueueCapacity: Int = 3) {
        self.parent = parent
        self.channelID = channelID
        self.eventLoop = eventLoop
        self.closePromise = eventLoop.makePromise()
        self.responseQueue = Deque(minimumCapacity: initialQueueCapacity)
        self.closePromise.futureResult.whenComplete { self.closeListeners.notify($0) }
    }

    func addConsumeListener(named name: String, listener: @escaping  AMQPListeners<AMQPResponse.Channel.Message.Delivery>.Listener) throws {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }

        return self.consumeListeners.addListener(named: name, listener: listener)
    }

    func removeConsumeListener(named name: String) throws {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }

        return self.consumeListeners.removeListener(named: name)
    }

    func addFlowListener(named name: String, listener: @escaping AMQPListeners<Bool>.Listener) throws {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }

        return self.flowListeners.addListener(named: name, listener: listener)
    }

    func removeFlowListener(named name: String) throws {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }

        return self.flowListeners.removeListener(named: name)
    }

    func addReturnListener(named name: String, listener: @escaping AMQPListeners<AMQPResponse.Channel.Message.Return>.Listener) throws {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }
        return self.returnListeners.addListener(named: name, listener: listener)
    }

    func removeReturnListener(named name: String) throws {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }
        return self.returnListeners.removeListener(named: name)
    }

    func addPublishListener(named name: String, listener: @escaping AMQPListeners<AMQPResponse.Channel.Basic.PublishConfirm>.Listener) throws {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }
        return self.publishListeners.addListener(named: name, listener: listener)
    }

    func removePublishListener(named name: String) throws {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }
        return self.publishListeners.removeListener(named: name)
    }
    
    func addCloseListener(named name: String, listener: @escaping AMQPListeners<Void>.Listener) throws {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }
        return self.closeListeners.addListener(named: name, listener: listener)
    }

    func removeCloseListener(named name: String) throws  {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }
        return self.closeListeners.removeListener(named: name)
    }

    func send(payload: Frame.Payload) -> EventLoopFuture<AMQPResponse> {
        let promise = self.eventLoop.makePromise(of: AMQPResponse.self)

        let sendResult: EventLoopFuture<Void> = self.send(payload: payload)
        
        sendResult.whenFailure { promise.fail($0) }
        sendResult.whenSuccess { self.responseQueue.append(promise) }

        return sendResult.flatMap {  
            promise.futureResult
        }
    }

    func send(payloads: [Frame.Payload]) -> EventLoopFuture<AMQPResponse> {
        let promise = self.eventLoop.makePromise(of: AMQPResponse.self)

        let sendResult: EventLoopFuture<Void> = self.send(payloads: payloads)

        sendResult.whenFailure { promise.fail($0) }
        sendResult.whenSuccess { self.responseQueue.append(promise) }

        return sendResult.flatMap {
            promise.futureResult
        }
    }

    func send(payloads: [Frame.Payload]) -> EventLoopFuture<Void> {
        guard self.isOpen else { return self.eventLoop.makeFailedFuture(AMQPConnectionError.channelClosed()) }

        let frames = payloads.map { Frame(channelID: self.channelID, payload: $0) }
        
        let promise = self.eventLoop.makePromise(of: Void.self)

        return self.eventLoop.flatSubmit {
            self.parent.write(frames: frames, promise: promise)
            return promise.futureResult
        }
    }

    func send(payload: Frame.Payload) -> EventLoopFuture<Void> {
        guard self.isOpen else { return self.eventLoop.makeFailedFuture(AMQPConnectionError.channelClosed()) }

        let frame = Frame(channelID: self.channelID, payload: payload)
        
        let promise = self.eventLoop.makePromise(of: Void.self)

        return self.eventLoop.flatSubmit {
            self.parent.write(frame: frame, promise: promise)
            return promise.futureResult
        }
    }

    func receive(payload: Frame.Payload) {
        switch payload {
        case .method(let method): 
            switch method {
            case .basic(let basic):
                switch basic {
                case .getEmpty:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.message(.get())))
                    }
                case .deliver, .getOk, .return:
                    self.nextMessage = (frame: basic, properties: nil)
                case .recoverOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.recovered)))
                    }
                case .qosOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.qosOk)))
                    }
                case .consumeOk(let consumerTag):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.consumeOk(.init(consumerTag: consumerTag)))))
                    }
                case .ack(let deliveryTag, let multiple):
                    self.publishListeners.notify(.success(.ack(deliveryTag: deliveryTag, multiple: multiple)))             
                case .nack(let nack):
                    self.publishListeners.notify(.success(.nack(deliveryTag: nack.deliveryTag, multiple: nack.multiple)))       
                case .cancel(let cancel):
                    self.consumeListeners.notify(named: cancel.consumerTag, .failure(AMQPConnectionError.consumerCanceled))
                case .cancelOk(let consumerTag):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.canceled)))
                    }

                    self.consumeListeners.notify(named: consumerTag, .failure(AMQPConnectionError.consumerCanceled))
                default:
                    preconditionUnexpectedPayload(payload)
                }
            case .channel(let channel):
                switch channel {
                case .closeOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.closed(self.channelID)))
                    }
                case .flow(let active):
                    self.flowListeners.notify(.success(active))
                case .flowOk(let active):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.flowed(.init(active: active))))
                    }
                default:
                    preconditionUnexpectedPayload(payload)
                }
            case .queue(let queue):
                switch queue {
                case .declareOk(let declareOk):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.queue(.declared(.init(queueName: declareOk.queueName, messageCount: declareOk.messageCount, consumerCount: declareOk.consumerCount)))))
                    }
                case .bindOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.queue(.binded)))
                    }
                case .purgeOk(let messageCount):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.queue(.purged(.init(messageCount: messageCount)))))
                    }
                case .deleteOk(let messageCount):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.queue(.deleted(.init(messageCount: messageCount)))))
                    }
                case .unbindOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.queue(.unbinded)))
                    }
                default:
                    preconditionUnexpectedPayload(payload)
                }
            case .exchange(let exchange):
                switch exchange {
                case .declareOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.exchange(.declared)))
                    }
                case .deleteOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.exchange(.deleted)))
                    }
                case .bindOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.exchange(.binded)))
                    }
                case .unbindOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.exchange(.unbinded)))
                    }
                default:
                    preconditionUnexpectedPayload(payload)
                }
            case .confirm(let confirm):
                switch confirm {
                case .selectOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.confirm(.selected)))
                    }
                default:
                    preconditionUnexpectedPayload(payload)
                }
            case .tx(let tx):
                switch tx {
                case .selectOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.tx(.selected)))
                    }
                case .commitOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.tx(.committed)))
                    }
                case .rollbackOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.tx(.rollbacked)))
                    }
                default:
                    preconditionUnexpectedPayload(payload)  
                }
            default:
                preconditionUnexpectedPayload(payload)
            }
        case .header(let header):
            self.nextMessage?.properties = header.properties
        case .body(let body):
            guard let msg = nextMessage, let properties = msg.properties else {
                if let promise = self.responseQueue.popFirst() {
                    promise.fail(AMQPConnectionError.invalidMessage)
                }
                return
            }

            switch msg.frame {
            case .getOk(let getOk):
                    if let promise = self.responseQueue.popFirst() {
                            promise.succeed(.channel(.message(.get(.init(
                                message: AMQPResponse.Channel.Message.Delivery(
                                    exchange: getOk.exchange,
                                    routingKey: getOk.routingKey,
                                    deliveryTag: getOk.deliveryTag,
                                    properties: properties,
                                    redelivered: getOk.redelivered,
                                    body: body),
                                messageCount: getOk.messageCount)))))
                    }
            case .deliver(let deliver):
                self.consumeListeners.notify(named: deliver.consumerTag, .success(.init(
                    exchange: deliver.exchange,
                    routingKey: deliver.routingKey,
                    deliveryTag: deliver.deliveryTag,
                    properties: properties,
                    redelivered: deliver.redelivered,
                    body: body)))
            case .return(let `return`):
                self.returnListeners.notify(.success(.init(
                    replyCode: `return`.replyCode,
                    replyText: `return`.replyText,
                    exchange: `return`.exchange,
                    routingKey: `return`.routingKey,
                    properties: properties,
                    body: body)))                   
            default:
                preconditionUnexpectedPayload(payload)
            }

            nextMessage = nil
        default:
            preconditionUnexpectedPayload(payload)
        }
    }

    func close(error: Error) {
        guard self.isOpen else { return }

        self.state = .shuttingDown

        let queue = self.responseQueue
        self.responseQueue.removeAll()

        queue.forEach { $0.fail(error) }

        closePromise.succeed(())
        
        self.state = .closed
    }

    deinit {
        if !self.responseQueue.isEmpty {
            assertionFailure("AMQP Channel Handler deinit when queue is not empty! Queue size: \(self.responseQueue.count)")
        }
    }
}
