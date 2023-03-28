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

    private let state = NIOLockedValueBox(ConnectionState.open)

    public var isOpen: Bool {
        return self.state.withLockedValue{ $0 == .open }
    }
    
    private let parent: Parent
    private let channelID: Frame.ChannelID
    private let eventLoop: EventLoop
    private var responseQueue = Deque<EventLoopPromise<AMQPResponse>>()
    private var nextMessage: (frame: Frame.Method.Basic, properties: Properties?)?

    private let closePromise: NIOCore.EventLoopPromise<Void>
    var closeFuture: NIOCore.EventLoopFuture<Void> {
        return self.closePromise.futureResult
    }
    
    private let _lock = NIOLock()

    public typealias Listener<Value> = @Sendable (Result<Value, Error>) -> Void
    
    public typealias Delivery = AMQPResponse.Channel.Message.Delivery
    public typealias Publish = AMQPResponse.Channel.Basic.PublishConfirm
    public typealias Return = AMQPResponse.Channel.Message.Return
    public typealias Flow = Bool
    public typealias Close = Void

    private typealias Listeners<Value> = [String:Listener<Value>]

    private var consumeListeners = Listeners<Delivery>()
    private var publishListeners = Listeners<Publish>()
    private var returnListeners = Listeners<Return>()
    private var flowListeners = Listeners<Flow>()
    private var closeListeners = Listeners<Close>()

    init(parent: Parent, channelID: Frame.ChannelID, eventLoop: EventLoop) {
        self.parent = parent
        self.channelID = channelID
        self.eventLoop = eventLoop
        self.closePromise = eventLoop.makePromise()
    }
    
    func addListener<Value>(type: Value.Type, named name: String, listener: @escaping Listener<Value>) throws {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }

        switch listener {
            case let l as Listener<Delivery>:
                return self._lock.withLock { self.consumeListeners[name] = l }
            case let l as Listener<Publish>:
                return self._lock.withLock { self.publishListeners[name] = l }
            case let l as Listener<Return>:
                return self._lock.withLock { self.returnListeners[name] = l }
            case let l as Listener<Flow>:
                return self._lock.withLock { self.flowListeners[name] = l }
            case let l as Listener<Close>:
                return self._lock.withLock { self.closeListeners[name] = l }
            default:
                preconditionUnexpectedType(type)
        }
    }

    func removeListener<Value>(type: Value.Type, named name: String) {
        guard self.isOpen else { return }

        switch type {
            case is Delivery.Type:
                return self._lock.withLock { self.consumeListeners[name] = nil }
            case is Publish.Type:
                return self._lock.withLock { self.publishListeners[name] = nil }
            case is Return.Type:
                return self._lock.withLock { self.returnListeners[name] = nil }
            case is Flow.Type:
                return self._lock.withLock { self.flowListeners[name] = nil }
            case is Close.Type:
                return self._lock.withLock { self.closeListeners[name] = nil }
            default:
                preconditionUnexpectedType(type)
        }
    }
    
    private func notify<ReturnType>(type: ReturnType.Type, named name: String, _ result: Result<ReturnType, Error>) {
        switch result {
        case let r as Result<AMQPResponse.Channel.Message.Delivery, Error>:
            if let listener = self._lock.withLock({ () -> Listener<Delivery>? in
                self.consumeListeners[name]
            }) {
                listener(r)
            }
        default:
            preconditionUnexpectedType(type)
        }
    }
    
    private func notify<ReturnType>(type: ReturnType.Type, _ result: Result<ReturnType, Error>) {
        switch result {
        case let r as Result<AMQPResponse.Channel.Basic.PublishConfirm, Error>:
            let listeners = self._lock.withLock { () -> Dictionary<String, Listener<Publish>>.Values in
                self.publishListeners.values
            }
            listeners.forEach { $0(r) }
        case let r as Result<AMQPResponse.Channel.Message.Return, Error>:
            let listeners = self._lock.withLock { () -> Dictionary<String, Listener<Return>>.Values in
                self.returnListeners.values
            }
            listeners.forEach { $0(r) }
        case let r as Result<Bool, Error>:
            let listeners = self._lock.withLock { () -> Dictionary<String, Listener<Flow>>.Values in  self.flowListeners.values }
            listeners.forEach { $0(r) }
        case let r as Result<Void, Error>:
            let listeners = self._lock.withLock { () -> Dictionary<String, Listener<Close>>.Values in  self.closeListeners.values }
            listeners.forEach { $0(r) }
        default:
            preconditionUnexpectedType(type)
        }
    }
    
    func existsConsumeListener(named name: String) throws -> Bool {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }

        return self._lock.withLock { self.consumeListeners.contains { key, _ in key == name } }
    }
    
    func send(payload: Frame.Payload) throws {
        guard self.isOpen else { throw AMQPConnectionError.channelClosed() }

        let frame = Frame(channelID: self.channelID, payload: payload)

        self.eventLoop.execute {
            self.parent.write(frame: frame, promise: nil)
        }
    }

    func send(payload: Frame.Payload) -> EventLoopFuture<AMQPResponse> {
        guard self.isOpen else { return self.eventLoop.makeFailedFuture(AMQPConnectionError.channelClosed()) }
        
        let frame = Frame(channelID: self.channelID, payload: payload)

        let responsePromise = self.eventLoop.makePromise(of: AMQPResponse.self)

        let writePromise = self.eventLoop.makePromise(of: Void.self)
        writePromise.futureResult.whenFailure { responsePromise.fail($0) }

        return self.eventLoop.flatSubmit {
            self.responseQueue.append(responsePromise)

            self.parent.write(frame: frame, promise: writePromise)

            return writePromise.futureResult.flatMap {
                    responsePromise.futureResult
                }
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
    
    func send(payloads: [Frame.Payload]) -> EventLoopFuture<Void> {
        guard self.isOpen else { return self.eventLoop.makeFailedFuture(AMQPConnectionError.channelClosed()) }

        let frames = payloads.map { Frame(channelID: self.channelID, payload: $0) }

        let promise = self.eventLoop.makePromise(of: Void.self)

        return self.eventLoop.flatSubmit {
            self.parent.write(frames: frames, promise: promise)
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
                    self.notify(type: Publish.self, .success(.ack(deliveryTag: deliveryTag, multiple: multiple)))
                case .nack(let nack):
                    self.notify(type: Publish.self, .success(.nack(deliveryTag: nack.deliveryTag, multiple: nack.multiple)))
                case .cancel(let cancel):
                    self.notify(type: Delivery.self, named: cancel.consumerTag, .failure(AMQPConnectionError.consumerCancelled))

                    self.removeListener(type: Delivery.self, named: cancel.consumerTag)
                case .cancelOk(let consumerTag):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.canceled)))
                    }

                    self.notify(type: Delivery.self, named: consumerTag, .failure(AMQPConnectionError.consumerCancelled))

                    self.removeListener(type: Delivery.self, named: consumerTag)
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
                    self.notify(type: Flow.self, .success(active))
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
                        promise.succeed(.channel(.queue(.declared(.init(queueName: declareOk.queueName,
                                                                        messageCount: declareOk.messageCount,
                                                                        consumerCount: declareOk.consumerCount)))))
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
                            message: .init(
                                exchange: getOk.exchange,
                                routingKey: getOk.routingKey,
                                deliveryTag: getOk.deliveryTag,
                                properties: properties,
                                redelivered: getOk.redelivered,
                                body: body),
                            messageCount: getOk.messageCount)))))
                }
            case .deliver(let deliver):
                self.notify(type: Delivery.self,
                            named: deliver.consumerTag,
                            .success(.init(
                                exchange: deliver.exchange,
                                routingKey: deliver.routingKey,
                                deliveryTag: deliver.deliveryTag,
                                properties: properties,
                                redelivered: deliver.redelivered,
                                body: body)))
            case .return(let `return`):
                self.notify(type: Return.self,
                            .success(.init(
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

    func close(error: Error? = nil) {
        let shouldClose = self.state.withLockedValue { state in
            if state == .open {
                state = .shuttingDown
                return true
            }

            return false
        }
        
        guard shouldClose else { return }

        let queue = self.responseQueue
        self.responseQueue.removeAll()

        queue.forEach { $0.fail(error ?? AMQPConnectionError.channelClosed()) }

        self.notify(type: Close.self, error == nil ? .success(()) : .failure(error!) )

        self.consumeListeners = [:]
        self.publishListeners = [:]
        self.returnListeners = [:]
        self.flowListeners = [:]
        self.closeListeners = [:]

        error == nil ? closePromise.succeed(()) : closePromise.fail(error!)

        self.state.withLockedValue{ $0 = .closed }
    }

    deinit {
        if isOpen {
            assertionFailure("close() was not called before deinit!")
        }

        if !self.responseQueue.isEmpty {
            assertionFailure("Queue is not empty! Queue size: \(self.responseQueue.count)")
        }
    }
}
