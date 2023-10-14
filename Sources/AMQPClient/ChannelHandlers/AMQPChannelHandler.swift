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
import Collections
import NIOConcurrencyHelpers
import NIOCore

internal final class AMQPChannelHandler: Sendable {
    internal enum ConnectionState {
        case open
        case shuttingDown
        case closed
    }

    private let state = NIOLockedValueBox(ConnectionState.open)

    public var isOpen: Bool {
        return state.withLockedValue { $0 == .open }
    }

    private let parent: AMQPConnectionHandler
    private let channelID: Frame.ChannelID
    private let eventLoop: EventLoop
    //TODO: push this down to channel handler
    private let responseQueue: NIOLoopBoundBox<Deque<EventLoopPromise<AMQPResponse>>>
    //TODO: push this down to channel handler
    private var nextMessage: (frame: Frame.Method.Basic, properties: Properties?)?

    private let closePromise: NIOCore.EventLoopPromise<Void>
    var closeFuture: NIOCore.EventLoopFuture<Void> {
        return closePromise.futureResult
    }

    private let _lock = NIOLock()

    public typealias Listener<Value> = @Sendable (Result<Value, Error>) -> Void

    public typealias Delivery = AMQPResponse.Channel.Message.Delivery
    public typealias Publish = AMQPResponse.Channel.Basic.PublishConfirm
    public typealias Return = AMQPResponse.Channel.Message.Return
    public typealias Flow = Bool
    public typealias Close = Void

    private typealias Listeners<Value> = [String: Listener<Value>]

    //TODO: place these in a locked box
    private var consumeListeners = [String: Listener<Delivery>]()
    private var publishListeners = Listeners<Publish>()
    private var returnListeners = Listeners<Return>()
    private var flowListeners = Listeners<Flow>()
    private var closeListeners = Listeners<Close>()

    init(parent: AMQPConnectionHandler, channelID: Frame.ChannelID, eventLoop: EventLoop) {
        self.parent = parent
        self.channelID = channelID
        self.eventLoop = eventLoop
        responseQueue = .init(Deque(), eventLoop: eventLoop)
        closePromise = eventLoop.makePromise()
    }

    func addListener<Value>(type: Value.Type, named name: String, listener: @escaping Listener<Value>) throws {
        guard isOpen else { throw AMQPConnectionError.channelClosed() }

        switch listener {
        case let l as Listener<Delivery>:
            return _lock.withLock { self.consumeListeners[name] = l }
        case let l as Listener<Publish>:
            return _lock.withLock { self.publishListeners[name] = l }
        case let l as Listener<Return>:
            return _lock.withLock { self.returnListeners[name] = l }
        case let l as Listener<Flow>:
            return _lock.withLock { self.flowListeners[name] = l }
        case let l as Listener<Close>:
            return _lock.withLock { self.closeListeners[name] = l }
        default:
            preconditionUnexpectedType(type)
        }
    }

    func removeListener<Value>(type: Value.Type, named name: String) {
        guard isOpen else { return }

        switch type {
        case is Delivery.Type:
            return _lock.withLock { self.consumeListeners[name] = nil }
        case is Publish.Type:
            return _lock.withLock { self.publishListeners[name] = nil }
        case is Return.Type:
            return _lock.withLock { self.returnListeners[name] = nil }
        case is Flow.Type:
            return _lock.withLock { self.flowListeners[name] = nil }
        case is Close.Type:
            return _lock.withLock { self.closeListeners[name] = nil }
        default:
            preconditionUnexpectedType(type)
        }
    }

    private func notify<ReturnType>(type: ReturnType.Type, named name: String, _ result: Result<ReturnType, Error>) {
        switch result {
        case let r as Result<AMQPResponse.Channel.Message.Delivery, Error>:
            if let listener = _lock.withLock({ () -> Listener<Delivery>? in
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
            let listeners = _lock.withLock { () -> Dictionary<String, Listener<Publish>>.Values in
                self.publishListeners.values
            }
            listeners.forEach { $0(r) }
        case let r as Result<AMQPResponse.Channel.Message.Return, Error>:
            let listeners = _lock.withLock { () -> Dictionary<String, Listener<Return>>.Values in
                self.returnListeners.values
            }
            listeners.forEach { $0(r) }
        case let r as Result<Bool, Error>:
            let listeners = _lock.withLock { () -> Dictionary<String, Listener<Flow>>.Values in self.flowListeners.values }
            listeners.forEach { $0(r) }
        case let r as Result<Void, Error>:
            let listeners = _lock.withLock { () -> Dictionary<String, Listener<Close>>.Values in self.closeListeners.values }
            listeners.forEach { $0(r) }
        default:
            preconditionUnexpectedType(type)
        }
    }

    func existsConsumeListener(named name: String) throws -> Bool {
        guard isOpen else { throw AMQPConnectionError.channelClosed() }

        return _lock.withLock { self.consumeListeners.contains { key, _ in key == name } }
    }

    func sendNoWait(payload: Frame.Payload) throws {
        guard isOpen else { throw AMQPConnectionError.channelClosed() }

        let frame = Frame(channelID: channelID, payload: payload)
        parent.write(
            .frame(frame),
            responsePromise: nil,
            writePromise: nil
        )
    }

    func send(payload: Frame.Payload) -> EventLoopFuture<AMQPResponse> {
        guard isOpen else { return eventLoop.makeFailedFuture(AMQPConnectionError.channelClosed()) }

        let frame = Frame(channelID: channelID, payload: payload)

        let responsePromise = eventLoop.makePromise(of: AMQPResponse.self)
        let writePromise = eventLoop.makePromise(of: Void.self)
        writePromise.futureResult.whenFailure { responsePromise.fail($0) }

        return eventLoop.submit { [self] in
            responseQueue.value.append(responsePromise)
            parent.write(
                .frame(frame),
                responsePromise: nil,
                writePromise: writePromise
            )
        }.flatMap { responsePromise.futureResult }
    }

    func send(payload: Frame.Payload) -> EventLoopFuture<Void> {
        guard isOpen else { return eventLoop.makeFailedFuture(AMQPConnectionError.channelClosed()) }

        let frame = Frame(channelID: channelID, payload: payload)
        let promise = eventLoop.makePromise(of: Void.self)

        parent.write(
            .frame(frame),
            responsePromise: nil,
            writePromise: promise
        )
        return promise.futureResult
    }

    func send(payloads: [Frame.Payload]) -> EventLoopFuture<Void> {
        guard isOpen else { return eventLoop.makeFailedFuture(AMQPConnectionError.channelClosed()) }

        let frames = payloads.map { Frame(channelID: self.channelID, payload: $0) }
        let promise = eventLoop.makePromise(of: Void.self)

        parent.write(
            .bulk(frames),
            responsePromise: nil,
            writePromise: promise
        )

        return promise.futureResult
    }

    func receive(payload: Frame.Payload) {
        switch payload {
        case let .method(method):
            switch method {
            case let .basic(basic):
                switch basic {
                case .getEmpty:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.message(.get())))
                    }
                case .deliver, .getOk, .return:
                    nextMessage = (frame: basic, properties: nil)
                case .recoverOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.basic(.recovered)))
                    }
                case .qosOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.basic(.qosOk)))
                    }
                case let .consumeOk(consumerTag):
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.basic(.consumeOk(.init(consumerTag: consumerTag)))))
                    }
                case let .ack(deliveryTag, multiple):
                    notify(type: Publish.self, .success(.ack(deliveryTag: deliveryTag, multiple: multiple)))
                case let .nack(nack):
                    notify(type: Publish.self, .success(.nack(deliveryTag: nack.deliveryTag, multiple: nack.multiple)))
                case let .cancel(cancel):
                    notify(type: Delivery.self, named: cancel.consumerTag, .failure(AMQPConnectionError.consumerCancelled))

                    removeListener(type: Delivery.self, named: cancel.consumerTag)
                case let .cancelOk(consumerTag):
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.basic(.canceled)))
                    }

                    notify(type: Delivery.self, named: consumerTag, .failure(AMQPConnectionError.consumerCancelled))

                    removeListener(type: Delivery.self, named: consumerTag)
                default:
                    preconditionUnexpectedPayload(payload)
                }
            case let .channel(channel):
                switch channel {
                case .closeOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.closed(channelID)))
                    }
                case let .flow(active):
                    notify(type: Flow.self, .success(active))
                case let .flowOk(active):
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.flowed(.init(active: active))))
                    }
                default:
                    preconditionUnexpectedPayload(payload)
                }
            case let .queue(queue):
                switch queue {
                case let .declareOk(declareOk):
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.queue(.declared(.init(queueName: declareOk.queueName,
                                                                        messageCount: declareOk.messageCount,
                                                                        consumerCount: declareOk.consumerCount)))))
                    }
                case .bindOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.queue(.binded)))
                    }
                case let .purgeOk(messageCount):
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.queue(.purged(.init(messageCount: messageCount)))))
                    }
                case let .deleteOk(messageCount):
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.queue(.deleted(.init(messageCount: messageCount)))))
                    }
                case .unbindOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.queue(.unbinded)))
                    }
                default:
                    preconditionUnexpectedPayload(payload)
                }
            case let .exchange(exchange):
                switch exchange {
                case .declareOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.exchange(.declared)))
                    }
                case .deleteOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.exchange(.deleted)))
                    }
                case .bindOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.exchange(.binded)))
                    }
                case .unbindOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.exchange(.unbinded)))
                    }
                default:
                    preconditionUnexpectedPayload(payload)
                }
            case let .confirm(confirm):
                switch confirm {
                case .selectOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.confirm(.selected)))
                    }
                default:
                    preconditionUnexpectedPayload(payload)
                }
            case let .tx(tx):
                switch tx {
                case .selectOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.tx(.selected)))
                    }
                case .commitOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.tx(.committed)))
                    }
                case .rollbackOk:
                    if let promise = responseQueue.value.popFirst() {
                        promise.succeed(.channel(.tx(.rollbacked)))
                    }
                default:
                    preconditionUnexpectedPayload(payload)
                }
            default:
                preconditionUnexpectedPayload(payload)
            }
        case let .header(header):
            nextMessage?.properties = header.properties
        case let .body(body):
            guard let msg = nextMessage, let properties = msg.properties else {
                if let promise = responseQueue.value.popFirst() {
                    promise.fail(AMQPConnectionError.invalidMessage)
                }
                return
            }

            switch msg.frame {
            case let .getOk(getOk):
                if let promise = responseQueue.value.popFirst() {
                    promise.succeed(.channel(.message(.get(.init(
                        message: .init(
                            exchange: getOk.exchange,
                            routingKey: getOk.routingKey,
                            deliveryTag: getOk.deliveryTag,
                            properties: properties,
                            redelivered: getOk.redelivered,
                            body: body
                        ),
                        messageCount: getOk.messageCount
                    )))))
                }
            case let .deliver(deliver):
                notify(type: Delivery.self,
                       named: deliver.consumerTag,
                       .success(.init(
                           exchange: deliver.exchange,
                           routingKey: deliver.routingKey,
                           deliveryTag: deliver.deliveryTag,
                           properties: properties,
                           redelivered: deliver.redelivered,
                           body: body
                       )))
            case let .return(`return`):
                notify(type: Return.self,
                       .success(.init(
                           replyCode: `return`.replyCode,
                           replyText: `return`.replyText,
                           exchange: `return`.exchange,
                           routingKey: `return`.routingKey,
                           properties: properties,
                           body: body
                       )))
            default:
                preconditionUnexpectedPayload(payload)
            }

            nextMessage = nil
        default:
            preconditionUnexpectedPayload(payload)
        }
    }

    func close(error: Error? = nil) {
        let shouldClose = state.withLockedValue { state in
            if state == .open {
                state = .shuttingDown
                return true
            }

            return false
        }

        guard shouldClose else { return }

        let queue = responseQueue.value
        responseQueue.value.removeAll()

        queue.forEach { $0.fail(error ?? AMQPConnectionError.channelClosed()) }

        notify(type: Close.self, error == nil ? .success(()) : .failure(error!))

        consumeListeners = [:]
        publishListeners = [:]
        returnListeners = [:]
        flowListeners = [:]
        closeListeners = [:]

        error == nil ? closePromise.succeed(()) : closePromise.fail(error!)

        state.withLockedValue { $0 = .closed }
    }

    deinit {
        if isOpen {
            assertionFailure("close() was not called before deinit!")
        }

        //TODO: refactor response queue into NIO handler
        // if !self.responseQueue.value.isEmpty {
        //     assertionFailure("Queue is not empty! Queue size: \(self.responseQueue.value.count)")
        // }
    }
}
