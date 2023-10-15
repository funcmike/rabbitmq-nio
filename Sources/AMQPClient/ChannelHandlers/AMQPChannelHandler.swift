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
    public typealias Listener<Value> = @Sendable (Result<Value, Error>) -> Void

    public typealias Delivery = AMQPResponse.Channel.Message.Delivery
    public typealias Publish = AMQPResponse.Channel.Basic.PublishConfirm
    public typealias Return = AMQPResponse.Channel.Message.Return
    public typealias Flow = Bool
    public typealias Close = Void

    internal enum ConnectionState {
        case open
        case shuttingDown
        case closed
    }

    private struct Listeners {
        var consumeListeners = [String: Listener<Delivery>]()
        var publishListeners = [String: Listener<Publish>]()
        var returnListeners = [String: Listener<Return>]()
        var flowListeners = [String: Listener<Flow>]()
        var closeListeners = [String: Listener<Close>]()
    }

    private let state = NIOLockedValueBox(ConnectionState.open)
    private let listeners = NIOLockedValueBox(Listeners())

    private let parent: AMQPConnectionHandler
    private let channelID: Frame.ChannelID
    private let eventLoop: EventLoop

    private let closePromise: NIOCore.EventLoopPromise<Void>

    var closeFuture: NIOCore.EventLoopFuture<Void> {
        return closePromise.futureResult
    }

    public var isOpen: Bool {
        return state.withLockedValue { $0 == .open }
    }

    init(parent: AMQPConnectionHandler, channelID: Frame.ChannelID, eventLoop: EventLoop) {
        self.parent = parent
        self.channelID = channelID
        self.eventLoop = eventLoop
        closePromise = eventLoop.makePromise()
    }

    func addListener<Value>(type: Value.Type, named name: String, listener: @escaping Listener<Value>) throws {
        guard isOpen else { throw AMQPConnectionError.channelClosed() }

        listeners.withLockedValue {
            switch listener {
            case let l as Listener<Delivery>:
                $0.consumeListeners[name] = l
            case let l as Listener<Publish>:
                $0.publishListeners[name] = l
            case let l as Listener<Return>:
                $0.returnListeners[name] = l
            case let l as Listener<Flow>:
                $0.flowListeners[name] = l
            case let l as Listener<Close>:
                $0.closeListeners[name] = l
            default:
                preconditionUnexpectedType(type)
            }
        }
    }

    func removeListener<Value>(type: Value.Type, named name: String) {
        guard isOpen else { return }

        listeners.withLockedValue {
            switch type {
            case is Delivery.Type:
                $0.consumeListeners.removeValue(forKey: name)
            case is Publish.Type:
                $0.publishListeners.removeValue(forKey: name)
            case is Return.Type:
                $0.returnListeners.removeValue(forKey: name)
            case is Flow.Type:
                $0.flowListeners.removeValue(forKey: name)
            case is Close.Type:
                $0.closeListeners.removeValue(forKey: name)
            default:
                preconditionUnexpectedType(type)
            }
        }
    }

    func existsConsumeListener(named name: String) throws -> Bool {
        guard isOpen else { throw AMQPConnectionError.channelClosed() }
        return listeners.withLockedValue { $0.consumeListeners.keys.contains(name) }
    }

    func receiveDelivery(_ message: Delivery, for consumerTag: String) {
        if let listener = listeners.withLockedValue({ $0.consumeListeners[consumerTag] }) {
            listener(.success(message))
        }
    }

    func receivePublishConfirm(_ message: Publish) {
        listeners.withLockedValue { $0.publishListeners.values }.forEach { $0(.success(message)) }
    }

    func receiveFlow(_ message: Flow) {
        listeners.withLockedValue { $0.flowListeners.values }.forEach { $0(.success(message)) }
    }

    func receiveReturn(_ message: Return) {
        listeners.withLockedValue { $0.returnListeners.values }.forEach { $0(.success(message)) }
    }

    func handleCancellation(consumerTag: String) {
        if let listener = listeners.withLockedValue({ $0.consumeListeners[consumerTag] }) {
            listener(.failure(AMQPConnectionError.consumerCancelled))
        }
        removeListener(type: Delivery.self, named: consumerTag)
    }

    func reportAsClosed(error: Error? = nil) {
        let shouldClose = state.withLockedValue { state in
            if state == .open {
                state = .shuttingDown
                return true
            }

            return false
        }

        guard shouldClose else { return }

        listeners.withLockedValue {
            $0.closeListeners.values.forEach { $0(error == nil ? .success(()) : .failure(error!)) }
            $0 = .init()
        }

        state.withLockedValue { $0 = .closed }
        error == nil ? closePromise.succeed(()) : closePromise.fail(error!)
    }

    deinit {
        if isOpen {
            assertionFailure("close() was not called before deinit!")
        }
    }
}

extension AMQPChannelHandler {
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
        parent.write(
            .frame(frame),
            responsePromise: responsePromise,
            writePromise: writePromise
        )

        return responsePromise.futureResult
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
}
