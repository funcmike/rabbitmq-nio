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
import NIOCore

public enum AMQPOutbound {
    case frame(Frame)
    case bulk([Frame])
    case bytes([UInt8])
}

internal final class AMQPConnectionMultiplexHandler: ChannelDuplexHandler {
    private enum State {
        case unblocked, blocked(Error), error(Error)
    }

    typealias InboundIn = Frame
    typealias OutboundIn = (AMQPOutbound, EventLoopPromise<AMQPResponse>?)
    typealias OutboundOut = AMQPOutbound

    private let eventLoop: EventLoop
    private var context: ChannelHandlerContext!
    private var channels: [Frame.ChannelID: AMQPChannelHandler] = [:]
    private var channelMax: UInt16 = 0
    private var state: State = .unblocked
    //private var responseQueue = [Frame.ChannelID: Deque<EventLoopPromise<AMQPResponse>>]()
    private var responseQueue = Deque<EventLoopPromise<AMQPResponse>>()

    private let config: AMQPConnectionConfiguration.Server

    init(eventLoop: EventLoop, config: AMQPConnectionConfiguration.Server, onReady: EventLoopPromise<AMQPResponse>) {
        self.config = config
        responseQueue.append(onReady)
        self.eventLoop = eventLoop
    }

    func addChannelHandler(_ handler: AMQPChannelHandler, forId id: Frame.ChannelID) {
        precondition(channels[id] == nil)
        channels[id] = handler
    }

    func channelActive(context: ChannelHandlerContext) {
        context.fireChannelActive()
        return start(use: context)
    }

    public func channelInactive(context _: ChannelHandlerContext) {
        switch state {
        case let .error(error):
            return failAllResponses(because: error)
        default:
            return failAllResponses(because: AMQPConnectionError.connectionClosed())
        }
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
    }

    func handlerRemoved(context _: ChannelHandlerContext) {
        switch state {
        case .unblocked, .blocked:
            state = .error(AMQPConnectionError.connectionClosed())
            context = nil
        case .error:
            context = nil
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = unwrapInboundIn(data)

        switch frame.payload {
        case let .method(method):
            switch method {
            case let .connection(connection):
                switch connection {
                case .start:
                    let clientProperties: Table = [
                        "connection_name": .longString(config.connectionName),
                        "product": .longString("rabbitmq-nio"),
                        "platform": .longString("Swift"),
                        "version": .longString("0.1"),
                        "capabilities": .table([
                            "publisher_confirms": .bool(true),
                            "exchange_exchange_bindings": .bool(true),
                            "basic.nack": .bool(true),
                            "per_consumer_qos": .bool(true),
                            "authentication_failure_close": .bool(true),
                            "consumer_cancel_notify": .bool(true),
                            "connection.blocked": .bool(true),
                        ]),
                    ]

                    let startOk = Frame(channelID: frame.channelID,
                                        payload: .method(.connection(.startOk(.init(clientProperties: clientProperties,
                                                                                    mechanism: "PLAIN",
                                                                                    response: "\u{0000}\(config.user)\u{0000}\(config.password)",
                                                                                    locale: "en_US")))))
                    context.writeAndFlush(wrapOutboundOut(.frame(startOk)), promise: nil)
                case let .tune(channelMax, frameMax, heartbeat):
                    self.channelMax = channelMax

                    let tuneOk = Frame(channelID: frame.channelID,
                                       payload: .method(.connection(.tuneOk(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat))))
                    let open = Frame(channelID: frame.channelID, payload: .method(.connection(.open(.init(vhost: config.vhost)))))

                    context.writeAndFlush(wrapOutboundOut(.bulk([tuneOk, open])), promise: nil)
                case .openOk:
                    if let promise = responseQueue.popFirst() {
                        promise.succeed(.connection(.connected(.init(channelMax: channelMax))))
                    }
                case let .close(close):
                    let closeOk = Frame(channelID: frame.channelID, payload: .method(.connection(.closeOk)))
                    context.writeAndFlush(wrapOutboundOut(.frame(closeOk)), promise: nil)

                    state = .error(AMQPConnectionError.connectionClosed(replyCode: close.replyCode, replyText: close.replyText))
                case .closeOk:
                    if let promise = responseQueue.popFirst() {
                        promise.succeed(.connection(.closed))
                    }
                case .blocked:
                    state = .blocked(AMQPConnectionError.connectionBlocked)
                case .unblocked:
                    state = .unblocked
                default:
                    preconditionUnexpectedFrame(frame)
                }
            case let .channel(channel):
                switch channel {
                case .openOk:
                    if let promise = responseQueue.popFirst() {
                        promise.succeed(.channel(.opened(frame.channelID)))
                    }
                case let .close(close):
                    if let channel = channels.removeValue(forKey: frame.channelID) {
                        channel.close(error: AMQPConnectionError.channelClosed(replyCode: close.replyCode, replyText: close.replyText))
                    }

                    let closeOk = Frame(channelID: frame.channelID, payload: .method(.channel(.closeOk)))
                    context.writeAndFlush(wrapOutboundOut(.frame(closeOk)), promise: nil)
                case .closeOk:
                    if let channel = channels.removeValue(forKey: frame.channelID) {
                        channel.receive(payload: frame.payload)
                        channel.close()
                    }
                case let .flow(active):
                    if let channel = channels.removeValue(forKey: frame.channelID) {
                        channel.receive(payload: frame.payload)
                    }

                    let flowOk = Frame(channelID: frame.channelID, payload: .method(.channel(.flowOk(active: active))))
                    context.writeAndFlush(wrapOutboundOut(.frame(flowOk)), promise: nil)
                default:
                    if let channel = channels[frame.channelID] {
                        channel.receive(payload: frame.payload)
                    }
                }
            case let .basic(basic):
                switch basic {
                case let .cancel(cancel):
                    if let channel = channels[frame.channelID] {
                        channel.receive(payload: frame.payload)
                    }

                    let cancelOk = Frame(channelID: frame.channelID, payload: .method(.basic(.cancelOk(consumerTag: cancel.consumerTag))))
                    context.writeAndFlush(wrapOutboundOut(.frame(cancelOk)), promise: nil)
                default:
                    if let channel = channels[frame.channelID] {
                        channel.receive(payload: frame.payload)
                    }
                }
            default:
                if let channel = channels[frame.channelID] {
                    channel.receive(payload: frame.payload)
                }
            }
        case .header, .body:
            if let channel = channels[frame.channelID] {
                channel.receive(payload: frame.payload)
            }
        case .heartbeat:
            let heartbeat = Frame(channelID: frame.channelID, payload: .heartbeat)
            context.writeAndFlush(wrapOutboundOut(.frame(heartbeat)), promise: nil)
        }
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let (outbound, responsePromise) = unwrapOutboundIn(data)
        switch state {
        case let .error(e), let .blocked(e):
            promise?.fail(e)
            responsePromise?.fail(e)
            return
        default:
            if let responsePromise { responseQueue.append(responsePromise) }
            context.write(wrapOutboundOut(outbound), promise: promise)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        failAllResponses(because: error)
        return context.close(promise: nil)
    }

    private func start(use context: ChannelHandlerContext) {
        return context.writeAndFlush(wrapOutboundOut(.bytes(PROTOCOL_START_0_9_1)), promise: nil)
    }

    func failAllResponses(because error: Error) {
        state = .error(error)

        let queue = responseQueue
        responseQueue.removeAll()

        queue.forEach { $0.fail(error) }

        let channels = self.channels
        self.channels.removeAll()

        channels.forEach { $1.close(error: error) }
    }

    deinit {
        if !self.responseQueue.isEmpty {
            assertionFailure("Queue is not empty! Queue size: \(self.responseQueue.count)")
        }
    }
}
