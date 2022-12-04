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
import AMQPProtocol

public enum AMQPOutbound {
    case frame(Frame)
    case bulk([Frame])
    case bytes([UInt8])
}

protocol AMPQChannelHandlerParent {
    func write(frame: Frame, promise: EventLoopPromise<Void>?)
    func write(frames: [Frame], promise: EventLoopPromise<Void>?)
}

typealias OutboundCommandPayload = (AMQPOutbound, EventLoopPromise<AMQPResponse>?)

internal final class AMQPConnectionMultiplexHandler: ChannelInboundHandler {
    private enum State {
        case unblocked, blocked(Error), error(Error)
    }

    public typealias InboundIn = Frame
    public typealias OutboundOut = AMQPOutbound

    var context: ChannelHandlerContext!
    private var channels: [Frame.ChannelID: AMQPChannelHandler<AMQPConnectionMultiplexHandler>] = [:]
    private var channelMax: UInt16 = 0
    private var state: State = .unblocked
    private var responseQueue: Deque<EventLoopPromise<AMQPResponse>>

    private let config: AMQPConnectionConfiguration.Server

    init(config: AMQPConnectionConfiguration.Server, initialQueueCapacity: Int = 3) {
        self.config = config
        self.responseQueue = Deque(minimumCapacity: initialQueueCapacity)
    }

    func channelActive(context: ChannelHandlerContext) {       
        return context.fireChannelActive()
    }

    public func channelInactive(context: ChannelHandlerContext) {
        switch self.state {
        case .error(let error):
            return self.failAllResponses(because: error)
        default:
            return self.failAllResponses(because: AMQPClientError.connectionClosed())
        }
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        self.context = nil
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {        
        let frame = self.unwrapInboundIn(data)

        switch frame.payload {
        case .method(let method):
            switch method {
            case .connection(let connection):
                switch connection {
                case .start(_):
                    let clientProperties: Table = [
                        "connection_name": .longString(config.connectionName),
                        "product": .longString("rabbitmq-nio"),
                        "platform": .longString("Swift"),
                        "version":  .longString("0.1"),
                        "capabilities": .table([
                            "publisher_confirms":           .bool(true),
                            "exchange_exchange_bindings":   .bool(true),
                            "basic.nack":                   .bool(true),
                            "per_consumer_qos":             .bool(true),
                            "authentication_failure_close": .bool(true),
                            "consumer_cancel_notify":       .bool(true),
                            "connection.blocked":           .bool(true),
                        ])
                    ]

                    let startOk = Frame(channelID: frame.channelID, payload: .method(.connection(.startOk(.init(
                        clientProperties: clientProperties, mechanism: "PLAIN", response:"\u{0000}\(config.user)\u{0000}\(config.password)", locale: "en_US")))))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(startOk)), promise: nil)
                case .tune(let channelMax, let frameMax, let heartbeat):
                    self.channelMax = channelMax

                    let tuneOk: Frame = Frame(channelID: frame.channelID, payload: .method(.connection(.tuneOk(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat))))
                    let open: Frame = Frame(channelID: frame.channelID, payload: .method(.connection(.open(.init(vhost: config.vhost)))))

                    context.writeAndFlush(self.wrapOutboundOut(.bulk([tuneOk, open])), promise: nil)
                case .openOk:
                    if let promise = responseQueue.popFirst() {
                        promise.succeed(.connection(.connected(.init(channelMax: channelMax))))
                    }
                    
                case .close(let close):
                    let closeOk = Frame(channelID: frame.channelID, payload: .method(.connection(.closeOk)))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(closeOk)), promise: nil)

                    self.state = .error(AMQPClientError.connectionClosed(replyCode: close.replyCode, replyText: close.replyText))
                case .closeOk:
                    if let promise = responseQueue.popFirst() {
                        promise.succeed(.connection(.closed))
                    }
                case .blocked:
                    self.state = .blocked(AMQPClientError.connectionBlocked)
                case .unblocked:
                    self.state = .unblocked
                default:
                    preconditionUnexpectedFrame(frame)
                }
            case .channel(let channel):
                switch channel {
                case .openOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.opened(frame.channelID)))
                    }
                case .close(let close):
                    if let channel = self.channels.removeValue(forKey: frame.channelID) {
                        channel.close(error: AMQPClientError.channelClosed(replyCode: close.replyCode, replyText: close.replyText))
                    }

                    let closeOk = Frame(channelID: frame.channelID, payload: .method(.channel(.closeOk)))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(closeOk)), promise: nil)
                case .closeOk:
                    if let channel = self.channels.removeValue(forKey: frame.channelID) {
                        channel.receive(payload: frame.payload)
                        channel.close(error: AMQPClientError.channelClosed())
                    }
                case .flow(let active):
                    if let channel = self.channels.removeValue(forKey: frame.channelID) {
                        channel.receive(payload: frame.payload)
                    }

                    let flowOk = Frame(channelID: frame.channelID, payload: .method(.channel(.flowOk(active: active))))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(flowOk)), promise: nil)
                default:
                    if let channel = self.channels[frame.channelID] {
                        channel.receive(payload: frame.payload)
                    }
                }
            case .basic(let basic):
                switch basic {
                case .cancel(let cancel):
                    if let channel = self.channels[frame.channelID] {
                        channel.receive(payload: frame.payload)
                    }

                    let cancelOk = Frame(channelID: frame.channelID, payload: .method(.basic(.cancelOk(consumerTag: cancel.consumerTag))))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(cancelOk)), promise: nil)
                default:
                    if let channel = self.channels[frame.channelID] {
                        channel.receive(payload: frame.payload)
                    }            
                }
            default:
                if let channel = self.channels[frame.channelID] {
                    channel.receive(payload: frame.payload)
                }
            }
        case .header, .body(_):
            if let channel = self.channels[frame.channelID] {
                channel.receive(payload: frame.payload)
            }
        case .heartbeat:
            let heartbeat = Frame(channelID: frame.channelID, payload: .heartbeat)
            context.writeAndFlush(self.wrapOutboundOut(.frame(heartbeat)), promise: nil)
        }
    }

    func openChannel(id: Frame.ChannelID) -> EventLoopFuture<AMQPChannelHandler<AMQPConnectionMultiplexHandler>> {
        if let channel = self.channels[id] {
            return self.context.eventLoop.makeSucceededFuture(channel)
        }

        return self.write(outband: .frame(.init(channelID: id, payload: .method(.channel(.open(reserved1: ""))))))
            .flatMapThrowing  { response in 
                guard case .channel(let channel) = response, case .opened(let channelID) = channel, channelID == id else {
                    throw AMQPClientError.invalidResponse(response)
                }

                let channelHandler = AMQPChannelHandler(parent: self, channelID: channelID, eventLoop: self.context.eventLoop)

                self.channels[channelID] = channelHandler

                return channelHandler
            }
    }

    func start(initialSequence: [UInt8]) -> EventLoopFuture<AMQPResponse.Connection.Connected> {
        return self.write(outband: .bytes(initialSequence))
            .flatMapThrowing { response in
                guard case .connection(let connection) = response, case .connected(let connected) = connection else {
                    throw AMQPClientError.invalidResponse(response)
                }
                return connected
            }
    }

    func close(reason: String = "", code: UInt16 = 200) -> EventLoopFuture<Error?> {
        return self.write(outband: .frame(.init(channelID: 0, payload: .method(.connection(.close(.init(replyCode: code, replyText: reason, failingClassID: 0, failingMethodID: 0)))))))
            .map { response in
                guard case .connection(let connection) = response, case .closed = connection else {
                    return AMQPClientError.invalidResponse(response)
                }
                return nil
            }
            .recover { $0 }
    }

    private func write(outband: AMQPOutbound) -> EventLoopFuture<AMQPResponse> {
        switch self.state {
        case .error(let e), .blocked(let e): return self.context.eventLoop.makeFailedFuture(e)
        default:
            let promise = self.context.eventLoop.makePromise(of: AMQPResponse.self)
            
            let writeFuture = self.context.writeAndFlush(wrapOutboundOut(outband))
        
            writeFuture.whenFailure { promise.fail($0) }
            writeFuture.whenSuccess { self.responseQueue.append(promise) }

            return writeFuture.flatMap { promise.futureResult }
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.failAllResponses(because: error)
        return context.close(promise: nil)
    }

    private func failAllResponses(because error: Error) {
        self.state = .error(error)

        let queue = self.responseQueue
        self.responseQueue.removeAll()

        queue.forEach { $0.fail(error) }

        let channels = self.channels
        self.channels.removeAll()

        channels.forEach { $1.close(error: error) }
    }

    deinit {
        if !self.responseQueue.isEmpty {
            assertionFailure("AMQP Frame handler deinit when queue is not empty! Queue size: \(self.responseQueue.count)")
        }
    }
}

extension AMQPConnectionMultiplexHandler: AMPQChannelHandlerParent {
    func write(frame: Frame, promise: EventLoopPromise<Void>?) {
        switch self.state {
        case .error(let e), .blocked(let e): promise?.fail(e);
        default:
            return self.context.writeAndFlush(self.wrapOutboundOut(.frame(frame)), promise: promise)
        }
    }

    func write(frames: [Frame], promise: EventLoopPromise<Void>?) {
        switch self.state {
        case .error(let e), .blocked(let e): promise?.fail(e);
        default:
            return self.context.writeAndFlush(self.wrapOutboundOut(.bulk(frames)), promise: promise)
        }
    }
}
