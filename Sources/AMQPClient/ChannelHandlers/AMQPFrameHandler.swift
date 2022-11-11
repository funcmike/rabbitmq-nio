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

public enum AMQPOutbound {
    case frame(Frame)
    case bulk([Frame])
    case bytes([UInt8])
}

enum CommandPayload {
    case openChannel(Frame)
    case write(Frame.ChannelID, AMQPOutbound)
}

typealias OutboundCommandPayload = (CommandPayload, EventLoopPromise<AMQPResponse>?)

internal final class AMQPFrameHandler: ChannelDuplexHandler  {
    private enum State {
        case unblocked, blocked(Error), error(Error)
    }

    public typealias InboundIn = Frame
    public typealias OutboundIn = OutboundCommandPayload
    public typealias OutboundOut = AMQPOutbound
    
    private var responseQueue: CircularBuffer<EventLoopPromise<AMQPResponse>>
    private var channels: [Frame.ChannelID: AMQPChannelHandler] = [:]
    private var channelMax: UInt16 = 0
    private var state: State = .unblocked

    private let config: AMQPClientConfiguration.Server

    init(config: AMQPClientConfiguration.Server, initialQueueCapacity: Int = 3) {
        self.responseQueue = CircularBuffer(initialCapacity: initialQueueCapacity)
        self.config = config
    }

    func channelActive(context: ChannelHandlerContext) {
        print("Client connected to \(context.remoteAddress as Any)")
       
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

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {        
        let frame = self.unwrapInboundIn(data)

        switch frame {
        case .method(let channelID, let method):
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

                    let startOk = Frame.method(channelID, .connection(.startOk(.init(
                        clientProperties: clientProperties, mechanism: "PLAIN", response:"\u{0000}\(config.user)\u{0000}\(config.password)", locale: "en_US"))))
                    context.writeAndFlush(self.wrapOutboundOut(AMQPOutbound.frame(startOk)), promise: nil)
                case .tune(let channelMax, let frameMax, let heartbeat):
                    self.channelMax = channelMax

                    let tuneOk: Frame = Frame.method(0, .connection(.tuneOk(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat)))
                    let open: Frame = Frame.method(0, .connection(.open(.init(vhost: config.vhost))))

                    context.writeAndFlush(self.wrapOutboundOut(.bulk([tuneOk, open])), promise: nil)
                case .openOk:
                    if let promise =  responseQueue.popFirst() {
                        promise.succeed(.connection(.connected(channelMax: channelMax)))
                    }
                    
                case .close(let close):
                    let closeOk = Frame.method(0, .connection(.closeOk))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(closeOk)), promise: nil)

                    self.state = .error(AMQPClientError.connectionClosed(replyCode: close.replyCode, replyText: close.replyText))
                case .closeOk:
                    if let promise =  responseQueue.popFirst() {
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
                case .close(let close):
                    if let channel = self.channels.removeValue(forKey: channelID) {
                        channel.close(error: AMQPClientError.channelClosed(replyCode: close.replyCode, replyText: close.replyText))
                    }

                    let closeOk = Frame.method(channelID, .channel(.closeOk))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(closeOk)), promise: nil)
                case .closeOk:
                    if let channel = self.channels.removeValue(forKey: channelID) {
                        channel.processIncomingFrame(frame: frame)
                        channel.close(error: AMQPClientError.channelClosed())
                    }
                case .flow(let active):
                    if let channel = self.channels.removeValue(forKey: channelID) {
                        channel.processIncomingFrame(frame: frame)
                    }

                    let flowOk = Frame.method(0, .channel(.flowOk(active: active)))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(flowOk)), promise: nil)
                default:
                    if let channel = self.channels[channelID] {
                        channel.processIncomingFrame(frame: frame)
                    }
                }
            case .basic(let basic):
                switch basic {
                case .cancel(let cancel):
                    if let channel = self.channels[channelID] {
                        channel.processIncomingFrame(frame: frame)
                    }

                    let cancelOk = Frame.method(0, .basic(.cancelOk(consumerTag: cancel.consumerTag)))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(cancelOk)), promise: nil)
                default:
                    if let channel = self.channels[channelID] {
                        channel.processIncomingFrame(frame: frame)
                    }            
                }
            default:
                if let channel = self.channels[channelID] {
                    channel.processIncomingFrame(frame: frame)
                }
            }
        case .header(let channelID, _), .body(let channelID, _):
            if let channel = self.channels[channelID] {
                channel.processIncomingFrame(frame: frame)
            }
        case .heartbeat(let channelID):
            let heartbeat = Frame.heartbeat(channelID)
            context.writeAndFlush(self.wrapOutboundOut(.frame(heartbeat)), promise: nil)
        }
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let (command, responsePromise) = self.unwrapOutboundIn(data)

        switch self.state {
        case .error(let e), .blocked(let e): promise?.fail(e); responsePromise?.fail(e);
        default:
            let channelID: Frame.ChannelID
            let outband: AMQPOutbound

            switch command {
            case .openChannel(let frame):
                guard (self.channelMax == 0 || self.channels.count < self.channelMax) else {
                    responsePromise?.fail(AMQPClientError.tooManyOpenedChannels)
                    promise?.fail(AMQPClientError.tooManyOpenedChannels)
                    return
                }

                channelID = frame.channelID
                outband = .frame(frame)

                self.channels[channelID] = AMQPChannelHandler(channelID: channelID, closePromise: context.eventLoop.makePromise())
            case .write(let id, let out):
                channelID = id
                outband = out
            }

            if let responsePromise = responsePromise {
                if channelID == 0  {
                    self.responseQueue.append(responsePromise) 
                } else {
                    guard let channel = self.channels[channelID] else {
                        responsePromise.fail(AMQPClientError.channelClosed())
                        promise?.fail(AMQPClientError.channelClosed())
                        return
                    }

                    channel.addResponse(promise: responsePromise)
                }
            }

            return context.write(self.wrapOutboundOut(outband), promise: promise)
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
