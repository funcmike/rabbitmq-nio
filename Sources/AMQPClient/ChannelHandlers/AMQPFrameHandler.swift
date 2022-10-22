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

public typealias OutboundCommandPayload = (outbound: AMQPOutbound, responsePromise: EventLoopPromise<AMQPResponse>?)

internal final class AMQPFrameHandler: ChannelDuplexHandler  {
    public typealias InboundIn = Frame
    public typealias OutboundIn = OutboundCommandPayload
    public typealias OutboundOut = AMQPOutbound
    
    private var responseQueue: CircularBuffer<EventLoopPromise<AMQPResponse>>
    private var channels: [Frame.ChannelID: AMQPChannelHandler] = [:]
    private var channelMax: UInt16 = 0
    private var blocked: Bool = false

    private let config: AMQPClientConfiguration.Server

    init(config: AMQPClientConfiguration.Server, initialQueueCapacity: Int = 3) {
        self.responseQueue = CircularBuffer(initialCapacity: initialQueueCapacity)
        self.config = config
    }

    func channelActive(context: ChannelHandlerContext) {
        print("Client connected to \(context.remoteAddress as Any)")
        
        // `fireChannelActive` needs to be called BEFORE we set the state machine to connected,
        // since we want to make sure that upstream handlers know about the active connection before
        // it receives a         
        return context.fireChannelActive()
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

                    self.shutdown(context: context, error: AMQPClientError.connectionClosed(replyCode: close.replyCode, replyText: close.replyText))
                case .closeOk:
                    if let promise =  responseQueue.popFirst() {
                        promise.succeed(.connection(.closed))
                    }

                    self.shutdown(context: context, error: AMQPClientError.connectionClosed())
                case .blocked:
                    blocked = true
                case .unblocked:
                    blocked = false
                default:
                    preconditionUnexpectedFrame(frame)
                }
            case .channel(let channel):
                switch channel {
                case .close:
                    if let channel = self.channels.removeValue(forKey: channelID) {
                        channel.processIncomingFrame(frame: frame)
                    }

                    let closeOk = Frame.method(channelID, .channel(.closeOk))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(closeOk)), promise: nil)
                case .closeOk:
                    if let channel = self.channels.removeValue(forKey: channelID) {
                        channel.processIncomingFrame(frame: frame)
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
        let (outbound, responsePromise) = self.unwrapOutboundIn(data)

        switch outbound {
        case .frame(let frame):
            guard !blocked else {
                responsePromise?.fail(AMQPClientError.connectionBlocked)
                promise?.fail(AMQPClientError.connectionBlocked)
                return                
            }

            if case .method(let channelID, let method) = frame, case .channel(let channel) = method, case .open = channel {
                guard (self.channelMax == 0 || self.channels.count < self.channelMax) else {
                    responsePromise?.fail(AMQPClientError.tooManyOpenedChannels)
                    promise?.fail(AMQPClientError.tooManyOpenedChannels)
                    return
                }
                
                self.channels[channelID] = AMQPChannelHandler(channelID: channelID, closePromise: context.eventLoop.makePromise())
            }

            let channelID = frame.channelID

            if let responsePromise = responsePromise {
                if channelID == 0  {
                    self.responseQueue.append(responsePromise) 
                } else if let channel = self.channels[channelID] {
                    channel.addResponse(promise: responsePromise)
                } else {
                    responsePromise.fail(AMQPClientError.channelClosed())
                    promise?.fail(AMQPClientError.channelClosed())
                }
            }
        case .bulk(let frames):
            guard !blocked else {
                responsePromise?.fail(AMQPClientError.connectionBlocked)
                promise?.fail(AMQPClientError.connectionBlocked)
                return                
            }

            if let responsePromise = responsePromise, let id = frames.first?.channelID {
                if id == 0  {
                    self.responseQueue.append(responsePromise)
                } else if let channel = self.channels[id] {
                    channel.addResponse(promise: responsePromise)
                } else {
                    responsePromise.fail(AMQPClientError.channelClosed())
                    promise?.fail(AMQPClientError.channelClosed())
                }
            }
        case .bytes(_):
            guard !blocked else {
                responsePromise?.fail(AMQPClientError.connectionBlocked)
                return                
            }

            if let promise = responsePromise {
                self.responseQueue.append(promise)
            }
        }

        return context.write(self.wrapOutboundOut(outbound), promise: promise)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        return self.shutdown(context: context, error: error)
    }

    private func shutdown(context: ChannelHandlerContext, error: Error) {
        let queue = self.responseQueue
        self.responseQueue.removeAll()

        queue.forEach { $0.fail(error) }

        return context.close(promise: nil)
    }
}
