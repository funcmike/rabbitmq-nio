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
    private var channelMax: UInt16?

    private let config: Configuration.Server

    init(config: Configuration.Server, initialQueueCapacity: Int = 3) {
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
                    guard let limit = channelMax else {
                        preconditionFailure("Required channelMax")
                    }

                    if let promise =  responseQueue.popFirst() {
                        promise.succeed(.connection(.connected(channelMax: limit)))
                    }
                    
                case .close(let close):
                    let closeOk = Frame.method(0, .connection(.closeOk))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(closeOk)), promise: nil)

                    self.shutdown(context: context, error: ClientError.connectionClosed(replyCode: close.replyCode, replyText: close.replyText))
                case .closeOk:
                    if let promise =  responseQueue.popFirst() {
                        promise.succeed(.connection(.closed))
                    }

                    self.shutdown(context: context, error: ClientError.connectionClosed())
                case .blocked, .unblocked:
                    //TODO: implement
                    return
                default:
                    return
                }
            case .channel(let channel):
                switch channel {
                case .close:
                    if let channel = self.channels.removeValue(forKey: channelID) {
                        channel.processFrame(frame: frame)
                    }

                    let closeOk = Frame.method(channelID, .channel(.closeOk))
                    context.writeAndFlush(self.wrapOutboundOut(.frame(closeOk)), promise: nil)
                case .closeOk:
                    if let channel = self.channels.removeValue(forKey: channelID) {
                        channel.processFrame(frame: frame)
                    }                       
                default:
                    if let channel = self.channels[channelID] {
                        channel.processFrame(frame: frame)
                    }
                }
            default:
                if let channel = self.channels[channelID] {
                    channel.processFrame(frame: frame)
                }
            }
        case .header(let channelID, _), .body(let channelID, _):
            if let channel = self.channels[channelID] {
                channel.processFrame(frame: frame)
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
            guard self.processChannelOpen(context: context, frame: frame, responsePromise: responsePromise, promise: promise) else {
                return
            }

            let channelID = frame.channelID

            if let promise = responsePromise {
                if channelID == 0  {
                    self.responseQueue.append(promise) 
                } else if let channel = self.channels[channelID] {
                    channel.append(promise: promise)
                }
            }
        case .bulk(let frames):            
            let channelID = frames.first?.channelID

            for frame in frames {
                guard self.processChannelOpen(context: context, frame: frame, responsePromise: responsePromise, promise: promise) else {
                    return
                }
            }

            if let promise = responsePromise, let id = channelID {
                if id == 0  {
                    self.responseQueue.append(promise)
                } else if let channel = self.channels[id] {
                    channel.append(promise: promise)
                }
            }
        case .bytes(_):
            if let promise = responsePromise {
                self.responseQueue.append(promise)
            }
        }

        return context.write(self.wrapOutboundOut(outbound), promise: promise)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        return self.shutdown(context: context, error: error)
    }

    private func processChannelOpen(context: ChannelHandlerContext, frame: Frame, responsePromise: EventLoopPromise<AMQPResponse>?, promise:  EventLoopPromise<Void>?) -> Bool {
        if case .method(let channelID, let method) = frame, case .channel(let channel) = method, case .open = channel {

            guard let limit = self.channelMax, (limit == 0 || self.channels.count < limit) else {
                responsePromise?.fail(ClientError.tooManyOpenedChannels)
                promise?.fail(ClientError.tooManyOpenedChannels)
                return false
            }

            self.channels[channelID] = AMQPChannelHandler(channelID: channelID)
        }

        return true
    }

    private func shutdown(context: ChannelHandlerContext, error: Error) {
        let queue = self.responseQueue
        self.responseQueue.removeAll()

        queue.forEach { $0.fail(error) }

        return context.close(promise: nil)
    }
}
