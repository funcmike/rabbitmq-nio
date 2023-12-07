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

    var channelId: Frame.ChannelID? {
        switch self {
        case let .frame(frame): return frame.channelID
        case let .bulk(frames): return frames.first?.channelID
        case .bytes: return .init(0)
        }
    }
}

internal final class AMQPConnectionMultiplexHandler: ChannelDuplexHandler {
    typealias InboundIn = Frame
    typealias OutboundIn = (AMQPOutbound, EventLoopPromise<AMQPResponse>?)
    typealias OutboundOut = AMQPOutbound

    private enum State {
        case unblocked, blocked(Error), error(Error)
    }

    fileprivate final class ChannelState {
        // NOTE: this can be extended to keep some state of the open request so a response can be verified against its request
        var pendingRequests: Deque<EventLoopPromise<AMQPResponse>>
        weak var eventHandler: AMQPChannelHandler?
        var nextMessage: (frame: Frame.Method.Basic, properties: Properties?, bodySize: UInt64?, prevBody: ByteBuffer?)?

        init(initialResponsePromise: EventLoopPromise<AMQPResponse>) {
            pendingRequests = .init([initialResponsePromise])
        }
    }

    private let eventLoop: EventLoop
    private var context: ChannelHandlerContext!
    private var channels: [Frame.ChannelID: ChannelState] = [:]
    private var channelMax: UInt16 = 0
    private var frameMax: UInt32 = 0
    private var state: State = .unblocked

    private let config: AMQPConnectionConfiguration.Server

    init(eventLoop: EventLoop, config: AMQPConnectionConfiguration.Server) {
        self.config = config
        self.eventLoop = eventLoop
    }

    func addChannelHandler(_ handler: AMQPChannelHandler, forId id: Frame.ChannelID) {
        eventLoop.assertInEventLoop()

        guard let channel = channels[id] else { preconditionFailure() }
        precondition(channel.eventHandler == nil)
        channel.eventHandler = handler
    }

    public func channelInactive(context _: ChannelHandlerContext) {
        switch state {
        case let .error(error):
            return failAllPendingRequestsAndChannels(because: error)
        default:
            return failAllPendingRequestsAndChannels(because: AMQPConnectionError.connectionClosed())
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
        guard let channel = channels[frame.channelID] else {
            // TODO: close channel with error
            assertionFailure("unexpected frame received")
            return
        }

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
                    self.frameMax = frameMax

                    let tuneOk = Frame(channelID: frame.channelID,
                                       payload: .method(.connection(.tuneOk(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat))))
                    let open = Frame(channelID: frame.channelID, payload: .method(.connection(.open(.init(vhost: config.vhost)))))

                    context.writeAndFlush(wrapOutboundOut(.bulk([tuneOk, open])), promise: nil)
                case .openOk:
                    channel.fulfilNextPendingRequest(with: .connection(.connected(.init(channelMax: channelMax, frameMax: frameMax))))
                case let .close(close):
                    let closeOk = Frame(channelID: frame.channelID, payload: .method(.connection(.closeOk)))
                    context.writeAndFlush(wrapOutboundOut(.frame(closeOk)), promise: nil)

                    state = .error(AMQPConnectionError.connectionClosed(replyCode: close.replyCode, replyText: close.replyText))
                case .closeOk:
                    channel.fulfilNextPendingRequest(with: .connection(.closed))
                case .blocked:
                    state = .blocked(AMQPConnectionError.connectionBlocked)
                case .unblocked:
                    state = .unblocked
                default:
                    // TODO: take down channel
                    preconditionUnexpectedFrame(frame)
                }
            case let .channel(channelMessage):
                switch channelMessage {
                case .openOk:
                    channel.fulfilNextPendingRequest(with: .channel(.opened(frame.channelID)))
                case let .close(close):
                    channels.removeValue(forKey: frame.channelID)
                    channel.reportAsClosed(error: AMQPConnectionError.channelClosed(replyCode: close.replyCode, replyText: close.replyText))

                    let closeOk = Frame(channelID: frame.channelID, payload: .method(.channel(.closeOk)))
                    context.writeAndFlush(wrapOutboundOut(.frame(closeOk)), promise: nil)
                case .closeOk:
                    channel.fulfilNextPendingRequest(with: .channel(.closed(frame.channelID)))
                    channels.removeValue(forKey: frame.channelID)
                    channel.reportAsClosed()
                case let .flow(active):
                    channel.eventHandler?.receiveFlow(active)

                    let flowOk = Frame(channelID: frame.channelID, payload: .method(.channel(.flowOk(active: active))))
                    context.writeAndFlush(wrapOutboundOut(.frame(flowOk)), promise: nil)
                case let .flowOk(active):
                    channel.fulfilNextPendingRequest(with: .channel(.flowed(.init(active: active))))
                default:
                    // TODO: take down channel
                    preconditionUnexpectedFrame(frame)
                }
            case let .queue(queue):
                switch queue {
                case let .declareOk(declareOk):
                    channel.fulfilNextPendingRequest(with: .channel(.queue(.declared(
                        .init(queueName: declareOk.queueName,
                              messageCount: declareOk.messageCount,
                              consumerCount: declareOk.consumerCount)))))

                case .bindOk:
                    channel.fulfilNextPendingRequest(with: .channel(.queue(.binded)))
                case let .purgeOk(messageCount):
                    channel.fulfilNextPendingRequest(with: .channel(.queue(.purged(.init(messageCount: messageCount)))))
                case let .deleteOk(messageCount):
                    channel.fulfilNextPendingRequest(with: .channel(.queue(.deleted(.init(messageCount: messageCount)))))
                case .unbindOk:
                    channel.fulfilNextPendingRequest(with: .channel(.queue(.unbinded)))
                default:
                    // TODO: take down channel
                    preconditionUnexpectedFrame(frame)
                }
            case let .basic(basic):
                switch basic {
                case .getEmpty:
                    channel.fulfilNextPendingRequest(with: .channel(.message(.get())))
                case .deliver, .getOk, .return:
                    // TODO: wrap this away more nicely, assert message must be nil
                    channel.nextMessage = (frame: basic, nil, nil, nil)
                case .recoverOk:
                    channel.fulfilNextPendingRequest(with: .channel(.basic(.recovered)))
                case let .consumeOk(consumerTag):
                    channel.fulfilNextPendingRequest(with: .channel(.basic(.consumeOk(.init(consumerTag: consumerTag)))))
                case let .cancelOk(consumerTag):
                    channel.fulfilNextPendingRequest(with: .channel(.basic(.canceled)))
                    channel.eventHandler?.handleCancellation(consumerTag: consumerTag)
                case .qosOk:
                    channel.fulfilNextPendingRequest(with: .channel(.basic(.qosOk)))
                case let .cancel(cancel):
                    channel.eventHandler?.handleCancellation(consumerTag: cancel.consumerTag)

                    let cancelOk = Frame(channelID: frame.channelID, payload: .method(.basic(.cancelOk(consumerTag: cancel.consumerTag))))
                    context.writeAndFlush(wrapOutboundOut(.frame(cancelOk)), promise: nil)
                case let .ack(deliveryTag, multiple):
                    channel.eventHandler?.receivePublishConfirm(.ack(deliveryTag: deliveryTag, multiple: multiple))
                case let .nack(nack):
                    channel.eventHandler?.receivePublishConfirm(.nack(deliveryTag: nack.deliveryTag, multiple: nack.multiple))
                default:
                    // TODO: take down channel
                    preconditionUnexpectedFrame(frame)
                }
            case let .exchange(exchange):
                switch exchange {
                case .declareOk:
                    channel.fulfilNextPendingRequest(with: .channel(.exchange(.declared)))
                case .deleteOk:
                    channel.fulfilNextPendingRequest(with: .channel(.exchange(.deleted)))
                case .bindOk:
                    channel.fulfilNextPendingRequest(with: .channel(.exchange(.binded)))
                case .unbindOk:
                    channel.fulfilNextPendingRequest(with: .channel(.exchange(.unbinded)))
                default:
                    // TODO: take down channel
                    preconditionUnexpectedFrame(frame)
                }
            case let .confirm(confirm):
                switch confirm {
                case .selectOk:
                    channel.fulfilNextPendingRequest(with: .channel(.confirm(.selected)))
                default:
                    // TODO: take down channel
                    preconditionUnexpectedFrame(frame)
                }
            case let .tx(tx):
                switch tx {
                case .selectOk:
                    channel.fulfilNextPendingRequest(with: .channel(.tx(.selected)))
                case .commitOk:
                    channel.fulfilNextPendingRequest(with: .channel(.tx(.committed)))
                case .rollbackOk:
                    channel.fulfilNextPendingRequest(with: .channel(.tx(.rollbacked)))
                default:
                    // TODO: take down channel
                    preconditionUnexpectedFrame(frame)
                }
            }
        case let .header(header):
            channel.nextMessage?.properties = header.properties
            channel.nextMessage?.bodySize = header.bodySize
        case let .body(body):
            guard let msg = channel.nextMessage, let properties = msg.properties, let bodySize = msg.bodySize else {
                // TODO: take down channel
                return
            }

            let prevSize = msg.prevBody?.readableBytes ?? 0
            if (prevSize + body.readableBytes < bodySize) {
                if var prevBody = msg.prevBody  {
                    prevBody.writeImmutableBuffer(body)
                    channel.nextMessage?.prevBody = prevBody
                } else {
                    channel.nextMessage?.prevBody = body
                }

                return
            }
            
            let completeBody: ByteBuffer
            
            if var prevBody = msg.prevBody {
                prevBody.writeImmutableBuffer(body)
                completeBody = prevBody
            } else {
                completeBody = body
            }

            switch msg.frame {
            case let .getOk(getOk):
                channel.fulfilNextPendingRequest(with: .channel(.message(.get(.init(
                    message: .init(
                        exchange: getOk.exchange,
                        routingKey: getOk.routingKey,
                        deliveryTag: getOk.deliveryTag,
                        properties: properties,
                        redelivered: getOk.redelivered,
                        body: completeBody
                    ),
                    messageCount: getOk.messageCount
                )))))
            case let .deliver(deliver):
                channel.eventHandler?.receiveDelivery(
                    .init(
                        exchange: deliver.exchange,
                        routingKey: deliver.routingKey,
                        deliveryTag: deliver.deliveryTag,
                        properties: properties,
                        redelivered: deliver.redelivered,
                        body: completeBody
                    ),
                    for: deliver.consumerTag
                )
            case let .return(`return`):
                channel.eventHandler?.receiveReturn(.init(
                    replyCode: `return`.replyCode,
                    replyText: `return`.replyText,
                    exchange: `return`.exchange,
                    routingKey: `return`.routingKey,
                    properties: properties,
                    body: completeBody
                ))
            default:
                // TODO: take down channel
                preconditionUnexpectedFrame(frame)
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
        case .unblocked:
            if let responsePromise {
                guard let channelId = outbound.channelId else { preconditionFailure("Frames without a channelId cannot wait for a response.") }

                if let channel = channels[channelId] {
                    channel.pendingRequests.append(responsePromise)
                } else {
                    channels[channelId] = .init(initialResponsePromise: responsePromise)
                }
            }

            context.write(wrapOutboundOut(outbound), promise: promise)
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        failAllPendingRequestsAndChannels(because: error)
        return context.close(promise: nil)
    }

    func failAllPendingRequestsAndChannels(because error: Error) {
        state = .error(error)

        let channels = self.channels
        self.channels.removeAll()

        channels.values.forEach { $0.reportAsClosed(error: error) }
    }

    deinit {
        let allPending = self.channels.values.flatMap(\.pendingRequests)
        if !allPending.isEmpty {
            assertionFailure("Queue is not empty! Queue size: \(allPending.count)")
        }
    }
}

extension AMQPConnectionMultiplexHandler.ChannelState {
    /// Succeeds the promise of the next pending request in the queue with a response.
    func fulfilNextPendingRequest(with response: AMQPResponse) {
        guard let promise = pendingRequests.popFirst() else {
            // TODO: treat as error, take channel down
            return
        }

        promise.succeed(response)
    }

    /// Fails all pending requests and forwards closed state to all attached event handlers.
    func reportAsClosed(error: Error? = nil) {
        pendingRequests.forEach { $0.fail(error ?? AMQPConnectionError.channelClosed()) }
        eventHandler?.reportAsClosed(error: error)
    }
}
