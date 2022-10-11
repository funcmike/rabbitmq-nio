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

internal final class AMQPChannelHandler {
    private let channelID: Frame.ChannelID
    private var responseQueue: CircularBuffer<EventLoopPromise<AMQPResponse>>
    private var nextMessage: (getOk: Basic.GetOk, properties: Properties?)?
    private var closePromise: EventLoopPromise<Void>

    init(channelID: Frame.ChannelID, closePromise: EventLoopPromise<Void>, initialQueueCapacity: Int = 3) {
        self.channelID = channelID
        self.closePromise = closePromise
        self.responseQueue = CircularBuffer(initialCapacity: initialQueueCapacity)
    }

    func append(promise: EventLoopPromise<AMQPResponse>) {
        return self.responseQueue.append(promise)
    }

    func processFrame(frame: Frame) {
        switch frame {
        case .method(let channelID, let method): 
            guard self.channelID == channelID else { preconditionFailure("Invalid channelID") }

            switch method {
            case .basic(let basic):
                switch basic {
                case .getOk(let getOk):
                    self.nextMessage = (getOk: getOk, properties: nil)
                default:
                    return
                }
            case .channel(let channel):
                switch channel {   
                case .openOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.opened(channelID: channelID, closeFuture: closePromise.futureResult)))
                    }
                case .close(let close):
                    self.shutdown(error: ClientError.channelClosed(replyCode: close.replyCode, replyText: close.replyText))
                case .closeOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.closed(self.channelID)))
                    }
                    self.shutdown(error: ClientError.channelClosed())
                default:
                    return
                }
            default:
                return
            }
        case .header(let channelID, let header):
            guard self.channelID == channelID else { preconditionFailure("Invalid channelID") }

            self.nextMessage?.properties = header.properties
        case .body(let channelID, let body):
            guard self.channelID == channelID else { preconditionFailure("Invalid channelID") }

            if let promise = self.responseQueue.popFirst() {
                guard let msg = nextMessage, let properties = msg.properties else {
                    promise.fail(ClientError.invalidMessage)
                    return
                }

                promise.succeed(.channel(.message(.get(.init(
                    message: AMQPMessage.Delivery(
                        exchange: msg.getOk.exchange,
                        routingKey: msg.getOk.routingKey,
                        deliveryTag: msg.getOk.deliveryTag,
                        properties: properties,
                        redelivered: msg.getOk.redelivered,
                        body: body),
                    messageCount: msg.getOk.messageCount)))))

                nextMessage = nil
            }
        default:
            return
        }
    }

    func shutdown(error: Error) {
        let queue = self.responseQueue
        self.responseQueue.removeAll()

        queue.forEach { $0.fail(error) }
        closePromise.succeed(())
    }
}
