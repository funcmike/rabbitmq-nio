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
    private var nextMessage: (properties: Properties, body: [UInt8]?)?
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
            guard self.channelID == channelID else {
                preconditionFailure("Invalid channelID")
            }

            switch method {
            case .basic(let basic):
                switch basic {
                case .getOk(let get):
                    if  let promise = self.responseQueue.popFirst() , let msg = nextMessage {
                        promise.succeed(.channel(.message(.get(.init(
                            message: AMQPMessage.Delivery(
                                exchange: get.exchange,
                                routingKey: get.routingKey,
                                deliveryTag: get.deliveryTag,
                                properties: msg.properties,
                                redelivered: get.redelivered,
                                body: msg.body ?? [UInt8]()),
                            messageCount: get.messageCount)))))
                    }
                default:
                    return
                }
            case .channel(let channel):
                switch channel {   
                case .openOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.opened(channelID: channelID, closeFuture: closePromise.futureResult)))
                    }
                case .closeOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.closed(self.channelID)))
                        closePromise.succeed(())
                    }
                default:
                    return
                }
            default:
                return
            }
        case .header(let channelID, let header):
            guard self.channelID == channelID else {
                preconditionFailure("Invalid channelID")
            }
    
            self.nextMessage = (properties: header.properties, body:  nil)
        case .body(let channelID, let body):
            guard self.channelID == channelID else {
                preconditionFailure("Invalid channelID")
            }

            self.nextMessage?.body = body
        default:
            return
        }
    }
}
