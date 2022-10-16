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

internal protocol Notifiable {
    func addConsumeListener(named name: String, listener: @escaping  AMQPListeners<AMQPMessage.Delivery>.Listener)
    func removeConsumeListener(named name: String)
}

internal final class AMQPChannelHandler: Notifiable {
    private let channelID: Frame.ChannelID
    private var responseQueue: CircularBuffer<EventLoopPromise<AMQPResponse>>
    private var nextMessage: (frame: Basic, properties: Properties?)?

    private var consumeListeners = AMQPListeners<AMQPMessage.Delivery>()

    init(channelID: Frame.ChannelID, initialQueueCapacity: Int = 3) {
        self.channelID = channelID
        self.responseQueue = CircularBuffer(initialCapacity: initialQueueCapacity)
    }

    func append(promise: EventLoopPromise<AMQPResponse>) {
        return self.responseQueue.append(promise)
    }

    func addConsumeListener(named name: String, listener: @escaping  AMQPListeners<AMQPMessage.Delivery>.Listener) {
        return self.consumeListeners.addListener(named: name, listener: listener)
    }

    func removeConsumeListener(named name: String) {
        return self.consumeListeners.removeListener(named: name)
    }

    func processFrame(frame: Frame) {
        switch frame {
        case .method(let channelID, let method): 
            guard self.channelID == channelID else { preconditionFailure("Invalid channelID") }

            switch method {
            case .basic(let basic):
                switch basic {
                case .getEmpty:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.message(.get())))
                    }
                case .deliver, .getOk:
                    self.nextMessage = (frame: basic, properties: nil)
                case .recoverOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.recovered)))
                    }
                case .qosOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.qosed)))
                    }
                case .consumeOk(let consumerTag):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.consumed(consumerTag: consumerTag))))
                    }                    
                default:
                    return
                }
            case .channel(let channel):
                switch channel {   
                case .openOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.opened(.init(channelID: channelID, notifier: self))))
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
            case .queue(let queue):
                switch queue {
                case .declareOk(let declareOk):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.queue(.declared(queueName: declareOk.queueName, messageCount: declareOk.messageCount, consumerCount: declareOk.consumerCount))))
                    }
                case .bindOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.queue(.binded)))
                    }
                case .purgeOk(let messageCount):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.queue(.purged(messageCount: messageCount))))
                    }
                case .deleteOk(let messageCount):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.queue(.deleted(messageCount: messageCount))))
                    }
                case .unbindOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.queue(.unbinded)))
                    }
                default:
                    return
                }
            case .exchange(let exchange):
                switch exchange {
                case .declareOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.exchange(.declared)))
                    }
                case .deleteOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.exchange(.deleted)))
                    }
                case .bindOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.exchange(.binded)))
                    }
                case .unbindOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.exchange(.unbinded)))
                    }
                default:
                    return
                }
            case .confirm(let confirm):
                switch confirm {
                case .selectOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.confirm(.selected)))
                    }
                default:
                    return
                }
            case .tx(let tx):
                switch tx {
                case .selectOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.tx(.selected)))
                    }
                case .commitOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.tx(.committed)))
                    }
                case .rollbackOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.tx(.rollbacked)))
                    }
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
                guard let msg = nextMessage, let properties = msg.properties else {
                    if let promise = self.responseQueue.popFirst() {
                        promise.fail(ClientError.invalidMessage)
                    }
                    return
                }

                switch msg.frame {
                    case .getOk(let getOk):
                            if let promise = self.responseQueue.popFirst() {
                                    promise.succeed(.channel(.message(.get(.init(
                                        message: AMQPMessage.Delivery(
                                            exchange: getOk.exchange,
                                            routingKey: getOk.routingKey,
                                            deliveryTag: getOk.deliveryTag,
                                            properties: properties,
                                            redelivered: getOk.redelivered,
                                            body: body),
                                        messageCount: getOk.messageCount)))))
                            }
                    case .deliver(let deliver):
                        consumeListeners.notify(named: deliver.consumerTag, .success(.init(
                            exchange: deliver.exchange,
                            routingKey: deliver.routingKey,
                            deliveryTag: deliver.deliveryTag,
                            properties: properties,
                            redelivered: deliver.redelivered,
                            body: body)))
                    default:
                        return
                }

                nextMessage = nil
        default:
            return
        }
    }

    func shutdown(error: Error) {
        let queue = self.responseQueue
        self.responseQueue.removeAll()

        queue.forEach { $0.fail(error) }
    }
}
