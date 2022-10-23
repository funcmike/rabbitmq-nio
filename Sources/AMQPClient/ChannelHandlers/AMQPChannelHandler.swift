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
    func addConsumeListener(named name: String, listener: @escaping  AMQPListeners<AMQPResponse.Channel.Message.Delivery>.Listener)
    func removeConsumeListener(named name: String)
    func addFlowListener(named name: String, listener: @escaping  AMQPListeners<Bool>.Listener)
    func removeFlowListener(named name: String)
    func addReturnListener(named name: String, listener: @escaping  AMQPListeners<AMQPResponse.Channel.Message.Return>.Listener)
    func removeReturnListener(named name: String)
    func addPublishListener(named name: String, listener: @escaping  AMQPListeners<AMQPResponse.Channel.Basic.PublishConfirm>.Listener)
    func removePublishListener(named name: String)
    var closeFuture: EventLoopFuture<Void> { get }

}

internal final class AMQPChannelHandler: Notifiable {
    var closeFuture: NIOCore.EventLoopFuture<Void> {
        get { return self.closePromise.futureResult }
    }

    private let channelID: Frame.ChannelID
    private var responseQueue: CircularBuffer<EventLoopPromise<AMQPResponse>>
    private var nextMessage: (frame: Basic, properties: Properties?)?
    var closePromise: NIOCore.EventLoopPromise<Void>

    private var consumeListeners = AMQPListeners<AMQPResponse.Channel.Message.Delivery>()
    private var flowListeners = AMQPListeners<Bool>()
    private var returnListeners = AMQPListeners<AMQPResponse.Channel.Message.Return>()
    private var publishListeners = AMQPListeners<AMQPResponse.Channel.Basic.PublishConfirm>()

    init(channelID: Frame.ChannelID, closePromise: NIOCore.EventLoopPromise<Void>, initialQueueCapacity: Int = 3) {
        self.channelID = channelID
        self.closePromise = closePromise
        self.responseQueue = CircularBuffer(initialCapacity: initialQueueCapacity)
    }

    func addResponse(promise: EventLoopPromise<AMQPResponse>) {
        return self.responseQueue.append(promise)
    }

    func addConsumeListener(named name: String, listener: @escaping  AMQPListeners<AMQPResponse.Channel.Message.Delivery>.Listener) {
        return self.consumeListeners.addListener(named: name, listener: listener)
    }

    func removeConsumeListener(named name: String) {
        return self.consumeListeners.removeListener(named: name)
    }

    func addFlowListener(named name: String, listener: @escaping AMQPListeners<Bool>.Listener) {
        return self.flowListeners.addListener(named: name, listener: listener)
    }

    func removeFlowListener(named name: String) {
        return self.flowListeners.removeListener(named: name)
    }

    func addReturnListener(named name: String, listener: @escaping AMQPListeners<AMQPResponse.Channel.Message.Return>.Listener) {
        return self.returnListeners.addListener(named: name, listener: listener)
    }

    func removeReturnListener(named name: String) {
        return self.returnListeners.removeListener(named: name)
    }

    func addPublishListener(named name: String, listener: @escaping AMQPListeners<AMQPResponse.Channel.Basic.PublishConfirm>.Listener) {
        return self.publishListeners.addListener(named: name, listener: listener)
    }

    func removePublishListener(named name: String) {
        return self.publishListeners.removeListener(named: name)
    }

    func processIncomingFrame(frame: Frame) {
        switch frame {
        case .method(let channelID, let method): 
            guard self.channelID == channelID else { preconditionUnexpectedChannel(channelID) }

            switch method {
            case .basic(let basic):
                switch basic {
                case .getEmpty:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.message(.get())))
                    }
                case .deliver, .getOk, .return:
                    self.nextMessage = (frame: basic, properties: nil)
                case .recoverOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.recovered)))
                    }
                case .qosOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.qosOk)))
                    }
                case .consumeOk(let consumerTag):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.consumeOk(.init(consumerTag: consumerTag)))))
                    }
                case .ack(let deliveryTag, let multiple):
                    self.publishListeners.notify(.success(.ack(deliveryTag: deliveryTag, multiple: multiple)))             
                case .nack(let nack):
                    self.publishListeners.notify(.success(.nack(deliveryTag: nack.deliveryTag, multiple: nack.multiple)))       
                case .cancel(let cancel):
                    self.consumeListeners.notify(named: cancel.consumerTag, .failure(AMQPClientError.consumerCanceled))
                case .cancelOk(let consumerTag):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.basic(.canceled)))
                    }

                    self.consumeListeners.notify(named: consumerTag, .failure(AMQPClientError.consumerCanceled))
                default:
                    preconditionUnexpectedFrame(frame)
                }
            case .channel(let channel):
                switch channel {   
                case .openOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.opened(.init(channelID: channelID, notifier: self))))
                    }
                case .close(let close):
                    self.shutdown(error: AMQPClientError.channelClosed(replyCode: close.replyCode, replyText: close.replyText))
                case .closeOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.closed(self.channelID)))
                    }
                    self.shutdown(error: AMQPClientError.channelClosed())
                case .flow(let active):
                    self.flowListeners.notify(.success(active))
                case .flowOk(let active):
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.flowed(active: active)))
                    }
                default:
                    preconditionUnexpectedFrame(frame)
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
                    preconditionUnexpectedFrame(frame)
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
                    preconditionUnexpectedFrame(frame)
                }
            case .confirm(let confirm):
                switch confirm {
                case .selectOk:
                    if let promise = self.responseQueue.popFirst() {
                        promise.succeed(.channel(.confirm(.selected)))
                    }
                default:
                    preconditionUnexpectedFrame(frame)
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
                    preconditionUnexpectedFrame(frame)  
                }
            default:
                preconditionUnexpectedFrame(frame)
            }
        case .header(let channelID, let header):
            guard self.channelID == channelID else { preconditionUnexpectedChannel(channelID) }

            self.nextMessage?.properties = header.properties
        case .body(let channelID, let body):
            guard self.channelID == channelID else { preconditionUnexpectedChannel(channelID) }
                guard let msg = nextMessage, let properties = msg.properties else {
                    if let promise = self.responseQueue.popFirst() {
                        promise.fail(AMQPClientError.invalidMessage)
                    }
                    return
                }

                switch msg.frame {
                    case .getOk(let getOk):
                            if let promise = self.responseQueue.popFirst() {
                                    promise.succeed(.channel(.message(.get(.init(
                                        message: AMQPResponse.Channel.Message.Delivery(
                                            exchange: getOk.exchange,
                                            routingKey: getOk.routingKey,
                                            deliveryTag: getOk.deliveryTag,
                                            properties: properties,
                                            redelivered: getOk.redelivered,
                                            body: body),
                                        messageCount: getOk.messageCount)))))
                            }
                    case .deliver(let deliver):
                        self.consumeListeners.notify(named: deliver.consumerTag, .success(.init(
                            exchange: deliver.exchange,
                            routingKey: deliver.routingKey,
                            deliveryTag: deliver.deliveryTag,
                            properties: properties,
                            redelivered: deliver.redelivered,
                            body: body)))
                    case .return(let `return`):
                        self.returnListeners.notify(.success(.init(
                            replyCode: `return`.replyCode,
                            replyText: `return`.replyText,
                            exchange: `return`.exchange,
                            routingKey: `return`.routingKey,
                            properties: properties,
                            body: body)))                   
                    default:
                        preconditionUnexpectedFrame(frame)
                }

                nextMessage = nil
        default:
            preconditionUnexpectedFrame(frame)
        }
    }

    func shutdown(error: Error) {
        let queue = self.responseQueue
        self.responseQueue.removeAll()

        queue.forEach { $0.fail(error) }

        closePromise.fail(error)
    }

    deinit {
        if !self.responseQueue.isEmpty {
            assertionFailure("AMQP Channel Handler deinit when queue is not empty! Queue size: \(self.responseQueue.count)")
        }
    }
}
