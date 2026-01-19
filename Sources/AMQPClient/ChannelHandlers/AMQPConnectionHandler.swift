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
import NIOCore

struct AMQPConnectionHandler: Sendable {
    let channel: Channel

    init(channel: Channel) {
        self.channel = channel
    }

    func write(
        _ outbound: AMQPOutbound,
        responsePromise: EventLoopPromise<AMQPResponse>?,
        writePromise: EventLoopPromise<Void>?
    ) {
        channel.writeAndFlush((outbound, responsePromise), promise: writePromise)
    }

    func startConnection() -> EventLoopFuture<AMQPResponse.Connection.Connected> {
        let promise = channel.eventLoop.makePromise(of: AMQPResponse.self)

        write(
            .bytes(PROTOCOL_START_0_9_1),
            responsePromise: promise,
            writePromise: nil
        )

        return promise.futureResult.flatMapThrowing { response in
            guard case let .connection(.connected(connected)) = response else {
                let error = AMQPConnectionError.invalidResponse(response)
                channel.triggerUserOutboundEvent(AMQPConnectionMultiplexHandler.UserEvent.invalidResponse(error), promise: nil)
                throw error
            }
            return connected
        }
    }

    func openChannel(id: Frame.ChannelID) -> EventLoopFuture<AMQPChannelHandler> {
        let promise = channel.eventLoop.makePromise(of: AMQPResponse.self)

        write(.frame(.init(channelID: id, payload: .method(.channel(.open(reserved1: ""))))),
              responsePromise: promise,
              writePromise: nil)

        return promise.futureResult.flatMap { response in
            guard case let .channel(.opened(channelId)) = response, channelId == id else {
                return channel.eventLoop.makeFailedFuture(AMQPConnectionError.invalidResponse(response))
            }

            let channelHandler = AMQPChannelHandler(parent: self, channelID: channelId)
            let promise = channel.eventLoop.makePromise(of: Void.self)
            channel
                .triggerUserOutboundEvent(
                    AMQPConnectionMultiplexHandler.UserEvent
                        .channelOpen(id, channelHandler),
                    promise: promise
                )

            return promise.futureResult.map { channelHandler }
        }
    }

    func close(reason: String = "", code: UInt16 = 200) -> EventLoopFuture<Void> {
        let responsePromise = channel.eventLoop.makePromise(of: AMQPResponse.self)

        write(.frame(.init(channelID: 0, payload: .method(.connection(.close(.init(replyCode: code, replyText: reason, failingClassID: 0, failingMethodID: 0)))))),
              responsePromise: responsePromise,
              writePromise: nil)

        return responsePromise.futureResult.flatMapThrowing { response in
            guard case .connection(.closed) = response else {
                throw AMQPConnectionError.invalidResponse(response)
            }
        }
    }
}
