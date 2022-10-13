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

public enum AMQPResponse {
    case channel(Channel)
    case connection(Connection)

    public enum Channel {
        case opened(channelID: Frame.ChannelID, closeFuture: EventLoopFuture<Void>)
        case closed(Frame.ChannelID)
        case message(AMQPMessage)
        case queue(Queue)
        case published

        public enum Queue {
            case declared(queueName: String, messageCount: UInt32, consumerCount: UInt32)
            case binded
            case purged(messageCount: UInt32)
            case deleted(messageCount: UInt32)
            case unbinded
        }
    }

    public enum Connection {
        case connected(channelMax: UInt16)
        case closed
    }
}
