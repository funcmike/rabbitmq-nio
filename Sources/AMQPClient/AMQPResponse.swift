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
        case published
    }

    public enum Connection {
        case connected(channelMax: UInt16)
        case closed
    }
}
