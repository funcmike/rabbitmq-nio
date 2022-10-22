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

import AMQPProtocol

public enum ClientError: Error {
    case alreadyShutdown
    case tooManyOpenedChannels
    case connectionClosed(replyCode: UInt16? = nil, replyText: String? = nil)
    case channelClosed(replyCode: UInt16? = nil, replyText: String? = nil)
    case consumerCanceled
    case connectionBlocked
    case invalidBody
    case invalidMessage
    case invalidResponse(AMQPResponse)
}

func preconditionUnexpectedFrame(_ frame: Frame) -> Never  {
    return preconditionFailure("Unexepected frame:\(frame)")
}

func preconditionUnexpectedChannel(_ channelID: Frame.ChannelID) -> Never  {
    return preconditionFailure("Unexepected channel: \(channelID)")
}
