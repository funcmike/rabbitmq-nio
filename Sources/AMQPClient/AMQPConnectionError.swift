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

public enum AMQPConnectionError: Error, Sendable {
    case invalidUrl
    case invalidUrlScheme
    case connectionClosed(replyCode: UInt16? = nil, replyText: String? = nil)
    case connectionClose(broker: Error? = nil, connection: Error? = nil)
    case connectionBlocked
    case channelClosed(replyCode: UInt16? = nil, replyText: String? = nil)
    case tooManyOpenedChannels
    case channelNotInConfirmMode
    case consumerCancelled
    case consumerAlreadyCancelled
    case invalidMessage
    case invalidResponse(AMQPResponse)
}

func preconditionUnexpectedFrame(_ frame: Frame) -> Never  {
    return preconditionFailure("Unexepected frame:\(frame)")
}

func preconditionUnexpectedPayload(_ payload: Frame.Payload) -> Never  {
    return preconditionFailure("Unexepected payload:\(payload)")
}

func preconditionUnexpectedType(_ type: Any.Type) -> Never  {
    return preconditionFailure("Unexpected type \(type)")
}
