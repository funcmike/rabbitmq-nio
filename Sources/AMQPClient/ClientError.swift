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
    case `protocol`(ProtocolError)
    case alreadyShutdown
    case tooManyOpenedChannels
    case invalidResponse(AMQPResponse)
}
