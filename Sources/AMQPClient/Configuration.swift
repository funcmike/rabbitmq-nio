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

import Foundation
import NIOSSL
import NIO

public enum Configuration {
    case tls(TLSConfiguration?, sniServerName: String?, Server)
    case plain(Server)

    public struct Server {
        let host: String
        let port: Int
        let user: String
        let password: String
        let vhost: String
        let timeout: TimeAmount
        let connectionName: String

        public init(host: String = "localhost", port: Int = 5672, user: String = "guest", password: String = "guest", vhost: String = "/", timeout: TimeAmount = TimeAmount.seconds(60), connectionName: String = ((Bundle.main.executablePath ?? "RabbitMQNIO") as NSString).lastPathComponent) {
            self.host = host
            self.port = port
            self.user = user
            self.password = password
            self.vhost = vhost
            self.timeout = timeout
            self.connectionName = connectionName
        }
    }
}
