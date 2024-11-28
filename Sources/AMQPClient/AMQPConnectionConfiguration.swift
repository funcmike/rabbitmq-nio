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

import struct Foundation.URL
import NIOSSL
import NIOCore

public struct AMQPConnectionConfiguration: Sendable {
    let connection: Connection
    let server: Server

    public enum Connection: Sendable {
        case tls(TLSConfiguration?, sniServerName: String?)
        case plain
    }

    public struct Server: Sendable {
        public var host: String
        public var port: Int
        public var user: String
        public var password: String
        public var vhost: String
        public var timeout: TimeAmount
        public var connectionName: String

        public init(host: String? = nil,
                    port: Int? = nil,
                    user: String? = nil,
                    password: String? = nil,
                    vhost: String? = nil,
                    timeout: TimeAmount? = nil,
                    connectionName: String? = nil) {

            self.host = host ?? Defaults.host
            self.port = port ?? Defaults.port
            self.user = user ?? Defaults.user
            self.password = password ?? (user == nil ? Defaults.password : "")
            self.vhost = vhost ?? Defaults.vhost
            self.timeout = timeout ?? Defaults.timeout
            self.connectionName = connectionName ?? Defaults.connectionName
        }
    }

    public init(connection: Connection, server: Server) {
        self.connection = connection
        self.server = server
    }
}

@available(macOS 13.0, *)
public extension AMQPConnectionConfiguration {
    enum UrlScheme: String {
        case amqp = "amqp"
        case amqps = "amqps"

        var defaultPort: Int {
            switch self {
            case .amqp: return Server.Defaults.port
            case .amqps: return Server.Defaults.tlsPort
            }
        }
    }

    /// Convenience init to create connection configuration from a URL string + key options.
    ///
    /// - Parameters:
    ///   - url: A string that contains the URL to create configuration from.
    ///   - tls: Optional TLS configuration. If `nil`, default will be used.
    ///   - sniServerName: Server name to use for TLS connection. If `nil`, default will be used.
    ///   - timeout: Optional connection timeout. If `nil`, default timeout will be used.
    ///   - connectionName: Optional connection name. If `nil`, default connection name will be used.
    /// - Throws: `AMQPConnectionError.invalidUrl` if URL is invalid, or `AMQPConnectionError.invalidUrlScheme` if URL scheme is not supported.
    init(url: String, tls: TLSConfiguration? = nil, sniServerName: String? = nil, timeout: TimeAmount? = nil, connectionName: String? = nil) throws {
        guard let url = URL(string: url) else { throw AMQPConnectionError.invalidUrl }
        try self.init(url: url, tls: tls, sniServerName: sniServerName, timeout: timeout, connectionName: connectionName)
    }

    /// Convenience init to create connection configuration from a URL + key options.
    ///
    /// - Parameters:
    ///   - url: A URL to create the configuration from.
    ///   - tls: Optional TLS configuration. If `nil`, default will be used.
    ///   - sniServerName: Server name to use for TLS connection. If `nil`, default will be used.
    ///   - timeout: Optional connection timeout. If `nil`, default timeout will be used.
    ///   - connectionName: Optional connection name. If `nil`, default connection name will be used.
    /// - Throws: `AMQPConnectionError.invalidUrlScheme` if URL scheme is not supported.
    init(url: URL, tls: TLSConfiguration? = nil, sniServerName: String? = nil, timeout: TimeAmount? = nil, connectionName: String? = nil) throws {
        guard let scheme = UrlScheme(rawValue: url.scheme ?? "") else { throw AMQPConnectionError.invalidUrlScheme }

        // there is no such thing as a "" host
        let host = url.host?.isEmpty == true ? nil : url.host?.removingPercentEncoding
        //special path magic for vhost interpretation (see https://www.rabbitmq.com/uri-spec.html)
        var vhost = url.path.isEmpty ? nil : String(url.path.removingPercentEncoding?.dropFirst() ?? "")

        // workaround: "/%f" is interpreted as / by URL (this restores %f as /)
        if url.absoluteString.lowercased().hasSuffix("%2f") {
            vhost = "/"
        }

        let server = Server(
            host: host, port: url.port ?? scheme.defaultPort,
            user: url.user?.removingPercentEncoding,password: url.password?.removingPercentEncoding,
            vhost: vhost, timeout: timeout, connectionName: connectionName
        )

        switch scheme {
        case .amqp: self = .init(connection: .plain, server: server)
        case .amqps: self = .init(connection: .tls(tls, sniServerName: sniServerName), server: server)
        }
    }
}

extension AMQPConnectionConfiguration.Server {
    struct Defaults {
        static let host = "localhost"
        static let port = 5672
        static let tlsPort = 5671
        static let user = "guest"
        static let password = "guest"
        static let vhost = "/"
        static var timeout: TimeAmount { TimeAmount.seconds(60) }
        static let connectionName = "RabbitMQNIO"
    }
}
