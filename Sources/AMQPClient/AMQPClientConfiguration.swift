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

import NIOSSL
import NIOCore
import Foundation

public enum AMQPClientConfiguration {
    case tls(TLSConfiguration?, sniServerName: String?, Server)
    case plain(Server)
    
    var server: Server {
        switch self {
        case let .plain(s): return s
        case let .tls(_, _, s): return s
        }
    }
    
    public struct Server {
        var host: String
        var port: Int
        var user: String
        var password: String
        var vhost: String
        var timeout: TimeAmount
        var connectionName: String
        
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
}

@available(macOS 13.0, *)
public extension AMQPClientConfiguration {
    
    enum ValidationError: Error, Equatable {
        case invalidUrl
        case invalidScheme
    }
    
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
    
    init(url: String) throws {
        guard let url = URL(string: url) else { throw ValidationError.invalidUrl }
        try self.init(url: url)
    }
    
    init(url: URL) throws {
        guard let scheme = UrlScheme(rawValue: url.scheme ?? "") else { throw ValidationError.invalidScheme }
        
        // there is no such thing as a "" host
        let host = url.host?.isEmpty == true ? nil : url.host
        //special path magic for vhost interpretation (see https://www.rabbitmq.com/uri-spec.html)
        let vhost = url.path.isEmpty ? nil : String(url.path(percentEncoded: false).dropFirst())
        let server = Server(host: host, port: url.port ?? scheme.defaultPort, user: url.user, password: url.password?.removingPercentEncoding, vhost: vhost)
        
        switch scheme {
        case .amqp: self = .plain(server)
        case .amqps: self = .tls(nil, sniServerName: nil, server)
        }
    }
}

extension AMQPClientConfiguration.Server {
    struct Defaults {
        static let host = "localhost"
        static let port = 5672
        static let tlsPort = 5671
        static let user = "guest"
        static let password = "guest"
        static let vhost = "/"
        static var timeout: TimeAmount { TimeAmount.seconds(60) }
        //NOTE: seems like an unneccassary dependency on Foundation / Bundle / NSString
        static var connectionName: String { ((Bundle.main.executablePath ?? "RabbitMQNIO") as NSString).lastPathComponent }
    }
}
