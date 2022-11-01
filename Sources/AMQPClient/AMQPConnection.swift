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
import NIOSSL
import NIOConcurrencyHelpers
import AMQPProtocol

internal final class AMQPConnection {
    internal enum ConnectionState {
        case open
        case shuttingDown
        case closed
        
        var isConnected: Bool {
            switch self {
            case .open: return true
            default: return false
            }
        }
    }

    private let channel: NIO.Channel
    private let eventLoopGroup: EventLoopGroup

    private let _stateLock = NIOLock()
    private var _state = ConnectionState.open
    private var state: ConnectionState {
        get { return _stateLock.withLock { self._state } }
        set(newValue) { _stateLock.withLockVoid { self._state = newValue } }
    }

    public var isConnected: Bool {
        // `Channel.isActive` is set to false before the `closeFuture` resolves in cases where the channel might be
        // closed, or closing, before our state has been updated
        return self.channel.isActive && self.state.isConnected
    }

    var closeFuture: NIOCore.EventLoopFuture<Void> {
        get { return  self.channel.closeFuture }
    }

    init(channel: NIO.Channel, eventLoopGroup: EventLoopGroup) {
        self.channel = channel
        self.eventLoopGroup = eventLoopGroup
    }

    static func create(use eventLoopGroup: EventLoopGroup, from config: AMQPClientConfiguration) -> EventLoopFuture<AMQPConnection> {
        return self.boostrapChannel(use: eventLoopGroup, from: config)
            .map { AMQPConnection(channel: $0, eventLoopGroup: eventLoopGroup) }
    }

    static func boostrapChannel(use eventLoopGroup: EventLoopGroup, from config: AMQPClientConfiguration) -> EventLoopFuture<NIO.Channel> {
        let eventLoop = eventLoopGroup.next()
        let channelPromise = eventLoop.makePromise(of: NIO.Channel.self)
        let serverConfig: AMQPClientConfiguration.Server
    
        switch config {
        case .tls(_, _, let server):
            serverConfig = server
        case .plain(let server):
            serverConfig = server
        }

        do {
            let bootstrap = try boostrapClient(use: eventLoopGroup, from: config)

            bootstrap
                .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .channelOption(ChannelOptions.socket(IPPROTO_TCP, TCP_NODELAY), value: 1)
                .connectTimeout(serverConfig.timeout)
                .channelInitializer { channel in
                    channel.pipeline.addHandlers([
                        MessageToByteHandler(AMQPFrameEncoder()),
                        ByteToMessageHandler(AMQPFrameDecoder()),
                        AMQPFrameHandler(config: serverConfig)
                    ])
                }
                .connect(host: serverConfig.host, port: serverConfig.port)
                .map { channelPromise.succeed($0) }
                .cascadeFailure(to: channelPromise)
        } catch {
            channelPromise.fail(error)
        }

        return channelPromise.futureResult        
    }

    static func boostrapClient(use eventLoopGroup: EventLoopGroup, from config: AMQPClientConfiguration) throws -> NIOClientTCPBootstrap {
        guard let clientBootstrap = ClientBootstrap(validatingGroup: eventLoopGroup) else {
            preconditionFailure("Cannot create bootstrap for the supplied EventLoop")
        }

        switch config {            
        case .plain(_): 
            return NIOClientTCPBootstrap(clientBootstrap, tls: NIOInsecureNoTLS())
        case .tls(let tls, let sniServerName, let server):
            let sslContext = try NIOSSLContext(configuration: tls ?? TLSConfiguration.makeClientConfiguration())
            let tlsProvider = try NIOSSLClientTLSProvider<ClientBootstrap>(context: sslContext, serverHostname: sniServerName ?? server.host)
            let bootstrap = NIOClientTCPBootstrap(clientBootstrap, tls: tlsProvider)
            return bootstrap.enableTLS()
        }        
    }

    func openChannel(frame: Frame, immediate: Bool = false) -> EventLoopFuture<AMQPResponse> {
        return self.write(command: .openChannel(frame), immediate: immediate)
    }

    func write(channelID: Frame.ChannelID, outbound: AMQPOutbound, immediate: Bool = false) -> EventLoopFuture<AMQPResponse> {
        return self.write(command: .write(channelID, outbound), immediate: immediate)
    }

    func write(channelID: Frame.ChannelID, outbound: AMQPOutbound, immediate: Bool = false) -> EventLoopFuture<Void> {
        return self.write(command: .write(channelID, outbound), immediate: immediate)
    }

    private func write(command: CommandPayload, immediate: Bool = false) -> EventLoopFuture<AMQPResponse> {
        guard self.isConnected else { return self.eventLoopGroup.any().makeFailedFuture(AMQPClientError.connectionClosed()) }

        let eventLoop = self.eventLoopGroup.any()
        let promise = eventLoop.makePromise(of: AMQPResponse.self)
        let outboundData: OutboundCommandPayload = (command, promise)

        let writeFuture = immediate ? self.channel.writeAndFlush(outboundData) : self.channel.write(outboundData)

        return writeFuture
            .flatMap{ promise.futureResult }
            .hop(to: eventLoop)
    }

    private func write(command: CommandPayload, immediate: Bool = false) -> EventLoopFuture<Void> {
        guard self.isConnected else { return self.eventLoopGroup.any().makeFailedFuture(AMQPClientError.connectionClosed()) }

        let outboundData: OutboundCommandPayload = (command, nil)
        return immediate ? self.channel.writeAndFlush(outboundData) : self.channel.write(outboundData)
    }

    func close() -> EventLoopFuture<Void> {
        guard self.isConnected else { return self.channel.closeFuture }

        self.state = .shuttingDown

        let result = self.channel.close()
        result.whenSuccess { self.state = .closed }
        return result
    }
}
