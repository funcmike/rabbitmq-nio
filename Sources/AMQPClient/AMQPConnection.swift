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

import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import NIOSSL

public final class AMQPConnection: Sendable {
    internal enum ConnectionState {
        case open
        case shuttingDown
        case closed
    }

    public var isConnected: Bool {
        // `Channel.isActive` is set to false before the `closeFuture` resolves in cases where the channel might be
        // closed, or closing, before our state has been updated
        return channel.isActive && state.withLockedValue { $0 == .open }
    }

    public var closeFuture: NIOCore.EventLoopFuture<Void> { connectionHandler.channel.closeFuture }
    public var eventLoop: EventLoop { return connectionHandler.channel.eventLoop }
    private let frameMax: UInt32

    private let connectionHandler: AMQPConnectionHandler
    private var channel: NIOCore.Channel { connectionHandler.channel }

    private let state = NIOLockedValueBox(ConnectionState.open)
    private let channels: NIOLockedValueBox<AMQPChannels>

    init(connectionHandler: AMQPConnectionHandler, channelMax: UInt16, frameMax: UInt32) {
        self.connectionHandler = connectionHandler
        self.frameMax = frameMax
        channels = .init(AMQPChannels(channelMax: channelMax))
    }

    /// Connect to broker.
    /// - Parameters:
    ///     - eventLoop: EventLoop on which to connect.
    ///     - config: Configuration data.
    /// - Returns:  EventLoopFuture with AMQP Connection.
    public static func connect(use eventLoop: EventLoop, from config: AMQPConnectionConfiguration) -> EventLoopFuture<AMQPConnection> {
        eventLoop.flatSubmit {
            self.boostrapChannel(use: eventLoop, from: config).flatMap { connectionHandler in
                connectionHandler.startConnection().map {
                    AMQPConnection(
                        connectionHandler: connectionHandler,
                        channelMax: $0.channelMax,
                        frameMax: $0.frameMax
                    )
                }
            }
        }
    }

    /// Open new channel.
    /// Can be used only when connection is connected.
    /// Channel ID is automatically assigned (next free one).
    /// - Returns: EventLoopFuture with AMQP Channel.
    public func openChannel() -> EventLoopFuture<AMQPChannel> {
        guard isConnected else { return eventLoop.makeFailedFuture(AMQPConnectionError.connectionClosed()) }

        let channelID = channels.withLockedValue { $0.reserveNext() }

        guard let channelID = channelID else {
            return eventLoop.makeFailedFuture(AMQPConnectionError.tooManyOpenedChannels)
        }

        let future = connectionHandler.openChannel(id: channelID)

        future.whenFailure { _ in self.channels.withLockedValue { $0.remove(id: channelID) } }

        return future.map { channel in
            channel.closeFuture.whenComplete { _ in self.channels.withLockedValue { $0.remove(id: channelID) } }

            let amqpChannel = AMQPChannel(channelID: channelID, eventLoop: self.eventLoop, channel: channel, frameMax: self.frameMax)
            self.channels.withLockedValue { $0.add(channel: amqpChannel) }
            return amqpChannel
        }
    }

    /// Close a connection.
    /// - Parameters:
    ///     - reason: Reason that can be logged by broker.
    ///     - code: Code that can be logged by broker.
    /// - Returns: EventLoopFuture that is resolved when connection is closed.
    public func close(reason: String = "", code: UInt16 = 200) -> EventLoopFuture<Void> {
        let shouldClose = state.withLockedValue { state in
            if state == .open {
                state = .shuttingDown
                return true
            }

            return false
        }

        guard shouldClose else { return closeFuture }

        let result = connectionHandler.close(reason: reason, code: code)
            .map { () in
                nil as Error?
            }
            .recover { $0 }
            .flatMap { result in
                self.channel.close().map {
                    self.state.withLockedValue { $0 = .closed }
                    return (result, nil) as (Error?, Error?)
                }
                .recover { error in
                    if case ChannelError.alreadyClosed = error {
                        self.state.withLockedValue { $0 = .closed }
                        return (result, nil)
                    }

                    return (result, error)
                }
            }
        return result.flatMapThrowing {
            let (broker, conn) = $0
            if (broker ?? conn) != nil { throw AMQPConnectionError.connectionClose(broker: broker, connection: conn) }
            return ()
        }
    }

    private static func boostrapChannel(
        use eventLoop: EventLoop,
        from config: AMQPConnectionConfiguration
    ) -> EventLoopFuture<AMQPConnectionHandler> {
        do {
            let bootstrap = try boostrapClient(use: eventLoop, from: config)
            let multiplexer = NIOLoopBound(
                AMQPConnectionMultiplexHandler(eventLoop: eventLoop, config: config.server),
                eventLoop: eventLoop)

            return bootstrap
                .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .channelOption(ChannelOptions.socket(IPPROTO_TCP, TCP_NODELAY), value: 1)
                .connectTimeout(config.server.timeout)
                .channelInitializer { channel in
                    channel.pipeline.addHandlers([
                        MessageToByteHandler(AMQPFrameEncoder()),
                        ByteToMessageHandler(AMQPFrameDecoder()),
                        multiplexer.value,
                    ])
                }
                .connect(host: config.server.host, port: config.server.port)
                .map { AMQPConnectionHandler(channel: $0, multiplexer: multiplexer.value) }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }

    private static func boostrapClient(
        use eventLoopGroup: EventLoopGroup,
        from config: AMQPConnectionConfiguration
    ) throws -> NIOClientTCPBootstrap {
        guard let clientBootstrap = ClientBootstrap(validatingGroup: eventLoopGroup) else {
            preconditionFailure("Cannot create bootstrap for the supplied EventLoop")
        }

        switch config.connection {
        case .plain:
            return NIOClientTCPBootstrap(clientBootstrap, tls: NIOInsecureNoTLS())
        case let .tls(tls, sniServerName):
            let sslContext = try NIOSSLContext(configuration: tls ?? TLSConfiguration.clientDefault)
            let tlsProvider = try NIOSSLClientTLSProvider<ClientBootstrap>(context: sslContext, serverHostname: sniServerName ?? config.server.host)
            let bootstrap = NIOClientTCPBootstrap(clientBootstrap, tls: tlsProvider)
            return bootstrap.enableTLS()
        }
    }

    deinit {
        if isConnected {
            assertionFailure("close() was not called before deinit!")
        }
    }
}
