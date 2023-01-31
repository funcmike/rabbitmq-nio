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

import NIOCore
import NIOPosix
import NIOSSL
import NIOConcurrencyHelpers
import AMQPProtocol

public final class AMQPConnection {
    internal enum ConnectionState {
        case open
        case shuttingDown
        case closed
    }

    public var eventLoop: EventLoop { return self.channel.eventLoop }
    public let channelMax: UInt16

    private let channel: NIOCore.Channel
    private let multiplexer: AMQPConnectionMultiplexHandler

    private let _lock = NIOLock()
    private var _state = ConnectionState.open
    private var state: ConnectionState {
        get { return _lock.withLock { self._state } }
        set(newValue) { _lock.withLockVoid { self._state = newValue } }
    }

    public var isConnected: Bool {
        // `Channel.isActive` is set to false before the `closeFuture` resolves in cases where the channel might be
        // closed, or closing, before our state has been updated
        return self.channel.isActive && self.state == .open
    }

    public var closeFuture: NIOCore.EventLoopFuture<Void> {
        get { return  self.channel.closeFuture }
    }

    private var _channels = AMQPChannels()

    init(channel: NIOCore.Channel, multiplexer: AMQPConnectionMultiplexHandler, channelMax: UInt16) {
        self.channel = channel
        self.multiplexer = multiplexer
        self.channelMax = channelMax
    }

    /// Connect to broker.
    /// - Parameters:
    ///     - eventLoop: EventLoop on which to connect.
    ///     - config: Configuration data.
    /// - Returns:  EventLoopFuture with Connection object.
    public static func connect(use eventLoop: EventLoop, from config: AMQPConnectionConfiguration) -> EventLoopFuture<AMQPConnection> {
        let promise = eventLoop.makePromise(of: AMQPResponse.self)
        let multiplexer = AMQPConnectionMultiplexHandler(config: config.server, onReady: promise)

        return eventLoop.flatSubmit { () -> EventLoopFuture<AMQPConnection> in
            let result = self.boostrapChannel(use: eventLoop, from: config, with: multiplexer).flatMap { channel in
                promise.futureResult.flatMapThrowing { response in
                    guard case .connection(let connection) = response, case .connected(let connected) = connection else {
                        throw AMQPConnectionError.invalidResponse(response)
                    }

                    return AMQPConnection(channel: channel, multiplexer: multiplexer, channelMax: connected.channelMax)
                }
            }

            result.whenFailure { err in multiplexer.failAllResponses(because: err) }
            return result
        }
    }

    /// Open new channel.
    /// Can be used only when connection is connected.
    /// - Returns: EventLoopFuture with AMQP Channel.
    public func openChannel() -> EventLoopFuture<AMQPChannel> {
        guard self.isConnected else { return self.eventLoop.makeFailedFuture(AMQPConnectionError.connectionClosed()) }

        guard let channelID = self._lock.withLock({ () -> UInt16? in
            self._channels.tryReserveAny(max: self.channelMax > 0 ? self.channelMax : UInt16.max) })  else {
            return self.eventLoop.makeFailedFuture(AMQPConnectionError.tooManyOpenedChannels)
        }

        return self.eventLoop.flatSubmit {
            let future = self.multiplexer.openChannel(id: channelID)

            future.whenFailure { _ in self._lock.withLock { self._channels.remove(id: channelID) } }
 
            return future.map { channel in
                let amqpChannel = AMQPChannel(channelID: channelID, eventLoop: self.eventLoop, channel: channel)
                self._lock.withLock { self._channels.add(channel: amqpChannel) }
                return amqpChannel
            }
        }
    }    

    /// Close a connection.
    /// - Parameters:
    ///     - reason: Reason that can be logged by broker.
    ///     - code: Code that can be logged by broker.
    /// - Returns: EventLoopFuture that is resolved when connection is closed.
    public func close(reason: String = "", code: UInt16 = 200) -> EventLoopFuture<Void> {
        guard self.isConnected else { return self.channel.closeFuture }

        self.state = .shuttingDown

        return self.eventLoop.flatSubmit {
            let result = self.multiplexer.close(reason: reason, code: code)
                .map { () in
                    return nil as Error?
                }
                .recover { $0 }
                .flatMap { result in
                    self.channel.close().map {
                        self.state = .closed
                        return (result, nil) as (Error?, Error?)
                    }
                    .recover { error in
                        if case ChannelError.alreadyClosed = error  {
                            self.state = .closed
                            return (result, nil)
                        }

                        return (result, error)
                    }
                }
            return result.flatMapThrowing {
                let (broker, conn) = $0
                if (broker ?? conn) != nil { throw AMQPConnectionError.close(broker: broker, connection: conn) }
                return ()
            }
        }
    }

    private static func boostrapChannel(
        use eventLoop: EventLoop,
        from config: AMQPConnectionConfiguration,
        with handler: AMQPConnectionMultiplexHandler
    ) -> EventLoopFuture<NIOCore.Channel> {
        let channelPromise = eventLoop.makePromise(of: NIOCore.Channel.self)

        do {
            let bootstrap = try boostrapClient(use: eventLoop, from: config)

            bootstrap
                .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .channelOption(ChannelOptions.socket(IPPROTO_TCP, TCP_NODELAY), value: 1)
                .connectTimeout(config.server.timeout)
                .channelInitializer { channel in
                    channel.pipeline.addHandlers([
                        MessageToByteHandler(AMQPFrameEncoder()),
                        ByteToMessageHandler(AMQPFrameDecoder()),
                        handler
                    ])
                }
                .connect(host: config.server.host, port: config.server.port)
                .map { channelPromise.succeed($0) }
                .cascadeFailure(to: channelPromise)
        } catch {
            channelPromise.fail(error)
        }

        return channelPromise.futureResult        
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
        case .tls(let tls, let sniServerName):
            let sslContext = try NIOSSLContext(configuration: tls ?? TLSConfiguration.makeClientConfiguration())
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
