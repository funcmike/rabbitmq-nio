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
        
        var isConnected: Bool {
            switch self {
            case .open: return true
            default: return false
            }
        }
    }

    public var eventLoop: EventLoop { return self.channel.eventLoop }
    public let channelMax: UInt16

    private let channel: NIOCore.Channel
    private let multiplexer: AMQPConnectionMultiplexHandler

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

    private var channels = AMQPChannels()

    init(channel: NIOCore.Channel, multiplexer: AMQPConnectionMultiplexHandler, channelMax: UInt16) {
        self.channel = channel
        self.multiplexer = multiplexer
        self.channelMax = channelMax
    }

    /// Connect to broker.
    /// - Parameters:
    ///     - eventLoop: EventLoop on which to conntec.
    ///     - config: Configuration
    /// - Returns:  EventLoopFuture with Connection object.
    public static func connect(use eventLoop: EventLoop, from config: AMQPConnectionConfiguration) -> EventLoopFuture<AMQPConnection> {
        let multiplexer = AMQPConnectionMultiplexHandler(config: config.server)

        return self.boostrapChannel(use: eventLoop, from: config, with: multiplexer)
            .flatMap { channel in
                multiplexer.start(initialSequence: PROTOCOL_START_0_9_1)
                .map {  AMQPConnection(channel: channel, multiplexer: multiplexer, channelMax: $0.channelMax) }
            }
    }

    /// Open new channel.
    /// Can be used only when connection is connected.
    /// - Parameters:
    ///     - id: Channel Identifer must be unique and greater then 0 if empty auto assign
    /// - Returns: EventLoopFuture with AMQP Channel.
    public func openChannel(id: UInt16? = nil) -> EventLoopFuture<AMQPChannel> {
        guard self.isConnected else { return self.eventLoop.makeFailedFuture(AMQPConnectionError.connectionClosed()) }

        if let id = id {
            if let channel = self.channels.get(id: id) {
                return self.eventLoop.makeSucceededFuture(channel)
            }

            guard self.channels.tryReserve(id: id) else {
                return self.eventLoop.makeFailedFuture(AMQPConnectionError.channelAlreadyReserved)
            }
        }

        guard let channelID = id ?? self.channels.tryReserveAny(max: self.channelMax > 0 ? self.channelMax : UInt16.max)  else {
            return self.eventLoop.makeFailedFuture(AMQPConnectionError.tooManyOpenedChannels)
        }

        return self.eventLoop.flatSubmit {
            let future = self.multiplexer.openChannel(id: channelID)

            future.whenFailure { _ in self.channels.remove(id: channelID) }
            return future.map  { response in 
                    let amqpChannel = AMQPChannel(channelID: channelID, eventLoop: self.eventLoop, channel: response)
                    self.channels.add(channel: amqpChannel)
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
            let result: EventLoopFuture<(Error?, Error?)> =  self.multiplexer.close(reason: reason, code: code)
                .map { 
                    return ($0, nil)
                }
                .flatMap  { result in
                    self.channel.close()
                    .map { 
                        self.state = .closed
                        return result
                    }
                    .recover  { error in
                        if case ChannelError.alreadyClosed = error  {
                            self.state = .closed
                            return result
                        }

                        return (result.0, error)
                    }
                }
            return result.flatMapThrowing {
                    let (broker, conn) = $0
                    if (broker ?? conn) != nil { throw AMQPConnectionError.close(broker: broker, connection: conn) }
                    return ()
                }
        }
    }

    private static func boostrapChannel(use eventLoop: EventLoop, from config: AMQPConnectionConfiguration, with handler: AMQPConnectionMultiplexHandler) -> EventLoopFuture<NIOCore.Channel> {
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

    private static func boostrapClient(use eventLoopGroup: EventLoopGroup, from config: AMQPConnectionConfiguration) throws -> NIOClientTCPBootstrap {
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
}
