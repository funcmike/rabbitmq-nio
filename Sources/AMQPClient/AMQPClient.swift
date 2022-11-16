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

import Atomics
import NIO
import Dispatch
import NIOConcurrencyHelpers
import AMQPProtocol

public final class AMQPClient {
    private let eventLoopGroup: EventLoopGroup
    private let eventLoopGroupProvider: NIOEventLoopGroupProvider
    private let config: AMQPClientConfiguration

    private let isShutdown = ManagedAtomic(false)

    private var lock = NIOLock()
    private var _connection: AMQPConnection?
    private var connection: AMQPConnection? {
        get {
            self.lock.withLock {
                _connection
            }
        }
        set {
            self.lock.withLock {
                _connection = newValue
            }
        }
    }

    private let channelMax = ManagedAtomic(UInt16(0))
    private var channels = AMQPChannels()

    /// EventLoop used by a connection.
    public var eventLoop: EventLoop? { return self.connection?.eventLoop }

    /// Future that resolves when connection is closed.
    public var closeFuture: EventLoopFuture<Void>? {
        get { return self._connection?.closeFuture }
    }

    public init(eventLoopGroupProvider: NIOEventLoopGroupProvider, config: AMQPClientConfiguration) {
        self.config = config
        self.eventLoopGroupProvider = eventLoopGroupProvider

        switch eventLoopGroupProvider {
        case .createNew:
            self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        case .shared(let elg):
            self.eventLoopGroup = elg
        }
    }

    /// Connect to broker.
    /// - Returns: EventLoopFuture with result confirming that broker has accepted a request.
    @discardableResult
    public func connect() ->  EventLoopFuture<AMQPResponse.Connection.Connected> {
        guard self.connection == nil else { return self.eventLoopGroup.any().makeFailedFuture(AMQPClientError.alreadyConnected) }

        return AMQPConnection.create(use: self.eventLoopGroup, from: self.config)
            .flatMap { connection  in 
                self.connection = connection
                connection.closeFuture.whenComplete { result in
                    if self.connection === connection {
                        self.connection = nil
                    }
                }

                let response: EventLoopFuture<AMQPResponse> = connection.write(channelID: 0, outbound: .bytes(PROTOCOL_START_0_9_1), immediate: true)
                return response
            }
            .flatMapThrowing { response in
                guard case .connection(let connection) = response, case .connected(let connected) = connection else {
                    throw AMQPClientError.invalidResponse(response)
                }
                self.channelMax.store(connected.channelMax, ordering: .relaxed)
                return connected
            }
    }

    /// Open new channel.
    /// Can be used only when connection is connected.
    /// - Parameters:
    ///     - id: Channel Identifer must be unique and greater then 0 if empty auto assign
    /// - Returns: EventLoopFuture with AMQP Channel.
    public func openChannel(id: UInt16? = nil) -> EventLoopFuture<AMQPChannel> {
        guard let connection = self.connection else { return self.eventLoopGroup.any().makeFailedFuture(AMQPClientError.connectionClosed()) }

        if let id = id {
            if let channel = self.channels.get(id: id) {
                return self.eventLoopGroup.any().makeSucceededFuture(channel)
            }

            guard self.channels.tryReserve(id: id) else {
                return self.eventLoopGroup.any().makeFailedFuture(AMQPClientError.channelAlreadyReserved)
            }
        }

        let max = self.channelMax.load(ordering: .relaxed)

        guard let channelID = id ?? self.channels.tryReserveAny(max: max > 0 ? max : UInt16.max)  else {
            return self.eventLoopGroup.any().makeFailedFuture(AMQPClientError.tooManyOpenedChannels)
        }

        let future = connection.openChannel(frame: .method(channelID, .channel(.open(reserved1: ""))), immediate: true)
        future.whenFailure { _ in self.channels.remove(id: channelID) }
        return future.flatMapThrowing  { response in 
                guard case .channel(let channel) = response, case .opened(let opened) = channel, opened.channelID == channelID else {
                    throw AMQPClientError.invalidResponse(response)
                }

                let notifier = opened.notifier
                notifier.closeFuture.whenComplete { _ in self.channels.remove(id: channelID) }

                let amqpChannel = AMQPChannel(channelID: channelID, eventLoopGroup: self.eventLoopGroup, notifier: notifier, connection: connection)
                self.channels.add(channel: amqpChannel)
                return amqpChannel
            }
    }

    /// Shutdown a connection with eventloop.
    /// - Parameters:
    ///     - reason: Reason that can be logged by broker.
    ///     - code: Code that can be logged by broker.
    ///     - queue: DispatchQueue for eventloop shutdown.
    ///     - callback: Function that will be executed after stop.
    public func shutdown(reason: String = "", code: UInt16 = 200, queue: DispatchQueue = .global(), _ callback: @escaping (Error?) -> Void) {
        guard self.isShutdown.compareExchange(expected: false, desired: true, ordering: .relaxed).exchanged else {
            callback(AMQPClientError.alreadyShutdown)
            return
        }

        let eventLoop = self.eventLoopGroup.next()
        let closeFuture: EventLoopFuture<(Error?, Error?)>

        if let connection = self.connection {
            closeFuture = self.close(reason: reason, code: code, connection: connection)
                .map { ($0, nil) }
                .flatMap  { result in
                    connection.close()
                    .map { result }
                    .recover  { error in
                        if case ChannelError.alreadyClosed = error  {
                            return result
                        }
                        return (result.0, error)
                    }
                }
        } else {
            closeFuture = eventLoop.makeSucceededFuture((nil, nil))
        }

        closeFuture.whenComplete { result in
            let eventLoopCallback: (Error?) -> Void

            switch result {
            case .failure(let e):
               eventLoopCallback = { callback(AMQPClientError.shutdown(connection: e, eventLoop: $0)) }
            case .success(let (broker, conn)):
               eventLoopCallback = {
                    (broker ?? conn ?? $0) != nil ? callback(AMQPClientError.shutdown(broker: broker, connection: conn, eventLoop: $0)) : callback(nil)
                }
            }

            self.shutdownEventLoopGroup(queue: queue, eventLoopCallback)
        }
    }

    private func close(reason: String = "", code: UInt16 = 200, connection: AMQPConnection) -> EventLoopFuture<Error?> {
        return connection.write(channelID: 0, outbound: .frame(.method(0, .connection(.close(.init(replyCode: code, replyText: reason, failingClassID: 0, failingMethodID: 0))))), immediate: true)
        .map { response in
            guard case .connection(let connection) = response, case .closed = connection else {
                return AMQPClientError.invalidResponse(response)
            }
            return nil
        }
        .recover { $0 }
    }

    private func shutdownEventLoopGroup(queue: DispatchQueue, _ callback: @escaping (Error?) -> Void) {
        switch self.eventLoopGroupProvider {
        case .shared:
            queue.async {
                callback(nil)
            }
        case .createNew:
            self.eventLoopGroup.shutdownGracefully(queue: queue, callback)
        }
    }

    deinit {
        guard isShutdown.load(ordering: .relaxed) else {
            preconditionFailure("Client not shut down before the deinit. Please call client.shutdown() when no longer needed.")
        }
    }
}
