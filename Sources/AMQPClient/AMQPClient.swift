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
    let eventLoopGroup: EventLoopGroup
    let eventLoopGroupProvider: NIOEventLoopGroupProvider
    let config: Configuration

    private let isShutdown = ManagedAtomic(false)

    private var lock = NIOLock()
    private var _connection: AMQPConnection?
    var connection: AMQPConnection? {
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

    public init(eventLoopGroupProvider: NIOEventLoopGroupProvider, config: Configuration) {
        self.config = config
        self.eventLoopGroupProvider = eventLoopGroupProvider

        switch eventLoopGroupProvider {
        case .createNew:
            self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        case .shared(let elg):
            self.eventLoopGroup = elg
        }
    }

    public func connect() ->  EventLoopFuture<AMQPResponse> {
        return AMQPConnection.create(use: self.eventLoopGroup, from: self.config)
            .flatMap { connection  in 
                self.connection = connection
                connection.closeFuture().whenComplete { result in
                    if self.connection === connection {
                        self.connection = nil
                    }
                }
                return connection.sendBytes(bytes: PROTOCOL_START_0_9_1, immediate: true)
            }
            .flatMapThrowing { response in
                guard case .connection(let connection) = response, case .connected = connection else {
                    throw ClientError.invalidResponse(response)
                }
                return response
            }
    }

    public func openChannel(id: Frame.ChannelID) -> EventLoopFuture<AMQPChannel> {
        guard let connection = self.connection else { return self.eventLoopGroup.next().makeFailedFuture(ClientError.connectionClosed()) }

        return connection.sendFrame(frame: .method(id, .channel(.open(reserved1: ""))), immediate: true)
            .flatMapThrowing  { response in 
                guard case .channel(let channel) = response, case .opened(let channelID, let closeFuture) = channel, id == channelID else {
                    throw ClientError.invalidResponse(response)
                }

                return AMQPChannel(channelID: id, eventLoopGroup: self.eventLoopGroup, connection: connection, channelCloseFuture: closeFuture)
            }
    }

    public func close(reason: String = "", code: UInt16 = 200) -> EventLoopFuture<AMQPResponse> {
        guard let connection = self.connection else { return self.eventLoopGroup.next().makeFailedFuture(ClientError.connectionClosed()) }

        return connection.sendFrame(frame: .method(0, .connection(.close(.init(replyCode: code, replyText: reason, failingClassID: 0, failingMethodID: 0)))))
        .flatMapThrowing { response in
            guard case .channel(let channel) = response, case .closed = channel else {
                throw ClientError.invalidResponse(response)
            }
            return response
        }
    }

    public func shutdown(queue: DispatchQueue = .global(), _ callback: @escaping (Error?) -> Void) {
        guard self.isShutdown.compareExchange(expected: false, desired: true, ordering: .relaxed).exchanged else {
            callback(ClientError.alreadyShutdown)
            return
        }

        let eventLoop = self.eventLoopGroup.next()
        let closeFuture: EventLoopFuture<Void>

        if let connection = self.connection {
            closeFuture = connection.close()
        } else {
            closeFuture = eventLoop.makeSucceededVoidFuture()
        }

        closeFuture.whenComplete { result in
            let closeError: Error?
            switch result {
            case .failure(let error):
                if case ChannelError.alreadyClosed = error {
                    closeError = nil
                } else {
                    closeError = error
                }
            case .success:
                closeError = nil
            }

            self.shutdownEventLoopGroup(queue: queue) { error in
                callback(closeError ?? error)
            }
        }
    }

    /// shutdown EventLoopGroup
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

    public func closeFuture() -> EventLoopFuture<Void>? {
        return self._connection?.closeFuture()
    }

    deinit {
        guard isShutdown.load(ordering: .relaxed) else {
            preconditionFailure("Client not shut down before the deinit. Please call client.syncShutdownGracefully() when no longer needed.")
        }
    }
}
