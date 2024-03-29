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
import NIOCore
import AMQPProtocol


@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
public extension AMQPConnection {
    /// Connect to broker.
    /// - Parameters:
    ///     - eventLoop: EventLoop on which to connect.
    ///     - config: Configuration data.
    /// - Returns: New AMQP Connection.
    static func connect(use eventLoop: EventLoop, from config: AMQPConnectionConfiguration) async throws -> AMQPConnection {
        return try await self.connect(use: eventLoop, from: config).get()
    }

    /// Open new channel.
    /// Can be used only when connection is connected.
    /// Channel ID is automatically assigned (next free one).
    /// - Returns: New opened AMQP Channel.
    func openChannel() async throws -> AMQPChannel {
        return try await self.openChannel().get()
    }

    /// Close a connection.
    /// - Parameters:
    ///     - reason: Reason that can be logged by broker.
    ///     - code: Code that can be logged by broker.
    func close(reason: String = "", code: UInt16 = 200) async throws {
        return try await self.close(reason: reason, code: code).get()
    }
}