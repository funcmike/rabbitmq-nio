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

#if compiler(>=5.5) && canImport(_Concurrency)

import Foundation
import AMQPProtocol

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
public extension AMQPClient {
    /// Connect to broker.
    /// - Returns: Response confirming that broker has accepted a request.
    func connect() async throws -> AMQPResponse {
        return try await self.connect().get()
    }

    /// Open new channel.
    /// Can be used only when connection is connected.
    /// - Parameters:
    ///     - id: Channel Identifer must be unique and greater then 0.
    func openChannel(id: Frame.ChannelID) async throws -> AMQPChannel {
        return try await self.openChannel(id: id).get()
    }

    /// Close a connection.
    /// - Parameters:
    ///     - reason: Reason that can be logged by broker.
    ///     - code: Code that can be logged by broker.
    /// - Returns: Response confirming that broker has accepted a request.
    func close(reason: String = "", code: UInt16 = 200) async throws -> AMQPResponse {
        return try await self.close(reason: reason, code: code).get()
    }

    /// Shutdown a connection with eventloop.
    /// - Parameters:
    ///     - queue: DispatchQueue for eventloop shutdown.
    func shutdown(queue: DispatchQueue = .global()) async throws {
        return try await withUnsafeThrowingContinuation { cont in
            self.shutdown(queue: queue) { error in
                if let error = error {
                    cont.resume(throwing: error)
                } else {
                    cont.resume()
                }
            }
        }
    }
}

#endif // compiler(>=5.5)
