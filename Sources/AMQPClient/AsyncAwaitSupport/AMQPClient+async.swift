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

import Foundation
import AMQPProtocol

public extension AMQPClient {
    func connect() async throws {
        return try await self.connect().get()
    }

    func openChannel(id: Frame.ChannelID) async throws -> AMQPChannel {
        return try await self.openChannel(id: id).get()
    }

    func close(reason: String = "", code: UInt16 = 200) async throws {
        return try await self.close(reason: reason, code: code).get()
    }

    func shutdown(queue: DispatchQueue = .global()) async throws {
        return try await withUnsafeThrowingContinuation { cont in
            shutdown(queue: queue) { error in
                if let error = error {
                    cont.resume(throwing: error)
                } else {
                    cont.resume()
                }
            }
        }
    }
}
