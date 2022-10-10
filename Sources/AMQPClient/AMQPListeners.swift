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

// Shamelessly taken from: https://github.com/swift-server-community/mqtt-nio/blob/240841287482e5b15180ca46d4419230be2f2086/Sources/MQTTNIO/MQTTListeners.swift

import NIO
import NIOConcurrencyHelpers

internal struct AMQPListeners<ReturnType> {
    typealias Listener = (Result<ReturnType, Error>) -> Void

    private let lock = NIOLock()
    private var listeners: [String: Listener] = [:]

    func notify(_ result: Result<ReturnType, Error>) {
        self.lock.withLock {
            listeners.values.forEach { listener in
                listener(result)
            }
        }
    }

    mutating func addListener(named name: String, listener: @escaping Listener) {
        self.lock.withLock {
            listeners[name] = listener
        }
    }

    mutating func removeListener(named name: String) {
        self.lock.withLock {
            listeners[name] = nil
        }
    }
}
