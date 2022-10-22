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
import NIOConcurrencyHelpers

struct AMQPListeners<ReturnType> {
    public typealias Listener = (Result<ReturnType, Error>) -> Void

    private let lock = NIOLock()
    private var listeners: [String: Listener] = [:]


    func notify(named name: String, _ result: Result<ReturnType, Error>) {
        self.lock.withLock {
            if let listener = self.listeners[name] {
                return listener(result)
            } else {

            }
        }
    }

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