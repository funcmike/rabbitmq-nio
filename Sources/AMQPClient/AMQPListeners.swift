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

struct AMQPListeners<ReturnType>: Sendable {
    public typealias Listener = @Sendable (Result<ReturnType, Error>) -> Void

    private var listeners: [String: Listener] = [:]

    func get(named name: String) -> Listener? {
        return self.listeners[name]
    }

    func get() -> Dictionary<String, Listener>.Values {
        return self.listeners.values
    }
    
    func exists(named name: String) -> Bool {
        if let _ = self.listeners[name] {
            return true
        }
        return false
    }

    mutating func addListener(named name: String, listener: @escaping Listener) {
        self.listeners[name] = listener
    }

    mutating func removeListener(named name: String) {
        self.listeners[name] = nil
    }
    
    mutating func removeAll() {
        self.listeners = [:]
    }
}
