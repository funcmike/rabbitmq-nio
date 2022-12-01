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

import NIOConcurrencyHelpers

struct AMQPChannels {
    private let lock = NIOLock()
    private var channels: [UInt16: AMQPChannel?] = [:]

    func get(id: UInt16) -> AMQPChannel? {
        self.lock.withLock {
            guard let channel = self.channels[id] else {
                return nil
            }
            return channel
        }
    }

    mutating func tryReserve(id: UInt16) -> Bool {
        self.lock.withLock {
            return self.reserve(id: id)
        }
    }    
    
    mutating func tryReserveAny(max: UInt16) -> UInt16? {
        self.lock.withLock {
            guard self.channels.count < max else {
                return nil
            }
             
            for i in 1...max {
                if self.reserve(id: i) { 
                    return i
                }
            }

            return nil
        }
    }

    mutating func add(channel: AMQPChannel) {
        self.lock.withLock {
            self.channels[channel.ID] = channel
        }
    }

    mutating func remove(id: UInt16) {
        self.lock.withLock {
            let _ = self.channels.removeValue(forKey: id)
        }
    }

    mutating private func reserve(id: UInt16) -> Bool {
        guard !(self.channels.contains { $0.key == id}) else {
            return false
        }
        self.channels[id] = nil
        return true
    }
} 
