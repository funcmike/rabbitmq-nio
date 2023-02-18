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


struct AMQPChannels {
    private var channels: [UInt16: AMQPChannel?] = [:]

    func get(id: UInt16) -> AMQPChannel? {
        guard let channel = self.channels[id] else {
            return nil
        }
        return channel
    }
    
    mutating func tryReserveAny(max: UInt16) -> UInt16? {
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

    mutating func add(channel: AMQPChannel) {
        self.channels[channel.ID] = channel
    }

    mutating func remove(id: UInt16) {
        let _ = self.channels.removeValue(forKey: id)
    }

    mutating private func reserve(id: UInt16) -> Bool {
        guard !(self.channels.contains { $0.key == id}) else {
            return false
        }

        self.channels[id] = nil
        return true
    }
}
