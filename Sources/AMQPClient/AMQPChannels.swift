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
    private enum ChannelSlot {
        case reserved
        case channel(AMQPChannel)
    }
    
    private var channels: [UInt16: ChannelSlot] = [:]
    
    let channelMax: UInt16
    
    init(channelMax: UInt16) {
        self.channelMax = channelMax
    }

    func get(id: UInt16) -> AMQPChannel? {
        guard case let .channel(channel) = self.channels[id] else {
            return nil
        }
        return channel
    }
    
    mutating func reserveNext() -> UInt16? {
        guard self.channels.count < channelMax else {
            return nil
        }
            
        for i in 1...channelMax where channels[i] == nil {
            channels[i] = .reserved
            return i
        }

        return nil
    }

    mutating func add(channel: AMQPChannel) {
        self.channels[channel.ID] = .channel(channel)
    }

    mutating func remove(id: UInt16) {
      self.channels[id] = nil
    }
}
