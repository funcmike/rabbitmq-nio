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

public final class AMQPFrameEncoder: MessageToByteEncoder {
    public typealias OutboundIn = AMQPOutbound

    public func encode(data: AMQPOutbound, out: inout ByteBuffer) throws {
        switch data {
        case .frame(let frame): 
            //print("write outbound frame", frame)
            try frame.encode(into: &out)
        case .bulk(let frames):
            //print("write bulk", frames)
            for frame in frames {
                try frame.encode(into: &out)
            }
        case .bytes(let bytes):
            //print("write outbound bytes", bytes)
            _ = out.writeBytes(bytes)
        }        
    }
}
