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
import AMQPProtocol

internal struct AMQPFrameDecoder: NIOSingleStepByteToMessageDecoder {
    mutating func decodeLast(buffer: inout ByteBuffer, seenEOF: Bool) throws -> Frame? {
        try self.decode(buffer: &buffer)
    }

    typealias InboundOut = Frame

    mutating func decode(buffer: inout ByteBuffer) throws -> Frame? {
        let startReaderIndex = buffer.readerIndex

        do {
            return try Frame.decode(from: &buffer)
        } catch let error as ProtocolError {
            buffer.moveReaderIndex(to: startReaderIndex)
            throw ClientError.protocol(error)
        } catch {
            preconditionFailure("Expected to only see `ProtocolError`s here.")
        }
    }
}
