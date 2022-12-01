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

import NIOCore
import AMQPProtocol

public final class AMQPFrameDecoder: ByteToMessageDecoder  {
    public typealias InboundOut = Frame

    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        let startReaderIndex = buffer.readerIndex

        do {
            let frame = try Frame.decode(from: &buffer)
            context.fireChannelRead(wrapInboundOut(frame))

            return .continue
        } catch let error as ProtocolError {
            buffer.moveReaderIndex(to: startReaderIndex)

            guard case .incomplete = error else {
                throw error
            }

            return .needMoreData
        } catch {
            preconditionFailure("Expected to only see `ProtocolError`s here.")
        }
    }

    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState  {
        return .needMoreData
    }
}
