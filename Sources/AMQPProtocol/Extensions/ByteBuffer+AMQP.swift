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

import NIOCore

internal extension ByteBuffer {
    @usableFromInline
    mutating func readInteger<E>(endianness: Endianness = .big, as rawRepresentable: E.Type) -> E? where E: RawRepresentable, E.RawValue: FixedWidthInteger {
        guard let rawValue = readInteger(endianness: endianness, as: E.RawValue.self) else {
            return nil
        }
        return E.init(rawValue: rawValue)
    }

    @usableFromInline
    mutating func readFloat() -> Float? {
        return self.readInteger(as: UInt32.self).map { Float(bitPattern: $0) }
    }

    @usableFromInline
    mutating func readDouble() -> Double? {
        return self.readInteger(as: UInt64.self).map { Double(bitPattern: $0) }
    }

    @usableFromInline
    mutating func writeFloat(_ float: Float) {
        self.writeInteger(float.bitPattern)
    }

    @usableFromInline
    mutating func writeDouble(_ double: Double) {
        self.writeInteger(double.bitPattern)
    }

    @usableFromInline
    mutating func readShortString() throws -> (String, Int) {
        guard let size = self.readInteger(as: UInt8.self) else {
            throw ProtocolError.decode(type: UInt8.self, context: ByteBuffer.self)
        }    

        guard let value = self.readString(length: Int(size)) else {
            throw ProtocolError.decode(type: String.self, message: "cannot read short string value", context: ByteBuffer.self)
        }

        return (value, 1 + Int(size))
    }

    @usableFromInline
    mutating func writeShortString(_ shortString: String) throws {
        let startIndex = self.writerIndex

        self.writeInteger(UInt8(0)) // placeholder for size

        let size = self.writeString(shortString)

        guard size <= UInt8.max else {
            throw ProtocolError.invalid(value: shortString, message: "shortString too big, max: \(UInt8.max)", context: ByteBuffer.self)
        }

        self.setInteger(UInt8(size), at: startIndex)
    }

    @usableFromInline
    mutating func readLongString() throws -> (String, Int) {
        guard let size = self.readInteger(as: UInt32.self) else {
            throw ProtocolError.decode(type:  UInt32.self, context: ByteBuffer.self)
        }

        guard let value = self.readString(length: Int(size)) else {
            throw ProtocolError.decode(type:  String.self, message: "cannot read longString value", context: ByteBuffer.self)
        }

        return (value, 4 + Int(size))
    }

    @usableFromInline
    mutating func writeLongString(_ longString: String) throws {
        let startIndex = self.writerIndex

        self.writeInteger(UInt32(0))  // placeholde for size

        let size = self.writeString(longString)

        guard size <= UInt32.max else {
            throw ProtocolError.invalid(value: longString, message: "longString too big, max: \(UInt32.max)", context: ByteBuffer.self)
        }

        self.setInteger(UInt32(size), at: startIndex)
    }
}
