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
import Foundation

public typealias Table = [String:Field]

public enum Field: Equatable {
    case bool(Bool)
    case int8(Int8)
    case uInt8(UInt8)
    case int16(Int16)
    case uInt16(UInt16)
    case int32(Int32)
    case uInt32(UInt32)
    case int64(Int64)
    case float(Float)
    case double(Double)
    case longString(String)
    case bytes([UInt8])
    case array([Field])
    case timestamp(Date)
    case table(Table)
    case decimal(scale: UInt8, value: UInt32)
    case `nil`

    var kind: Kind {
        switch self {        
        case .bool:
            return .bool
        case .int8:
            return .int8
        case .uInt8:
            return .uInt8
        case .int16:
            return .int16
        case .uInt16:
            return .uInt16
        case .int32:
            return .int32
        case .uInt32:
            return .uInt32
        case .int64:
            return .int64
        case .float:
            return .float
        case .double:
            return .double
        case .longString:
            return .longString
        case .bytes:
            return .bytes
        case .array:
            return .array
        case .timestamp:
            return .timestamp
        case .table:
            return .table
        case .decimal:
            return .decimal
        case .`nil`:
            return .nil
        }
    }

    public enum Kind: UInt8 {
        case bool = 116         // t
        case int8 = 98          // b
        case uInt8 = 66         // B
        case int16 = 115        // s
        case uInt16 = 117       // u
        case int32 = 73         // I
        case uInt32 = 105       // i
        case int64 = 108        // l
        case float = 102        // f
        case double = 100       // d
        case longString = 83    // S
        case bytes = 120        // x
        case array = 65         // A
        case timestamp = 84     // T
        case table = 70         // F
        case decimal = 68       // D
        case `nil` = 86         // V
    }
}

extension Table: PayloadDecodable {
    public static func decode(from buffer: inout ByteBuffer) throws -> Self {
        let (table, _) = try readTable(from: &buffer)
        return table
    }

    static func readTable(from buffer: inout ByteBuffer)  throws -> (Table, Int) {
        guard let size = buffer.readInteger(as: UInt32.self) else {
            throw ProtocolError.decode(type: UInt32.self, context: self)
        }

        var result: Table = [:]

        var bytesRead = 0

        while bytesRead < size {
            let key: String, keySize: Int

            do {
                (key, keySize) = try buffer.readShortString()
            } catch let error as ProtocolError {
                throw ProtocolError.decode(message: "cannot read table key", context: self, inner: error)
            }

            bytesRead += keySize

            do {
                let (value, valueSize) = try readField(from: &buffer)

                bytesRead += valueSize

                result[key] = value
            } catch let error as ProtocolError {
                throw ProtocolError.decode(message: "cannot read table field value of key: \(key)", context: self, inner: error)
            }
        }
        
        return (result, 4 + bytesRead)
    }

    static func readArray(from buffer: inout ByteBuffer) throws -> ([Field], Int) {
        guard let size = buffer.readInteger(as: UInt32.self) else {
            throw ProtocolError.decode(type: UInt32.self, context: self)
        }

        var result: [Field] = []

        var bytesRead = 0

        while bytesRead < size {
            do {
                let (value, valueSize) =  try readField(from: &buffer)

                bytesRead += valueSize

                result.append(value)
            } catch let error as ProtocolError {
                throw ProtocolError.decode(message: "cannot read array value", context: self, inner: error)
            }
        }

        return (result, 4 + bytesRead)
    }

    static func readField(from buffer: inout ByteBuffer) throws -> (Field, Int) {
        guard let kind = buffer.readInteger(as: Field.Kind.self) else {
            throw ProtocolError.decode(type:  Field.Kind.self, context: self)
        }

        switch kind {
        case .bool:
            guard let value = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, kind: kind, context: self)
            }
            return (.bool(value == 1), 1+1)
        case .int8:
            guard let value = buffer.readInteger(as: Int8.self) else {
                throw ProtocolError.decode(type: Int8.self, kind: kind, context: self)
            }
            return (.int8(value), 1+1)
        case .uInt8:
            guard let value = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, kind: kind, context: self)
            }
            return (.uInt8(value), 1+1)
        case .int16:
            guard let value = buffer.readInteger(as: Int16.self) else {
                throw ProtocolError.decode(type: Int16.self, kind: kind, context: self)
            }
            return (.int16(value), 1+2)
        case .uInt16:
            guard let value = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, kind: kind, context: self)
            }
            return (.uInt16(value), 1+2)
        case .int32:
            guard let value = buffer.readInteger(as: Int32.self) else {
                throw ProtocolError.decode(type: Int32.self, kind: kind, context: self)
            }
            return (.int32(value), 1+4)
        case .uInt32:
            guard let value = buffer.readInteger(as: UInt32.self) else {
                throw ProtocolError.decode(type: UInt32.self, kind: kind, context: self)
            }
            return (.uInt32(value), 1+4)
        case .int64:
            guard let value = buffer.readInteger(as: Int64.self) else {
                throw ProtocolError.decode(type: Int64.self, kind: kind, context: self)
            }
            return (.int64(value), 1+8)
        case .float:
            guard let value = buffer.readFloat() else {
                throw ProtocolError.decode(type: Float.self, kind: kind, context: self)
            }
            return (.float(value), 1+4)
        case .double:
            guard let value = buffer.readDouble() else {
                throw ProtocolError.decode(type: Double.self, kind: kind, context: self)
            }
            return (.double(value), 1+8)
        case .longString:
            do {
                let (value, valueSize) = try buffer.readLongString()
                return (.longString(value), 1 + valueSize)
            } catch let error as ProtocolError {
                throw ProtocolError.decode(type: String.self, kind: kind, context: self, inner: error)
            }
        case .bytes:
            guard let size = buffer.readInteger(as: UInt32.self) else {
                throw ProtocolError.decode(type: UInt32.self, kind: kind, context: self)
            }
            guard let value = buffer.readBytes(length: Int(size)) else {
                throw ProtocolError.decode(type: [UInt8].self, kind: kind, context: self)
            }
            return (.bytes(value), 1+Int(size));
        case .array:
            do {
                let (value, valueSize) = try readArray(from: &buffer)
                return (.array(value), 1 + valueSize)
            } catch let error as ProtocolError {
                throw ProtocolError.decode(type: [Any].self, kind: kind, context: self, inner: error)
            }
        case .timestamp:
            guard let timestamp = buffer.readInteger(as: Int64.self) else {
                throw ProtocolError.decode(type: Int64.self, kind: kind, context: self)
            }
            return (.timestamp(Date(timeIntervalSince1970: TimeInterval(timestamp))), 1+8)
        case .table:
            do {
                let (value, valueSize) = try readTable(from: &buffer)
                return  (.table(value), 1 + valueSize)
            } catch let error as ProtocolError {
                throw ProtocolError.decode(type: [String: Any].self, kind: kind, context: self, inner: error)
            }
        case .decimal:
            guard let (scale, value) = buffer.readMultipleIntegers(as: (UInt8, UInt32).self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            return (.decimal(scale: scale, value: value), 1+1+4)
        case .nil:
            return (.nil, 1)
        }
    }
}

extension Table: PayloadEncodable {
    public func encode(into buffer: inout ByteBuffer) throws {
        let startIndex: Int = buffer.writerIndex

        buffer.writeInteger(UInt32(0)) // placeholder for size

        for (key, value) in self {
            do {
                try buffer.writeShortString(key)
            } catch let error as ProtocolError {
                throw ProtocolError.encode(value: key, message: "cannot write table key", context: Table.self, inner: error)
            }
        
            do {
                try Table.writeField(field: value, into: &buffer)
            } catch let error as ProtocolError {
                throw ProtocolError.encode(value: value, message: "cannot write table value of key: \(key)", context: Table.self, inner: error)
            }
        }

        let size = UInt32(buffer.writerIndex - startIndex - 4)

        buffer.setInteger(size, at: startIndex)
    }

    static func writeArray(values: [Field], into buffer: inout ByteBuffer) throws {
        let startIndex = buffer.writerIndex

        buffer.writeInteger(UInt32(0)) // placeholder for size

        for (value) in values {
            do {
                try writeField(field: value, into: &buffer)
            } catch let error as ProtocolError {
                throw ProtocolError.encode(value: value, message: "cannot write array value", context: self, inner: error)
            }
        }

        let size = UInt32(buffer.writerIndex - startIndex - 4)

        buffer.setInteger(size, at: startIndex)
    }

    static func writeField(field: Field, into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(field.kind.rawValue)

        switch field {
        case .bool(let v):
            buffer.writeInteger(v ? UInt8(1) : UInt8(0))
        case .int8(let v):
            buffer.writeInteger(v)
        case .uInt8(let v):
            buffer.writeInteger(v)
        case .int16(let v):
            buffer.writeInteger(v)
        case .uInt16(let v):
            buffer.writeInteger(v)
        case .int32(let v):
            buffer.writeInteger(v)
        case .uInt32(let v):
            buffer.writeInteger(v)
        case .int64(let v):
            buffer.writeInteger(v)
        case .float(let v):
            buffer.writeFloat(v)
        case .double(let v):
            buffer.writeDouble(v)
        case .longString(let v):
            try buffer.writeLongString(v)
        case .bytes(let v):
            buffer.writeBytes(v)
        case .array(let v):
            try writeArray(values: v, into: &buffer)
        case .timestamp(let v):
            buffer.writeInteger(v.toUnixEpoch())
        case .table(let v):
            try v.encode(into: &buffer)
        case .decimal(let scale, let value):
            buffer.writeMultipleIntegers(scale, value)
        case .nil:
            break
        }
    }
}
