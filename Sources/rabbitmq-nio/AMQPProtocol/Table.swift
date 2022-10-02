import NIOCore
import Foundation

public typealias Table = [String:Field]

public enum Field {
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
    case decimal(Decimal)
    case `nil`
}

func readTable(from buffer: inout ByteBuffer)  throws ->  (Table, Int)  {
    guard let size = buffer.readInteger(as: UInt32.self) else {
        throw DecodeError.value(type:  UInt32.self, message: "cannot read table size")
    }

    var result: [String:Field] = [:]

    var bytesRead = 0

    while bytesRead < size {
        let key: String, keySize: Int

        do {
            (key, keySize) = try readShortStr(from: &buffer)
        } catch let error as DecodeError {
            throw DecodeError.value(message: "cannot read table key", inner: error)
        }

        bytesRead += keySize

        do {
            let (value, valueSize) = try readField(from: &buffer)

            bytesRead += valueSize

            result[key] = value
        } catch let error as DecodeError {
            throw DecodeError.value(message: "cannot read table field value of key: \(key)", inner: error)
        }
    }
    
    return (result, 4 + bytesRead)
}

func writeTable(values: Table, into buffer: inout ByteBuffer) throws {
    let startIndex: Int = buffer.writerIndex

    buffer.writeInteger(UInt32(0)) // placeholder for size

    for (key, value) in values {
        do {
            try writeShortStr(value: key, into: &buffer)
        } catch let error as EncodeError {
            throw EncodeError.value(value: key, message: "cannot write key", inner: error)
        }
    
        do {
            try writeField(field: value, into: &buffer)
        } catch let error as EncodeError {
            throw EncodeError.value(value: value, message: "cannot write value of key: \(key)", inner: error)
        }
    }

    let size = UInt32(buffer.writerIndex - startIndex - 4)

    buffer.setInteger(size, at: startIndex)
}

func readArray(from buffer: inout ByteBuffer) throws -> ([Field], Int) {
    guard let size = buffer.readInteger(as: UInt32.self) else {
        throw DecodeError.value(type: UInt32.self, message: "cannot read array size")
    }

    var result: [Field] = []

    var bytesRead = 0

    while bytesRead < size {
        do {
            let (value, valueSize) =  try readField(from: &buffer)

            bytesRead += valueSize

            result.append(value)
        } catch let error as DecodeError {
            throw DecodeError.value(message: "cannot read array value", inner: error)
        }
    }

    return (result, 4 + bytesRead)
}

func writeArray(values: [Field], into buffer: inout ByteBuffer) throws {
    let startIndex = buffer.writerIndex

    buffer.writeInteger(UInt32(0)) // placeholder for size

    for (value) in values {
        do {
            try writeField(field: value, into: &buffer)
        } catch let error as EncodeError {
            throw EncodeError.value(value: value, message: "cannot write array value", inner: error)
        }
    }

    let size = UInt32(buffer.writerIndex - startIndex - 4)

    buffer.setInteger(size, at: startIndex)
}

func readDecimal(from buffer: inout ByteBuffer) throws-> (Decimal, Int) {
    guard let scale = buffer.readInteger(as: UInt8.self) else {
        throw DecodeError.value(type: UInt8.self, message: "cannot read decimal size")
    }

    guard let value = buffer.readInteger(as: UInt32.self) else {
        throw DecodeError.value(type: UInt32.self, message: "cannot read decimal value")
    }
    
    return (Decimal(value) / pow(10, Int(scale)), 1+4)
}


func writeDecimal(value: Decimal, into buffer: inout ByteBuffer) throws {
    return TODO("implement Decimal writing")
}


func readField(from buffer: inout ByteBuffer) throws -> (Field, Int) {
    guard let rawtype = buffer.readInteger(as: UInt8.self) else {
        throw DecodeError.value(type: UInt8.self, message: "cannot read field type")
    }

    let type = Character(UnicodeScalar(rawtype))

    switch type {
    case "t":
        guard let value: UInt8 = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.value(type: UInt8.self, amqpType: type)
        }
        return (.bool(value == 1), 1+1)
    case "b":
        guard let value = buffer.readInteger(as: Int8.self) else {
            throw DecodeError.value(type: Int8.self, amqpType: type)
        }
        return (.int8(value), 1+1)
    case "B":
        guard let value = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.value(type: UInt8.self, amqpType: type)
        }
        return (.uInt8(value), 1+1)
    case "s":
        guard let value = buffer.readInteger(as: Int16.self) else {
            throw DecodeError.value(type: Int16.self, amqpType: type)
        }
        return (.int16(value), 1+2)
    case "u":
        guard let value = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self, amqpType: type)
        }
        return (.uInt16(value), 1+2)
    case "I":
        guard let value = buffer.readInteger(as: Int32.self) else {
            throw DecodeError.value(type: Int32.self, amqpType: type)
        }
        return (.int32(value), 1+4)
    case "i":
        guard let value = buffer.readInteger(as: UInt32.self) else {
            throw DecodeError.value(type: UInt32.self, amqpType: type)
        }
        return (.uInt32(value), 1+4)
    case "l":
        guard let value = buffer.readInteger(as: Int64.self) else {
            throw DecodeError.value(type: Int64.self, amqpType: type)
        }
        return (.int64(value), 1+8)
    case "f":
        guard let value = buffer.readFloat() else {
            throw DecodeError.value(type: Float.self, amqpType: type)
        }
        return (.float(value), 1+4)
    case "d":
        guard let value = buffer.readDouble() else {
            throw DecodeError.value(type: Double.self, amqpType: type)
        }
        return (.double(value), 1+8)
    case "S":
        do {
            let (value, valueSize) = try readLongStr(from: &buffer)
            return (.longString(value), 1 + valueSize)
        } catch let error as DecodeError {
            throw DecodeError.value(type: String.self, amqpType: type, inner: error)
        }
    case "x":
        guard let size = buffer.readInteger(as: UInt32.self) else {
            throw DecodeError.value(type: UInt32.self, amqpType: type)
        }
        guard let value = buffer.readBytes(length: Int(size)) else {
            throw DecodeError.value(type: [UInt8].self, amqpType: type)
        }            
        return (.bytes(value), 1+Int(size));
    case "A":
        do {
            let (value, valueSize) = try readArray(from: &buffer)
            return (.array(value), 1 + valueSize)
        } catch let error as DecodeError {
            throw DecodeError.value(type: [Any].self, amqpType: type, inner: error)
        }
    case "T":
        guard let timestamp = buffer.readInteger(as: Int64.self) else {
            throw DecodeError.value(type: Int64.self, amqpType: type)
        }
        return (.timestamp(Date(timeIntervalSince1970: TimeInterval(timestamp))), 1+8)
    case "F":
        do {
            let (value, valueSize) = try readTable(from: &buffer)
            return  (.table(value), 1 + valueSize)
        } catch let error as DecodeError {
            throw DecodeError.value(type: [String: Any].self, amqpType: type, inner: error)
        }
    case "D":
        do {
            let (value, valueSize) = try readDecimal(from: &buffer)
            return (.decimal(value), 1 + valueSize)
        } catch let error as DecodeError {
            throw DecodeError.value(type: Decimal.self, amqpType: type, inner: error)
        }
    case "V":
        return (.nil, 1)
    default:
        throw DecodeError.unsupported(value: type, message: "invalid field type")
    }
}

func writeField(field: Field, into buffer: inout ByteBuffer) throws {
    switch field {
    case .bool(let v):
        buffer.writeInteger(Character("t").asciiValue!)
        buffer.writeInteger(v ? UInt8(1) : UInt8(0))
    case .int8(let v):
        buffer.writeInteger(Character("b").asciiValue!)
        buffer.writeInteger(v)
    case .uInt8(let v):
        buffer.writeInteger(Character("B").asciiValue!)
        buffer.writeInteger(v)
    case .int16(let v):
        buffer.writeInteger(Character("s").asciiValue!)
        buffer.writeInteger(v)
    case .uInt16(let v):
        buffer.writeInteger(Character("u").asciiValue!)
        buffer.writeInteger(v)
    case .int32(let v):
        buffer.writeInteger(Character("I").asciiValue!)
        buffer.writeInteger(v)
    case .uInt32(let v):
        buffer.writeInteger(Character("i").asciiValue!)
        buffer.writeInteger(v)
    case .int64(let v):
        buffer.writeInteger(Character("l").asciiValue!)
        buffer.writeInteger(v)
    case .float(let v):
        buffer.writeInteger(Character("f").asciiValue!)
        buffer.writeFloat(v)
    case .double(let v):
        buffer.writeInteger(Character("d").asciiValue!)
        buffer.writeDouble(v)
    case .longString(let v):
        buffer.writeInteger(Character("S").asciiValue!)
        try writeLongStr(value: v, into: &buffer)
    case .bytes(let v):
        buffer.writeInteger(Character("x").asciiValue!)
        buffer.writeBytes(v)
    case .array(let v):
        buffer.writeInteger(Character("A").asciiValue!)
        try writeArray(values: v, into: &buffer)
    case .timestamp(let v):
        buffer.writeInteger(Character("T").asciiValue!)
        buffer.writeInteger(v.toUnixEpoch())
    case .table(let v):
        buffer.writeInteger(Character("F").asciiValue!)
        try writeTable(values: v, into: &buffer)
    case .decimal(let v):
        buffer.writeInteger(Character("D").asciiValue!)
        try writeDecimal(value: v, into: &buffer)
    case .nil:
        buffer.writeInteger(Character("V").asciiValue!)
    }
}
