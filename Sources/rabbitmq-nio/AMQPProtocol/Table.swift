import NIOCore
import Foundation

public typealias Table = [String:Any]


func readTable(from buffer: inout ByteBuffer)  throws ->  (Table, Int)  {
    guard let size = buffer.readInteger(as: UInt32.self) else {
        throw DecodeError.value(type:  UInt32.self, message: "cannot read table size")
    }

    var result: [String:Any] = [:]

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
            let (value, valueSize) = try readFieldValue(from: &buffer)

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
            try writeFieldValue(value: value, into: &buffer)
        } catch let error as EncodeError {
            throw EncodeError.value(value: value, message: "cannot write value of key: \(key)", inner: error)
        }
    }

    let size = UInt32(buffer.writerIndex - startIndex - 4)

    buffer.setInteger(size, at: startIndex)
}

func readArray(from buffer: inout ByteBuffer) throws -> ([Any], Int) {
    guard let size = buffer.readInteger(as: UInt32.self) else {
        throw DecodeError.value(type: UInt32.self, message: "cannot read array size")
    }

    var result: [Any] = []

    var bytesRead = 0

    while bytesRead < size {
        do {
            let (value, valueSize) =  try readFieldValue(from: &buffer)

            bytesRead += valueSize

            result.append(value)
        } catch let error as DecodeError {
            throw DecodeError.value(message: "cannot read array value", inner: error)
        }
    }

    return (result, 4 + bytesRead)
}

func writeArray(values: [Any], into buffer: inout ByteBuffer) throws {
    let startIndex = buffer.writerIndex

    buffer.writeInteger(UInt32(0)) // placeholder for size

    for (value) in values {
        do {
            try writeFieldValue(value: value, into: &buffer)
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


func readFieldValue(from buffer: inout ByteBuffer) throws -> (Any, Int) {
    guard let rawtype = buffer.readInteger(as: UInt8.self) else {
        throw DecodeError.value(type: UInt8.self)
    }

    let type = Character(UnicodeScalar(rawtype))

    switch type {
    case "t":
        guard let value: UInt8 = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.value(type: UInt8.self, amqpType: type)
        }
        return (value == 1, 1+1)
    case "b":
        guard let value = buffer.readInteger(as: Int8.self) else {
            throw DecodeError.value(type: Int8.self, amqpType: type)
        }
        return (value, 1+1)
    case "B":
        guard let value = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.value(type: UInt8.self, amqpType: type)
        }
        return (value, 1+1)
    case "s":
        guard let value = buffer.readInteger(as: Int16.self) else {
            throw DecodeError.value(type: Int16.self, amqpType: type)
        }
        return (value, 1+2)
    case "u":
        guard let value = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self, amqpType: type)
        }
        return (value, 1+2)
    case "I":
        guard let value = buffer.readInteger(as: Int32.self) else {
            throw DecodeError.value(type: Int32.self, amqpType: type)
        }
        return (value, 1+4)
    case "i":
        guard let value = buffer.readInteger(as: UInt32.self) else {
            throw DecodeError.value(type: UInt32.self, amqpType: type)
        }
        return (value, 1+4)
    case "l":
        guard let value = buffer.readInteger(as: Int64.self) else {
            throw DecodeError.value(type: Int64.self, amqpType: type)
        }
        return (value, 1+8)
    case "f":
        guard let value = buffer.readFloat() else {
            throw DecodeError.value(type: Float.self, amqpType: type)
        }
        return (value, 1+4)
    case "d":
        guard let value = buffer.readDouble() else {
            throw DecodeError.value(type: Double.self, amqpType: type)
        }
        return (value, 1+8)
    case "S":
        do {
            let (value, valueSize) = try readLongStr(from: &buffer)
            return (value, 1 + valueSize)
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
        return (value, 1+Int(size));
    case "A":
        do {
            let (value, valueSize) = try readArray(from: &buffer)
            return (value, 1 + valueSize)
        } catch let error as DecodeError {
            throw DecodeError.value(type: [Any].self, amqpType: type, inner: error)
        }
    case "T":
        guard let timestamp = buffer.readInteger(as: Int64.self) else {
            throw DecodeError.value(type: Int64.self, amqpType: type)
        }
        return (Date(timeIntervalSince1970: TimeInterval(timestamp)), 1+8)
    case "F":
        do {
            let (value, valueSize) = try readTable(from: &buffer)
            return  (value, 1 + valueSize)
        } catch let error as DecodeError {
            throw DecodeError.value(type: [String: Any].self, amqpType: type, inner: error)
        }
    case "D":
        do {
            let (value, valueSize) = try readDecimal(from: &buffer)
            return (value, 1 + valueSize)
        } catch let error as DecodeError {
            throw DecodeError.value(type: Decimal.self, amqpType: type, inner: error)
        }
    case "V":
        return (Empty(), 1)
    default:
        throw DecodeError.unsupported(value: type, message: "invalid field type")
    }
}

func writeFieldValue(value: Any, into buffer: inout ByteBuffer) throws {
    switch value {
    case let v as Bool:
        buffer.writeInteger(Character("t").asciiValue!)
        buffer.writeInteger(v ? UInt8(1) : UInt8(0))
    case let v as Int8:
        buffer.writeInteger(Character("b").asciiValue!)
        buffer.writeInteger(v)
    case let v as UInt8:
        buffer.writeInteger(Character("B").asciiValue!)
        buffer.writeInteger(v)
    case let v as Int16:
        buffer.writeInteger(Character("s").asciiValue!)
        buffer.writeInteger(v)
    case let v as UInt16:
        buffer.writeInteger(Character("u").asciiValue!)
        buffer.writeInteger(v)
    case let v as Int32:
        buffer.writeInteger(Character("I").asciiValue!)
        buffer.writeInteger(v)
    case let v as UInt32:
        buffer.writeInteger(Character("i").asciiValue!)
        buffer.writeInteger(v)
    case let v as Int64:
        buffer.writeInteger(Character("l").asciiValue!)
        buffer.writeInteger(v)
    case let v as Float:
        buffer.writeInteger(Character("f").asciiValue!)
        buffer.writeFloat(v)
    case let v as Double:
        buffer.writeInteger(Character("d").asciiValue!)
        buffer.writeDouble(v)
    case let v as String:
        buffer.writeInteger(Character("S").asciiValue!)
        try writeLongStr(value: v, into: &buffer)
    case let v as [UInt8]:
        buffer.writeInteger(Character("x").asciiValue!)
        buffer.writeBytes(v)
    case let v as [Any]:
        buffer.writeInteger(Character("A").asciiValue!)
        try writeArray(values: v, into: &buffer)
    case let v as Date:
        buffer.writeInteger(Character("T").asciiValue!)
        buffer.writeInteger(v.toUnixEpoch())
    case let v as Table:
        buffer.writeInteger(Character("F").asciiValue!)
        try writeTable(values: v, into: &buffer)
    case let v as Decimal:
        buffer.writeInteger(Character("D").asciiValue!)
        try writeDecimal(value: v, into: &buffer)
    case is Empty:
        buffer.writeInteger(Character("V").asciiValue!)
    default:
        throw EncodeError.unsupported(value: value)
    }
}
