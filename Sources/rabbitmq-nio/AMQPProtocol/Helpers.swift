import NIOCore
import Foundation

public struct Empty {}

func readShortStr(from buffer: inout ByteBuffer) throws -> (String, Int) {
    guard let size = buffer.readInteger(as: UInt8.self) else {
        throw DecodeError.value(type: UInt8.self, message: "cannot read size")
    }    

    guard let value = buffer.readString(length: Int(size)) else {
        throw DecodeError.value(type: String.self, message: "cannot read value")
    }

    return (value, 1 + Int(size))
}

func writeShortStr(value: String, into buffer: inout ByteBuffer) throws {
    let startIndex: Int = buffer.writerIndex

    buffer.writeInteger(UInt8(0)) // placeholder for size

    let size = buffer.writeString(value)

    guard size > UINT8_MAX else {
        throw EncodeError.unsupported(value: value, message: "Short string too long, max \(UINT8_MAX)")
    }

    buffer.setInteger(UInt8(size), at: startIndex)
}

func readLongStr(from buffer: inout ByteBuffer) throws -> (String, Int) {
    guard let size: UInt32 = buffer.readInteger(as: UInt32.self) else {
        throw DecodeError.value(type:  UInt32.self, message: "cannot read size")
    }

    guard let value = buffer.readString(length: Int(size)) else {
        throw DecodeError.value(type:  String.self, message: "cannot read value")
    }

    return (value, 4 + Int(size))
}

func writeLongStr(value: String, into buffer: inout ByteBuffer) throws {
    let startIndex: Int = buffer.writerIndex

    buffer.writeInteger(UInt32(0))  // placeholde for size

    let size = buffer.writeString(value)

    guard size > UINT32_MAX else {
       throw EncodeError.unsupported(value: value, message: "short string too long, max \(UINT32_MAX)")
    }

    buffer.setInteger(UInt32(size), at: startIndex)
}

func readTable(from buffer: inout ByteBuffer)  throws ->  (Table, Int)  {
    guard let size = buffer.readInteger(as: UInt32.self) else {
        throw DecodeError.value(type:  UInt32.self, message: "cannot read size")
    }

    var result: [String:Any] = [:]

    var bytesRead = 0

    while bytesRead < size {
        let (key, keySize) = try readShortStr(from: &buffer)

        bytesRead += keySize

        do {
            let (value, valueSize) = try readFieldValue(from: &buffer)

            bytesRead += valueSize

            result[key] = value
        } catch let error as DecodeError {
            throw DecodeError.value(message: "cannot read field value of key: \(key)", inner: error)
        }
    }
    
    return (result, 4 + bytesRead)
}

func writeTable(values: Table, into buffer: inout ByteBuffer) throws {
    let startIndex: Int = buffer.writerIndex

    buffer.writeInteger(UInt32(0)) // placeholder for size

    for (key, value) in values {
        try writeShortStr(value: key, into: &buffer)
    
        do {
            try writeFieldValue(value: value, into: &buffer)
        } catch let error as EncodeError {
            throw EncodeError.value(message: "cannot write value: \(value) of key: \(key)", inner: error)
        }
    }

    let size = UInt32(buffer.writerIndex - startIndex - 4)

    buffer.setInteger(size, at: startIndex)
}

func readArray(from buffer: inout ByteBuffer) throws -> ([Any], Int) {
    guard let size = buffer.readInteger(as: UInt32.self) else {
        throw DecodeError.value(type: UInt32.self, message: "cannot read size")
    }

    var result: [Any] = []

    var bytesRead = 0

    while bytesRead < size {
        let (value, valueSize) =  try readFieldValue(from: &buffer)

        bytesRead += valueSize

        result.append(value)
    }

    return (result, 4 + bytesRead)
}

func writeArray(values: [Any], into buffer: inout ByteBuffer) throws {
    let startIndex = buffer.writerIndex

    buffer.writeInteger(UInt32(0)) // placeholder for size

    for (value) in values {
        try writeFieldValue(value: value, into: &buffer)
    }

    let size = UInt32(buffer.writerIndex - startIndex - 4)

    buffer.setInteger(size, at: startIndex)
}

func readDecimal(from buffer: inout ByteBuffer) -> (Decimal, Int)? {
    guard let scale = buffer.readInteger(as: UInt8.self) else {
        return nil
    }

    guard let value = buffer.readInteger(as: UInt32.self) else {
        return nil
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
            throw DecodeError.value(type: UInt8.self, amqpType: "t")
        }
        return (value == 1, 1+1)
    case "b":
        guard let value = buffer.readInteger(as: Int8.self) else {
            throw DecodeError.value(type: Int8.self, amqpType: "b")
        }
        return (value, 1+1)
    case "B":
        guard let value = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.value(type: UInt8.self, amqpType: "B")
        }
        return (value, 1+1)
    case "s":
        guard let value = buffer.readInteger(as: Int16.self) else {
            throw DecodeError.value(type: Int16.self, amqpType: "s")
        }
        return (value, 1+2)
    case "u":
        guard let value = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self, amqpType: "u")
        }
        return (value, 1+2)
    case "I":
        guard let value = buffer.readInteger(as: Int32.self) else {
            throw DecodeError.value(type: Int32.self, amqpType: "I")
        }
        return (value, 1+4)
    case "i":
        guard let value = buffer.readInteger(as: UInt32.self) else {
            throw DecodeError.value(type: UInt32.self, amqpType: "i")
        }
        return (value, 1+4)
    case "l":
        guard let value = buffer.readInteger(as: Int64.self) else {
            throw DecodeError.value(type: Int64.self, amqpType: "l")
        }
        return (value, 1+8)
    case "f":
        guard let value = buffer.readFloat() else {
            throw DecodeError.value(type: Float.self, amqpType: "f")
        }
        return (value, 1+4)
    case "d":
        guard let value = buffer.readDouble() else {
            throw DecodeError.value(type: Double.self, amqpType: "d")
        }
        return (value, 1+8)
    case "S":
        do {
            let (value, valueSize) = try readLongStr(from: &buffer)
            return (value, 1 + valueSize)
        } catch let error as DecodeError {
            throw DecodeError.value(type: String.self, amqpType: "S", inner: error)
        }
    case "x":
        guard let size = buffer.readInteger(as: UInt32.self) else {
            throw DecodeError.value(type: UInt32.self, amqpType: "x")
        }
        guard let value = buffer.readBytes(length: Int(size)) else {
            throw DecodeError.value(type: [UInt8].self, amqpType: "x")
        }            
        return (value, 1+Int(size));
    case "A":
        do {
            let (value, valueSize) = try readArray(from: &buffer)
            return (value, 1 + valueSize)
        } catch let error as DecodeError {
            throw DecodeError.value(type: [Any].self, amqpType: "A", inner: error)
        }
    case "T":
        guard let timestamp = buffer.readInteger(as: Int64.self) else {
            throw DecodeError.value(type: Int64.self, amqpType: "T")
        }
        return (Date(timeIntervalSince1970: TimeInterval(timestamp)), 1+8)
    case "F":
        do {
            let (value, valueSize) = try readTable(from: &buffer)
            return  (value, 1 + valueSize)
        } catch let error as DecodeError {
            throw DecodeError.value(type: [String: Any].self, amqpType: "F", inner: error)
        }
    case "D":
        guard let (value, valueSize) = readDecimal(from: &buffer) else {
            throw DecodeError.value(type: Decimal.self, amqpType: "D")
        }
        return (value, 1 + valueSize)
    case "V":
        return (Empty(), 1)
    default:
        throw DecodeError.unsupported(value: type, message: "invalid field type")
    }
}

func writeFieldValue(value: Any, into buffer: inout ByteBuffer) throws {
    switch value {
    case let v as Bool where v == true:
        buffer.writeInteger(Character("t").asciiValue!)
        buffer.writeInteger(UInt8(1))
    case let v as Bool where v == false:
        buffer.writeInteger(Character("t").asciiValue!)
        buffer.writeInteger(UInt8(1))
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
