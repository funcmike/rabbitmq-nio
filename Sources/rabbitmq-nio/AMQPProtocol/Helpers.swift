
import NIOCore
import Foundation

public struct Empty {}

func readShortStr(from buffer: inout ByteBuffer) -> (String, Int)?
{
    guard let size = buffer.readInteger(as: UInt8.self) else {
        return nil
    }    

    guard let value = buffer.readString(length: Int(size)) else {
        return nil
    }

    return  (value, 1 + Int(size))
}

func writeShortStr(value: String, into buffer: inout ByteBuffer)
{
    let startIndex: Int = buffer.writerIndex

    buffer.writeInteger(UInt8(0)) // placeholder for size

    let size = UInt8(buffer.writeString(value))

    buffer.setInteger(size, at: startIndex)
}

func readLongStr(from buffer: inout ByteBuffer) -> (String, Int)?
{
    guard let size: UInt32 = buffer.readInteger(as: UInt32.self) else {
        return nil
    }

    guard let value = buffer.readString(length: Int(size)) else {
        return nil
    }

    return (value, 4 + Int(size))
}

func writeLongStr(value: String, into buffer: inout ByteBuffer)
{
    let startIndex: Int = buffer.writerIndex

    buffer.writeInteger(UInt32(0))  // placeholde for size

    let size: UInt32 = UInt32(buffer.writeString(value))

    buffer.setInteger(size, at: startIndex)
}


func readDictionary(from buffer: inout ByteBuffer)  throws ->  ([String:Any], Int)  {
    guard let size = buffer.readInteger(as: UInt32.self) else {
        throw DecodeError.table(.size)
    }

    var result: [String:Any] = [:]

    var bytesRead = 0

    while bytesRead < size {
        guard let (key, keySize) = readShortStr(from: &buffer) else {
            throw DecodeError.table(.key)
        }

        bytesRead += keySize

        do {
            let (value, valueSize) = try readFieldValue(from: &buffer)

            bytesRead += valueSize

            result[key] = value
        } catch let error as DecodeError {
            throw DecodeError.table(.value(Key: key, Inner: error))
        }
    }
    
    return (result, 4 + bytesRead)
}

func writeDictionary(values: [String:Any], into buffer: inout ByteBuffer) throws
{
    let startIndex: Int = buffer.writerIndex

    buffer.writeInteger(UInt32(0)) // placeholder for size

    for (key, value) in values {
        writeShortStr(value: key, into: &buffer)
    
        try! writeFieldValue(value: value, into: &buffer)
    }

    let size = UInt32(buffer.writerIndex - startIndex - 4)

    buffer.setInteger(size, at: startIndex)
}

func readArray(from buffer: inout ByteBuffer) throws -> ([Any], Int) {
    guard let size = buffer.readInteger(as: UInt32.self) else {
        throw DecodeError.array(.size)
    }

    var result: [Any] = []

    var bytesRead = 0

    while bytesRead < size {
        do {
            let (value, valueSize) =  try readFieldValue(from: &buffer)

            bytesRead += valueSize

            result.append(value)
        }
        catch let error as DecodeError
        {
            throw DecodeError.array(.value(Inner: error))
        }
    }

    return (result, 4 + bytesRead)
}

func writeArray(values: [Any], into buffer: inout ByteBuffer) throws
{
    let startIndex = buffer.writerIndex

    buffer.writeInteger(UInt32(0)) // placeholder for size

    for (value) in values {
        try! writeFieldValue(value: value, into: &buffer)
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
    guard let type = buffer.readInteger(as: UInt8.self) else {
        throw DecodeError.field(.type)
    }

    switch type {
        case Character("t").asciiValue:
            guard let value = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value == 1, 1+1)
        case Character("b").asciiValue:
            guard let value = buffer.readInteger(as: Int8.self) else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value, 1+1)
        case Character("B").asciiValue:
            guard let value = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value, 1+1)
        case Character("s").asciiValue:
            guard let value = buffer.readInteger(as: Int16.self) else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value, 1+2)
        case Character("u").asciiValue:
            guard let value = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value, 1+2)
        case Character("I").asciiValue:
            guard let value = buffer.readInteger(as: Int32.self) else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value, 1+4)
        case Character("i").asciiValue:
            guard let value = buffer.readInteger(as: UInt32.self) else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value, 1+4)
        case Character("l").asciiValue:
            guard let value = buffer.readInteger(as: Int64.self) else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value, 1+8)
        case Character("f").asciiValue:
            guard let value = buffer.psqlReadFloat() else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value, 1+4)
        case Character("d").asciiValue:
            guard let value = buffer.psqlReadDouble() else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value, 1+8)
        case Character("S").asciiValue:
            guard let (value, valueSize) = readLongStr(from: &buffer) else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value, 1 + valueSize)
        case Character("x").asciiValue:
            guard let size = buffer.readInteger(as: UInt32.self) else {
                throw DecodeError.field(.value(Type: type))
            }
            guard let value = buffer.readBytes(length: Int(size)) else {
                throw DecodeError.field(.value(Type: type))
            }            
            return (value, 1+Int(size));
        case Character("A").asciiValue:
            let (value, valueSize) = try! readArray(from: &buffer)
            return (value, 1 + valueSize)
        case Character("T").asciiValue:
            guard let timestamp = buffer.readInteger(as: Int64.self) else {
                throw DecodeError.field(.value(Type: type))
            }
            return (Date(timeIntervalSince1970: TimeInterval(timestamp)), 1+8)
        case Character("F").asciiValue:
            let (value, valueSize) = try! readDictionary(from: &buffer)
            return  (value, 1 + valueSize)
        case Character("D").asciiValue:
            guard let (value, valueSize) = readDecimal(from: &buffer) else {
                throw DecodeError.field(.value(Type: type))
            }
            return (value, 1 + valueSize)
        case Character("V").asciiValue:
            return (Empty(), 1)
        default:
            throw DecodeError.field(.unsupported(Type: type))
    }
}

func writeFieldValue(value: Any, into buffer: inout ByteBuffer) throws
{
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
            writeLongStr(value: v, into: &buffer)
        case let v as [UInt8]:
            buffer.writeInteger(Character("x").asciiValue!)
            buffer.writeBytes(v)
        case let v as [Any]:
            buffer.writeInteger(Character("A").asciiValue!)
            try! writeArray(values: v, into: &buffer)
        case let v as Date:
            buffer.writeInteger(Character("T").asciiValue!)
            buffer.writeInteger(Int64(v.timeIntervalSince1970*1000))
        case let v as Table:
            buffer.writeInteger(Character("F").asciiValue!)
            try! writeDictionary(values: v, into: &buffer)
        case let v as Decimal:
            buffer.writeInteger(Character("D").asciiValue!)
            try! writeDecimal(value: v, into: &buffer)
        case is Empty:
            buffer.writeInteger(Character("V").asciiValue!)
        default:
            throw EncodeError.field(.unsupported(Value: value))
    }
}
