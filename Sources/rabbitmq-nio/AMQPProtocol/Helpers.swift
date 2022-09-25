
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

func readLongStr(from buffer: inout ByteBuffer) -> (String, Int)?
{
    guard let size = buffer.readInteger(as: UInt32.self) else {
        return nil
    }

    guard let value = buffer.readString(length: Int(size)) else {
        return nil
    }    

    return (value, 4 + Int(size))
}


func readDictionary(from buffer: inout ByteBuffer) -> ([String:Any], Int)? {
    guard let size = buffer.readInteger(as: UInt32.self) else {
        return nil
    }

    var result: [String:Any] = [:]

    var bytesRead = 0

    while bytesRead < size {
        guard let (key, keySize) = readShortStr(from: &buffer) else {
            return nil
        }

        bytesRead += keySize

        guard let (value, valueSize) = readFieldValue(from: &buffer) else {
            print("value", key,  bytesRead, size, result)
            return nil
        }
    
        bytesRead += valueSize

        result[key] = value
    }
    
    return (result, 4 + bytesRead)
}

func readArray(from buffer: inout ByteBuffer) -> ([Any], Int)? {
    guard let size = buffer.readInteger(as: UInt32.self) else {
            return nil
    }

    var result: [Any] = []

    var bytesRead = 0

    while bytesRead < size {
        guard let (value, valueSize) = readFieldValue(from: &buffer) else {
            return nil
        }
        
        bytesRead += valueSize

        result.append(value)
    }

    return (result, 4 + bytesRead)
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


func readFieldValue(from buffer: inout ByteBuffer) -> (Any, Int)? {
    guard let type = buffer.readInteger(as: UInt8.self) else {
        return nil
    }

    switch type {
        case Character("t").asciiValue:
            guard let value = buffer.readInteger(as: UInt8.self) else {
                return nil
            }
            return (value == 1, 1+1)
        case Character("b").asciiValue:
            guard let value = buffer.readInteger(as: Int8.self) else {
                return nil
            }
            return (value, 1+1)
        case Character("B").asciiValue:
            guard let value = buffer.readInteger(as: UInt8.self) else {
                return nil
            }
            return (value, 1+1)
        case Character("s").asciiValue:
            guard let value = buffer.readInteger(as: Int16.self) else {
                return nil
            }
            return (value, 1+2)
        case Character("u").asciiValue:
            guard let value = buffer.readInteger(as: UInt16.self) else {
                return nil
            }
            return (value, 1+2)
        case Character("I").asciiValue:
            guard let value = buffer.readInteger(as: Int32.self) else {
                return nil
            }
            return (value, 1+4)
        case Character("i").asciiValue:
            guard let value = buffer.readInteger(as: UInt32.self) else {
                return nil
            }
            return (value, 1+4)
        case Character("l").asciiValue:
            guard let value = buffer.readInteger(as: Int64.self) else {
                return nil
            }
            return (value, 1+8)
        case Character("f").asciiValue:
            guard let value = buffer.psqlReadFloat() else {
                return nil
            }
            return (value, 1+4)
        case Character("d").asciiValue:
            guard let value = buffer.psqlReadDouble() else {
                return nil
            }
            return (value, 1+8)
        case Character("S").asciiValue:
            guard let (value, valueSize) = readLongStr(from: &buffer) else {
                return nil
            }
            return (value, 1 + valueSize)
        case Character("x").asciiValue:
            guard let size = buffer.readInteger(as: UInt32.self) else {
                return nil
            }
            guard let value = buffer.readBytes(length: Int(size)) else {
                return nil
            }            
            return (value, 1+Int(size));
        case Character("A").asciiValue:
            guard let (value, valueSize) = readArray(from: &buffer) else {
                return nil
            }
            return (value, 1 + valueSize)
        case Character("T").asciiValue:
            guard let timestamp = buffer.readInteger(as: Int64.self) else {
                return nil
            }
            return (Date(timeIntervalSince1970: TimeInterval(timestamp)), 1+8)
        case Character("F").asciiValue:
            guard let (value, valueSize) = readDictionary(from: &buffer) else {
                return nil
            }
            return  (value, 1 + valueSize)
        case Character("D").asciiValue:
            guard let (value, valueSize) = readDecimal(from: &buffer) else {
                return nil
            }
            return (value, 1 + valueSize)
        case Character("V").asciiValue:
            return (Empty(), 1)
        default:
            return nil
    }
}