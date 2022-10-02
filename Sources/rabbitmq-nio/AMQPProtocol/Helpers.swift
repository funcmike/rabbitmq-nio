import NIOCore
import Foundation

public struct Empty {}

func readShortStr(from buffer: inout ByteBuffer) throws -> (String, Int) {
    guard let size = buffer.readInteger(as: UInt8.self) else {
        throw DecodeError.value(type: UInt8.self, message: "cannot read short string size")
    }    

    guard let value = buffer.readString(length: Int(size)) else {
        throw DecodeError.value(type: String.self, message: "cannot read short string value")
    }

    return (value, 1 + Int(size))
}

func writeShortStr(value: String, into buffer: inout ByteBuffer) throws {
    let startIndex: Int = buffer.writerIndex

    buffer.writeInteger(UInt8(0)) // placeholder for size

    let size = buffer.writeString(value)

    guard size <= UINT8_MAX else {
        throw EncodeError.unsupported(value: value, message: "Short string too long, max \(UINT8_MAX)")
    }

    buffer.setInteger(UInt8(size), at: startIndex)
}

func readLongStr(from buffer: inout ByteBuffer) throws -> (String, Int) {
    guard let size: UInt32 = buffer.readInteger(as: UInt32.self) else {
        throw DecodeError.value(type:  UInt32.self, message: "cannot read long string size")
    }

    guard let value = buffer.readString(length: Int(size)) else {
        throw DecodeError.value(type:  String.self, message: "cannot read long string value")
    }

    return (value, 4 + Int(size))
}

func writeLongStr(value: String, into buffer: inout ByteBuffer) throws {
    let startIndex: Int = buffer.writerIndex

    buffer.writeInteger(UInt32(0))  // placeholde for size

    let size = buffer.writeString(value)

    guard size <= UINT32_MAX else {
       throw EncodeError.unsupported(value: value, message: "long string too long, max \(UINT32_MAX)")
    }

    buffer.setInteger(UInt32(size), at: startIndex)
}

