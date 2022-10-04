import NIOCore

internal extension ByteBuffer {
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
            throw ProtocolError.decode(param: "size", type: UInt8.self)
        }    

        guard let value = self.readString(length: Int(size)) else {
            throw ProtocolError.decode(param: "value", type: String.self, message: "cannot read short string value")
        }

        return (value, 1 + Int(size))
    }

    @usableFromInline
    mutating func writeShortString(_ shortString: String) throws {
        let startIndex: Int = self.writerIndex

        self.writeInteger(UInt8(0)) // placeholder for size

        let size = self.writeString(shortString)

        guard size <= UInt8.max else {
            throw ProtocolError.invalid(param: "size", value: shortString, message: "short string too long, max \(UInt8.max)")
        }

        self.setInteger(UInt8(size), at: startIndex)
    }

    @usableFromInline
    mutating func readLongString() throws -> (String, Int) {
        guard let size: UInt32 = self.readInteger(as: UInt32.self) else {
            throw ProtocolError.decode(param: "size", type:  UInt32.self)
        }

        guard let value = self.readString(length: Int(size)) else {
            throw ProtocolError.decode(param: "value", type:  String.self, message: "cannot read long string value")
        }

        return (value, 4 + Int(size))
    }

    @usableFromInline
    mutating func writeLongString(_ longString: String) throws {
        let startIndex: Int = self.writerIndex

        self.writeInteger(UInt32(0))  // placeholde for size

        let size = self.writeString(longString)

        guard size <= UInt32.max else {
        throw ProtocolError.invalid(param: "size", value: longString, message: "long string too long, max \(UInt32.max)")
        }

        self.setInteger(UInt32(size), at: startIndex)
    }
}