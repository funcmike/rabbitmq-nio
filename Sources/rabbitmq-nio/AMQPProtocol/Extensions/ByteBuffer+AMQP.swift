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
}