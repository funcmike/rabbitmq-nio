internal extension UInt8 {
    @usableFromInline
    func isBitSet(pos: Int) -> Bool {
        return (self & (1 << pos)) != 0
    }
}
