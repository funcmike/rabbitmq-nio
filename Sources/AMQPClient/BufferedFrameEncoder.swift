import NIOCore
import AMQPProtocol

struct BufferedFrameEncoder {
    private enum State {
        case flushed
        case writable
    }
    
    private var buffer: ByteBuffer
    private var state: State = .writable
    
    init(buffer: ByteBuffer) {
        self.buffer = buffer
    }

    mutating func write(from bytes: [UInt8]) {
        self.prepare()
        self.buffer.writeBytes(bytes)
    }
    
    mutating func encode(_ frame: Frame) throws {
        self.prepare()
        try frame.encode(into: &self.buffer)
    }
    
    mutating func flush() -> ByteBuffer {
        self.state = .flushed
        return self.buffer
    }

    mutating func prepare() {
        switch self.state {
        case .flushed:
            self.buffer.clear()
            self.state = .writable
        case .writable:
            break
        }
    }
}