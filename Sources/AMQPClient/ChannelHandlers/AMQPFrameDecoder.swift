import NIO
import AMQPProtocol

struct AMQPFrameDecoder: NIOSingleStepByteToMessageDecoder {
    mutating func decodeLast(buffer: inout ByteBuffer, seenEOF: Bool) throws -> Frame? {
        try self.decode(buffer: &buffer)
    }

    typealias InboundOut = Frame

    mutating func decode(buffer: inout ByteBuffer) throws -> Frame? {
        let startReaderIndex = buffer.readerIndex

        do {
            let frame = try Frame.decode(from: &buffer)
            return frame
        } catch let error as ProtocolError {
            buffer.moveReaderIndex(to: startReaderIndex)
            throw ClientError.protocol(error)
        } catch {
            preconditionFailure("Expected to only see `ProtocolError`s here.")
        }
    }
}
