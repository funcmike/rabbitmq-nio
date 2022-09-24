
import NIOCore

public struct AMQPConnection: BufferHandler {
    func sendActive(buffer: inout NIOCore.ByteBuffer) throws {
        buffer.writeBytes(PROTOCOL_START_0_9_1)
    }

    func sendResponse(buffer: inout NIOCore.ByteBuffer) throws {
        let frame = try! Frame.decode(from: &buffer)
        
        print(frame)
        buffer.writeString("response")
    }
}