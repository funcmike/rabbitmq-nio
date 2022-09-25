
import NIOCore

public struct ConnectionHandler: BufferHandler {
    func sendActive(buffer: inout NIOCore.ByteBuffer) throws {
        buffer.writeBytes(PROTOCOL_START_0_9_1)
    }

    func sendResponse(buffer: inout ByteBuffer) throws {
        let frame = try! Frame.decode(from: &buffer)

        print(frame)
        
        let clientProperties: Table = [
            "connection_name": "test",
            "product": "rabbitmq-nio",
            "platform": "Swift",
            "version":  "0.1",
            "capabilities": [
                "publisher_confirms":           true,
                "exchange_exchange_bindings":   true,
                "basic.nack":                   true,
                "per_consumer_qos":             true,
                "authentication_failure_close": true,
                "consumer_cancel_notify":       true,
                "connection.blocked":           true,
            ]
        ]

        let response: Frame = Frame.method(1, Method.connection(Connection.startOk(ConnnectionStartOk(
            clientProperties: clientProperties, mechanism: "PLAIN", response:"\u{0000}vxos\u{0000}vxos", locale: "en_US"))))
        
        print(response)

        buffer.clear()

        try! response.encode(into: &buffer)

        let checkFrame = try! Frame.decode(from: &buffer)

        print(checkFrame)

        try! checkFrame.encode(into: &buffer)

    }
}