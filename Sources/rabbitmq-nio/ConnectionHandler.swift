
import NIOCore

public struct ConnectionHandler: BufferHandler {
    func sendActive(buffer: inout NIOCore.ByteBuffer) throws {
        buffer.writeBytes(PROTOCOL_START_0_9_1)
    }

    func sendResponse(buffer: inout ByteBuffer) throws {
        let frame = try! Frame.decode(from: &buffer)

        print("got frame", frame)

        switch frame {
            case .method(let channelId, let method):
                switch method {
                    case .connection(let connection):
                        switch connection {
                            case .start:
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

                                    let response: Frame = Frame.method(channelId, Method.connection(Connection.startOk(ConnnectionStartOk(
                                        clientProperties: clientProperties, mechanism: "PLAIN", response:"\u{0000}vxos\u{0000}vxos", locale: "en_US"))))


                                    buffer.clear()

                                    try! response.encode(into: &buffer)
                            case .tune(let tune):
                                    let tuneOk: Frame = Frame.method(0, Method.connection(Connection.tuneOk(TuneOk(channelMax: tune.channelMax, frameMax: tune.frameMax, heartbeat: tune.heartbeat))))

                                    buffer.clear()

                                    try! tuneOk.encode(into: &buffer)

                                    let open: Frame = Frame.method(0, Method.connection(Connection.open(Open(vhost: "/"))))

                                    try! open.encode(into: &buffer)
                            case .openOk:
                                print("connected")
                                return
                               
                        default: 
                                return
                        }
                }

        }
        


        // let checkFrame = try! Frame.decode(from: &buffer)

        // print(checkFrame)

        //buffer.clear()

        //try! checkFrame.encode(into: &buffer)

        //try! checkFrame.encode(into: &buffer)

    }
}