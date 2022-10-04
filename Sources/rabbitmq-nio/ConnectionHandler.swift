
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
                                "connection_name": .longString("test"),
                                "product": .longString("rabbitmq-nio"),
                                "platform": .longString("Swift"),
                                "version":  .longString("0.1"),
                                "capabilities": .table([
                                    "publisher_confirms":           .bool(true),
                                    "exchange_exchange_bindings":   .bool(true),
                                    "basic.nack":                   .bool(true),
                                    "per_consumer_qos":             .bool(true),
                                    "authentication_failure_close": .bool(true),
                                    "consumer_cancel_notify":       .bool(true),
                                    "connection.blocked":           .bool(true),
                                ])
                            ]

                            let response: Frame = Frame.method(channelId, Method.connection(Connection.startOk(Connection.StartOk(
                                clientProperties: clientProperties, mechanism: "PLAIN", response:"\u{0000}vxos\u{0000}vxos", locale: "en_US"))))


                            buffer.clear()

                            try! response.encode(into: &buffer)
                    case .tune(let channelMax, let frameMax, let heartbeat):
                            let tuneOk: Frame = Frame.method(0, Method.connection(Connection.tuneOk(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat)))

                            buffer.clear()

                            try! tuneOk.encode(into: &buffer)

                            let open: Frame = Frame.method(0, Method.connection(Connection.open(Connection.Open(vhost: "/"))))

                            try! open.encode(into: &buffer)
                    case .openOk:
                        print("connected")
                        return        
                    default: 
                        buffer.clear()
                    }
                case .channel(_): 
                    buffer.clear()
                case .exchange(_):
                    buffer.clear()
                case .queue(_):
                    buffer.clear() 
                case .basic(_):
                    buffer.clear()
                case .confirm(_):
                    buffer.clear()
                case .tx(_): 
                    buffer.clear()
                }
            case .heartbeat(let channelID):
                let heartbeat: Frame = Frame.heartbeat(channelID)

                buffer.clear()
                
                try! heartbeat.encode(into: &buffer)
            default:
                buffer.clear()
                return

        }
        


        // let checkFrame = try! Frame.decode(from: &buffer)

        // print(checkFrame)

        //buffer.clear()

        //try! checkFrame.encode(into: &buffer)

        //try! checkFrame.encode(into: &buffer)

    }
}