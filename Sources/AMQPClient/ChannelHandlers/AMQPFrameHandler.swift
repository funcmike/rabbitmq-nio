import NIO
import AMQPProtocol

internal final class AMQPFrameHandler: ChannelDuplexHandler  {
    public typealias InboundIn = ByteBuffer
    public typealias InboundOut  = Frame
    public typealias OutboundIn = Frame
    public typealias OutboundOut = ByteBuffer

    private let state: ConnectionState = .start
    private var encoder: BufferedFrameEncoder!
    private let decoder = NIOSingleStepByteToMessageProcessor(AMQPFrameDecoder())


    func handlerAdded(context: ChannelHandlerContext) {
        self.encoder = BufferedFrameEncoder(
            buffer: context.channel.allocator.buffer(capacity: 256)
        )
        
        if context.channel.isActive {
            self.connected(context: context)
        }
    }

    public func channelActive(context: ChannelHandlerContext) {
        print("Client connected to \(context.remoteAddress!)")
        
        // `fireChannelActive` needs to be called BEFORE we set the state machine to connected,
        // since we want to make sure that upstream handlers know about the active connection before
        // it receives a         
        context.fireChannelActive()

        self.connected(context: context)
    }

    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {        
        let buffer = self.unwrapInboundIn(data)
        
        do {
            try self.decoder.process(buffer: buffer) { frame in
                print("got frame", frame)

                let action: ConnectionState.ConnectionAction

                switch frame {
                case .method(let channelID, let method): 
                    switch method {
                        case .connection(let connection):
                            switch connection {
                            case .start(_):
                                action = .start(channelID: channelID, user: "vxos", pass: "vxos")
                            case .tune(let channelMax, let frameMax, let heartbeat):
                                action = .tuneOpen(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat, vhost: "/")
                            case .openOk:
                                action = .none
                            default:
                                action = .none
                            }
                        default:
                            action = .none
                    }
                case .body(_, _):
                    action = .none
                case .header(_, _): 
                    action = .none
                case .heartbeat(let channelID): 
                    action = .heartbeat(channelID: channelID)
                }
                self.run(action, with: context)
            }
        } catch let error as ProtocolError {
            let action = self.state.errorHappened(error)
            self.run(action, with: context)
        } catch {
            preconditionFailure("Expected to only get ProtocolError from the AMQPFrameDecoder.")
        }
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let frame = self.unwrapOutboundIn(data)

        do {
            try self.encoder.encode(frame)
            context.write(wrapOutboundOut(self.encoder.flush()), promise: promise)
            print("write", frame)
        } catch {
            promise?.fail(error)
        }
    }

    func connected(context: ChannelHandlerContext) {
        self.run(.initStart, with: context)
    }

    func run(_ action: ConnectionState.ConnectionAction, with context: ChannelHandlerContext) {
        print("got action", action)

        switch action {
        case .initStart:
            var buffer = context.channel.allocator.buffer(capacity: PROTOCOL_START_0_9_1.count)
            buffer.writeBytes(PROTOCOL_START_0_9_1)
            context.writeAndFlush(self.wrapOutboundOut(buffer), promise: nil)
        case .start(let channelId, let user, let pass):
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

            let startOk = Frame.method(channelId, Method.connection(Connection.startOk(Connection.StartOk(
                clientProperties: clientProperties, mechanism: "PLAIN", response:"\u{0000}\(user)\u{0000}\(pass)", locale: "en_US"))))
            try! self.encoder.encode(startOk)
            context.writeAndFlush(wrapOutboundOut(self.encoder.flush()), promise: nil)
            print("send frame", startOk)
        case .none:
            ()
        case .close:
            ()
        case .tuneOpen(let channelMax, let frameMax, let heartbeat, let vhost):
            let tuneOk: Frame = Frame.method(0, Method.connection(Connection.tuneOk(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat)))
            try! self.encoder.encode(tuneOk)

            let open: Frame = Frame.method(0, Method.connection(Connection.open(Connection.Open(vhost: vhost))))
            try! self.encoder.encode(open)
            context.writeAndFlush(wrapOutboundOut(self.encoder.flush()), promise: nil)

            print("send frame", tuneOk)
            print("send frame", open)
        case .heartbeat(let channelID):
            let heartbeat: Frame = Frame.heartbeat(channelID)
            try! self.encoder.encode(heartbeat)
            context.writeAndFlush(wrapOutboundOut(self.encoder.flush()), promise: nil)
            print("send frame", heartbeat)
        }
    }
    
    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        print("error: ", error)

        // As we are not really interested getting notified on success or failure we just pass nil as promise to
        // reduce allocations.
        context.close(promise: nil)
    }
}