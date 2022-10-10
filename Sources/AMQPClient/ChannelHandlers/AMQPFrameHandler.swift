import NIO
import AMQPProtocol

internal final class AMQPFrameHandler: ChannelDuplexHandler  {
    enum AMQPOutbound {
        case frame(Frame)
        case bulk([Frame])
        case bytes([UInt8])
    }

    public typealias InboundIn = ByteBuffer
    public typealias InboundOut  = Frame

    public typealias OutboundCommandPayload = (outbound: AMQPOutbound, responsePromise: EventLoopPromise<AMQPResponse>?)

    public typealias OutboundIn = OutboundCommandPayload
    public typealias OutboundOut = ByteBuffer

    private let state: ConnectionState = .connecting
    private var encoder: BufferedFrameEncoder!
    private let decoder = NIOSingleStepByteToMessageProcessor(AMQPFrameDecoder())
    
    private var responseQueue: CircularBuffer<EventLoopPromise<AMQPResponse>>
    private var channels: [Frame.ChannelID: AMQPChannelHandler] = [:]
    private var channelMax: UInt16?

    private let config: Configuration.Server

    init(config: Configuration.Server, initialQueueCapacity: Int = 3) {
        self.responseQueue = CircularBuffer(initialCapacity: initialQueueCapacity)
        self.config = config
    }

    func handlerAdded(context: ChannelHandlerContext) {
        self.encoder = BufferedFrameEncoder(
            buffer: context.channel.allocator.buffer(capacity: 256)
        )
    }

    func channelActive(context: ChannelHandlerContext) {
        print("Client connected to \(context.remoteAddress!)")
        
        // `fireChannelActive` needs to be called BEFORE we set the state machine to connected,
        // since we want to make sure that upstream handlers know about the active connection before
        // it receives a         
        return context.fireChannelActive()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {        
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
                            action = .start(channelID: channelID, user: config.user, pass: config.password)
                        case .tune(let channelMax, let frameMax, let heartbeat):
                            action = .tuneOpen(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat, vhost: config.vhost)
                        case .openOk:
                            action = .connected
                        default:
                            action = .none
                        }
                    case .basic(let basic):
                        switch basic {
                        case .deliver, .getEmpty, .getOk, .return, .ack, .nack, .cancel:
                            action = .channel(channelID, frame)//
                        default:
                            action = .none
                        }
                    case .channel(let channel):
                        switch channel {
                        case .openOk, .flow, .close:
                            action = .channel(channelID, frame)
                        default:
                            action = .none
                        }
                    default:
                        action = .none
                    }
                case .header(let channelID, _), .body(let channelID, _):
                    action = .channel(channelID, frame)
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
        let (outbound, responsePromise) = self.unwrapOutboundIn(data)

        switch outbound {
        case .frame(let frame): 
            print("write outbound frame", frame)
    
            do {
                try self.encoder.encode(frame)
            } catch {
                responsePromise?.fail(error)
                promise?.fail(error)
                return
            }

            guard self.processChannelOpen(context: context, frame: frame, responsePromise: responsePromise, promise: promise) else {
                return
            }

            let channelID = frame.channelID

            if let promise = responsePromise {
                if channelID == 0  {
                    self.responseQueue.append(promise) 
                } else if let channel = self.channels[channelID] {
                    channel.append(promise: promise)
                }
            }
        case .bulk(let frames):            
            let channelID = frames.first?.channelID

            for frame in frames {
                do {
                    try self.encoder.encode(frame)
                } catch {
                    responsePromise?.fail(error)
                    promise?.fail(error)
                    return
                }

                guard self.processChannelOpen(context: context, frame: frame, responsePromise: responsePromise, promise: promise) else {
                    return
                }
            }

            if let promise = responsePromise, let id = channelID {
                if id == 0  {
                    self.responseQueue.append(promise)
                } else if let channel = self.channels[id] {
                    channel.append(promise: promise)
                }
            }
        case .bytes(let bytes):
            print("write outbound bytes", bytes)

            if let promise = responsePromise {
                self.responseQueue.append(promise)
            }

            _ = self.encoder.writeBytes(from: bytes)
        }

        return context.write(wrapOutboundOut(self.encoder.flush()), promise: promise)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        print("error: ", error)

        // As we are not really interested getting notified on success or failure we just pass nil as promise to
        // reduce allocations.
        return context.close(promise: nil)
    }

    private func processChannelOpen(context: ChannelHandlerContext, frame: Frame, responsePromise: EventLoopPromise<AMQPResponse>?, promise:  EventLoopPromise<Void>?) -> Bool {
        if case .method(let channelID, let method) = frame, case .channel(let channel) = method, case .open = channel {

            guard let limit = self.channelMax, (limit == 0 || self.channels.count < limit) else {
                responsePromise?.fail(ClientError.tooManyOpenedChannels)
                promise?.fail(ClientError.tooManyOpenedChannels)
                return false
            }

            let closePromise = context.eventLoop.makePromise(of: Void.self)
            self.channels[channelID] = AMQPChannelHandler(channelID: channelID, closePromise: closePromise)
        }

        return true
    }

    private func run(_ action: ConnectionState.ConnectionAction, with context: ChannelHandlerContext) {
        switch action {
        case .start(let channelID, let user, let pass):
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

            let startOk = Frame.method(channelID, .connection(.startOk(.init(
                clientProperties: clientProperties, mechanism: "PLAIN", response:"\u{0000}\(user)\u{0000}\(pass)", locale: "en_US"))))
            try! self.encoder.encode(startOk)
            context.writeAndFlush(wrapOutboundOut(self.encoder.flush()), promise: nil)
        case .none:
            ()
        case .close:
            ()
        case .tuneOpen(let channelMax, let frameMax, let heartbeat, let vhost):
            let tuneOk: Frame = Frame.method(0, .connection(.tuneOk(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat)))
            try! self.encoder.encode(tuneOk)

            let open: Frame = Frame.method(0, .connection(.open(.init(vhost: vhost))))
            try! self.encoder.encode(open)
            context.writeAndFlush(wrapOutboundOut(self.encoder.flush()), promise: nil)

            self.channelMax = channelMax
        case .heartbeat(let channelID):
            let heartbeat: Frame = Frame.heartbeat(channelID)
            try! self.encoder.encode(heartbeat)
            context.writeAndFlush(wrapOutboundOut(self.encoder.flush()), promise: nil)
        case .channel(let channelID, let frame):
            if let channel = self.channels[channelID] {
                channel.processFrame(frame: frame)
            }
        case .connected:
            if let promise =  responseQueue.popFirst() {
                promise.succeed(.connection(.connected(channelMax: channelMax!)))
            }
        }
    }
}