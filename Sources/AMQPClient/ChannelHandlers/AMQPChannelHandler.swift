import NIO
import AMQPProtocol


internal struct AMQPChannelHandler {
    private let channelID: Frame.ChannelID
    private var responseQueue: CircularBuffer<EventLoopPromise<AMQPResponse>>
    private var nextMessage: (properties: Properties, body: [UInt8]?)?

    init(channelID: Frame.ChannelID, initialQueueCapacity: Int = 3) {
        self.responseQueue = CircularBuffer(initialCapacity: initialQueueCapacity)
        self.channelID = channelID
    }

    mutating func append(promise: EventLoopPromise<AMQPResponse>) {
        self.responseQueue.append(promise)
    }

    mutating func processFrame(frame: Frame) {
        switch frame {
        case .method(let channelID, let method): 
            guard self.channelID == channelID else {
                preconditionFailure("Invalid channelID")
            }

            switch method {
            case .basic(let basic):
                switch basic {
                case .getOk(let get):
                    if  let promise = self.responseQueue.popFirst() , let msg = nextMessage {
                        promise.succeed(.message(.get(AMQPMessage.Get(
                            message: AMQPMessage.Delivery(
                                exchange: get.exchange,
                                routingKey: get.routingKey,
                                deliveryTag: get.deliveryTag,
                                properties: msg.properties,
                                redelivered: get.redelivered,
                                body: msg.body ?? [UInt8]()),
                            messageCount: get.messageCount))))
                    }
                default:
                    return
                }
            case .channel(_):
                ()
            default:
                return
            }
        case .header(let channelID, let header):
            guard self.channelID == channelID else {
                preconditionFailure("Invalid channelID")
            }
    
            self.nextMessage = (properties: header.properties, body:  nil)
        case .body(let channelID, let body):
            guard self.channelID == channelID else {
                preconditionFailure("Invalid channelID")
            }

            self.nextMessage?.body = body
        default:
            return
        }
    }
}
