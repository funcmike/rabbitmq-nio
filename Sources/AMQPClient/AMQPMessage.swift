import NIO
import AMQPProtocol

public enum AMQPMessage {
    case delivery(Delivery)
    case get(Get)
    case `return`(Return)

    public struct Delivery {
        public let exchange: String
        public let routingKey: String
        public let deliveryTag: UInt64
        public let properties: Properties
        public let redelivered: Bool
        public let body: [UInt8]
    }

    public struct Get {
        public let message: Delivery
        public let messageCount: UInt32
    }

    public struct Return  {
        public let replyCode: String
        public let replyText: String
        public let exchange: String
        public let routingKey: String
        public let properties: Properties
        public let body: [UInt8]
    }
}
