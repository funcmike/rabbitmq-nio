import NIO
import AMQPProtocol

public enum AMQPResponse {
    case channel(Channel)
    case connection(Connection)

    public enum Channel {
        case opened(channelID: Frame.ChannelID, closeFuture: EventLoopFuture<Void>)
        case closed(Frame.ChannelID)
        case message(AMQPMessage)
        case published
    }

    public enum Connection {
        case connected(channelMax: UInt16)
    }
}
