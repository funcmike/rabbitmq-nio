import NIO
import AMQPProtocol

public enum AMQPResponse {
    case message(AMQPMessage)
    case connection(Connection)

    public enum Connection {
        case connected
        case channelOpened(Frame.ChannelID)
    }
}
