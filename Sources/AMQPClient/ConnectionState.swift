import AMQPProtocol


enum ConnectionState {
    case start

    enum ConnectionAction {
        case initStart
        case start(channelID: Frame.ChannelID, user: String, pass: String)
        case tuneOpen(channelMax: UInt16, frameMax: UInt32, heartbeat: UInt16, vhost: String)
        case heartbeat(channelID: Frame.ChannelID)
        case close
        case none
    }

    func errorHappened(_ error: ProtocolError) -> ConnectionAction {
        return .close
    }
}