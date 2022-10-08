import AMQPProtocol


enum ConnectionState {
    case connecting

    enum ConnectionAction {
        case start(channelID: Frame.ChannelID, user: String, pass: String)
        case tuneOpen(channelMax: UInt16, frameMax: UInt32, heartbeat: UInt16, vhost: String)
        case heartbeat(channelID: Frame.ChannelID)
        case channelOpen(Frame.ChannelID)
        case channel(Frame.ChannelID, Frame)
        case connected
        case close
        case none
    }

    func errorHappened(_ error: ProtocolError) -> ConnectionAction {
        return .close
    }
}