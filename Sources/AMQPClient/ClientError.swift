import AMQPProtocol

public enum ClientError: Error {
    case `protocol`(ProtocolError)
    case alreadyShutdown
    case tooManyOpenedChannels
    case invalidResponse(AMQPResponse)
}