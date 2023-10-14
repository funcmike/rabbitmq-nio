import AMQPProtocol
import NIOCore

struct AMQPConnectionHandler: Sendable {
    let channel: Channel
    let multiplexer: NIOLoopBound<AMQPConnectionMultiplexHandler>

    init(channel: Channel, multiplexer: AMQPConnectionMultiplexHandler) {
        self.channel = channel
        self.multiplexer = NIOLoopBound(multiplexer, eventLoop: channel.eventLoop)
    }

    func write(
        _ outbound: AMQPOutbound,
        responsePromise: EventLoopPromise<AMQPResponse>?,
        writePromise: EventLoopPromise<Void>?
    ) {
        channel.writeAndFlush((outbound, responsePromise), promise: writePromise)
    }

    func close(reason: String = "", code: UInt16 = 200) -> EventLoopFuture<Void> {
        let responsePromise = channel.eventLoop.makePromise(of: AMQPResponse.self)

        write(.frame(.init(channelID: 0, payload: .method(.connection(.close(.init(replyCode: code, replyText: reason, failingClassID: 0, failingMethodID: 0)))))),
            responsePromise: responsePromise,
            writePromise: nil)

        return responsePromise.futureResult.flatMapThrowing { response in
            guard case let .connection(connection) = response, case .closed = connection else {
                throw AMQPConnectionError.invalidResponse(response)
            }
        }
    }

    func openChannel(id: Frame.ChannelID) -> EventLoopFuture<AMQPChannelHandler> {
        let promise = channel.eventLoop.makePromise(of: AMQPResponse.self)

        write(.frame(.init(channelID: id, payload: .method(.channel(.open(reserved1: ""))))),
              responsePromise: promise,
              writePromise: nil)

        return promise.futureResult.flatMapThrowing { response in
            guard case let .channel(channel) = response, case let .opened(channelID) = channel, channelID == id else {
                throw AMQPConnectionError.invalidResponse(response)
            }

            let channelHandler = AMQPChannelHandler(parent: self, channelID: channelID, eventLoop: self.channel.eventLoop)
            self.multiplexer.value.addChannelHandler(channelHandler, forId: id)

            return channelHandler
        }
    }
}
