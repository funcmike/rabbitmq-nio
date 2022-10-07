import NIO
import NIOSSL
import AMQPProtocol

final class AMQPConnection {
    let channel: NIO.Channel

    init(channel: NIO.Channel) {
        self.channel = channel
    }

    static func create(use eventLoopGroup: EventLoopGroup, from config: Configuration) -> EventLoopFuture<AMQPConnection> {
        return self.boostrapChannel(use: eventLoopGroup, from: config)
            .map { AMQPConnection(channel: $0) }
    }

    static func boostrapChannel(use eventLoopGroup: EventLoopGroup, from config: Configuration) -> EventLoopFuture<NIO.Channel> {
        let eventLoop = eventLoopGroup.next()
        let channelPromise = eventLoop.makePromise(of: NIO.Channel.self)
        let serverConfig: Configuration.Server
    
        switch config {
        case .tls(_, _, let server):
            serverConfig = server
        case .plain(let server):
            serverConfig = server
        }

        do {
            let bootstrap = try boostrapClient(use: eventLoopGroup, from: config)

            bootstrap
                .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .channelOption(ChannelOptions.socket(IPPROTO_TCP, TCP_NODELAY), value: 1)
                .connectTimeout(serverConfig.timeout)
                .channelInitializer { channel in
                    channel.pipeline.addHandler(AMQPFrameHandler())
                }
                .connect(host: serverConfig.host, port: serverConfig.port)
                .cascadeFailure(to: channelPromise)
        } catch {
            channelPromise.fail(error)
        }

        return channelPromise.futureResult        
    }

    static func boostrapClient(use eventLoopGroup: EventLoopGroup, from config: Configuration) throws -> NIOClientTCPBootstrap {
        guard let clientBootstrap = ClientBootstrap(validatingGroup: eventLoopGroup) else {
            preconditionFailure("Cannot create bootstrap for the supplied EventLoop")
        }

        switch config {            
        case .plain(_): 
            return NIOClientTCPBootstrap(clientBootstrap, tls: NIOInsecureNoTLS())
        case .tls(let tls, let sniServerName, let server):
            let sslContext = try NIOSSLContext(configuration: tls ?? TLSConfiguration.makeClientConfiguration())
            let tlsProvider = try NIOSSLClientTLSProvider<ClientBootstrap>(context: sslContext, serverHostname: sniServerName ?? server.host)
            let bootstrap = NIOClientTCPBootstrap(clientBootstrap, tls: tlsProvider)
            return bootstrap.enableTLS()
        }        
    }

    func sendFrameNoWait(_ frame: AMQPProtocol.Frame) -> EventLoopFuture<Void> {
        return self.channel.writeAndFlush(frame)
    }

    func close() -> EventLoopFuture<Void> {
        if self.channel.isActive {
            return self.channel.close()
        } else {
            return self.channel.eventLoop.makeSucceededFuture(())
        }
    }

    //TODO: remove after client will be used normally
    func closeFuture() -> EventLoopFuture<Void> {
        return self.channel.closeFuture
    }
}
