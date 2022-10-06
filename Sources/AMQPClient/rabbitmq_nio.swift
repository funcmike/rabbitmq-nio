import NIO
import NIOPosix

@main
public struct rabbitmq_nio {
    public private(set) var text = "Hello, World!"

    public static func main() {
        print(rabbitmq_nio().text)
        setupEventloop(arguments: CommandLine.arguments)
    }
}

func setupEventloop(arguments: [String]) {
    let group: MultiThreadedEventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let bootstrap = ClientBootstrap(group: group)
        // Enable SO_REUSEADDR.
        .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
        .channelInitializer { channel in
            channel.pipeline.addHandler(AMQPFrameHandler())
        }
    defer {
        try! group.syncShutdownGracefully()
    }

    // First argument is the program path
    let arg1 = arguments.dropFirst().first
    let arg2 = arguments.dropFirst(2).first

    let defaultHost = "::1"
    let defaultPort: Int = 9999

    enum ConnectTo {
        case ip(host: String, port: Int)
        case unixDomainSocket(path: String)
    }

    let connectTarget: ConnectTo
    switch (arg1, arg1.flatMap(Int.init), arg2.flatMap(Int.init)) {
    case (.some(let h), _ , .some(let p)):
        /* we got two arguments, let's interpret that as host and port */
        connectTarget = .ip(host: h, port: p)
    case (.some(let portString), .none, _):
        /* couldn't parse as number, expecting unix domain socket path */
        connectTarget = .unixDomainSocket(path: portString)
    case (_, .some(let p), _):
        /* only one argument --> port */
        connectTarget = .ip(host: defaultHost, port: p)
    default:
        connectTarget = .ip(host: defaultHost, port: defaultPort)
    }

    let channel = try! { () -> NIOCore.Channel in
        switch connectTarget {
        case .ip(let host, let port):
            return try bootstrap.connect(host: host, port: port).wait()
        case .unixDomainSocket(let path):
            return try bootstrap.connect(unixDomainSocketPath: path).wait()
        }
    }()

    // Will be closed after we echo-ed back to the server.
    try! channel.closeFuture.wait()

    print("Client closed")
}
