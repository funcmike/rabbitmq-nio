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
    let arg1 = arguments.dropFirst().first
    let arg2 = arguments.dropFirst(2).first


    let connectTarget: (host: String, port: Int)?
    switch (arg1, arg1.flatMap(Int.init), arg2.flatMap(Int.init)) {
    case (.some(let h), _ , .some(let p)):
        /* we got two arguments, let's interpret that as host and port */
        connectTarget = (host: h, port: p)
    default:
        connectTarget = nil
    }

    let client: AMQPClient

    if let target = connectTarget {
        client = AMQPClient(eventLoopGroupProvider: .createNew, config: .plain(Configuration.Server(
             host: target.0, port: target.1)))
    } else {
         client = AMQPClient(eventLoopGroupProvider: .createNew, config: .plain(Configuration.Server()))
    }

    defer {
        client.shutdown({error  in return ()})
    }

    let result = client.connect()
    try! result.wait()
    

    try! client.closeFuture()?.wait()
    print("Client closed")
}
