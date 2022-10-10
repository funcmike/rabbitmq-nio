import Atomics
import NIO
import Dispatch
import NIOConcurrencyHelpers
import AMQPProtocol

public final class AMQPClient {
    let eventLoopGroup: EventLoopGroup
    let eventLoopGroupProvider: NIOEventLoopGroupProvider
    let config: Configuration

    private let isShutdown = ManagedAtomic(false)
    var shutdownListeners = AMQPListeners<Void>()

    private var lock = NIOLock()
    private var _connection: AMQPConnection?
    var connection: AMQPConnection? {
        get {
            self.lock.withLock {
                _connection
            }
        }
        set {
            self.lock.withLock {
                _connection = newValue
            }
        }
    }

    public init(eventLoopGroupProvider: NIOEventLoopGroupProvider, config: Configuration) {
        self.config = config
        self.eventLoopGroupProvider = eventLoopGroupProvider
        switch eventLoopGroupProvider {
        case .createNew:
            self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        case .shared(let elg):
            self.eventLoopGroup = elg
        }
    }

    public func connect() ->  EventLoopFuture<Void> {
        return AMQPConnection.create(use: self.eventLoopGroup, from: self.config)
            .flatMap { connection  in 
                self.connection = connection
                connection.closeFuture().whenComplete { result in
                    if self.connection === connection {
                        self.connection = nil
                    }
                }
                return connection.sendBytes(bytes: PROTOCOL_START_0_9_1, immediate: true)
            }
            .flatMapThrowing { response in
                guard case .connection(let connection) = response, case .connected = connection else {
                    throw ClientError.invalidResponse(response)
                }
                return ()
            }
    }

    public func openChannel(id: Frame.ChannelID) -> EventLoopFuture<AMQPChannel> {
        return self.connection!.sendFrame(frame: .method(id, .channel(.open(reserved1: ""))), immediate: true)
            .flatMapThrowing  { response in 
                guard case .channel(let channel) = response, case .opened(let channelID, let closeFuture) = channel, id == channelID else {
                    throw ClientError.invalidResponse(response)
                }
                return AMQPChannel(channelID: channelID, connection: self.connection!, channelCloseFuture: closeFuture)
            }
    }

    public func shutdown(queue: DispatchQueue = .global(), _ callback: @escaping (Error?) -> Void) {
        guard self.isShutdown.compareExchange(expected: false, desired: true, ordering: .relaxed).exchanged else {
            callback(ClientError.alreadyShutdown)
            return
        }
        let eventLoop = self.eventLoopGroup.next()
        let closeFuture: EventLoopFuture<Void>

        self.shutdownListeners.notify(.success(()))
        if let connection = self.connection {
            closeFuture = connection.close()
        } else {
            closeFuture = eventLoop.makeSucceededVoidFuture()
        }
        closeFuture.whenComplete { result in
            let closeError: Error?
            switch result {
            case .failure(let error):
                if case ChannelError.alreadyClosed = error {
                    closeError = nil
                } else {
                    closeError = error
                }
            case .success:
                closeError = nil
            }
            self.shutdownListeners.notify(.success(()))
            self.shutdownEventLoopGroup(queue: queue) { error in
                callback(closeError ?? error)
            }
        }
    }

    /// shutdown EventLoopGroup
    private func shutdownEventLoopGroup(queue: DispatchQueue, _ callback: @escaping (Error?) -> Void) {
        switch self.eventLoopGroupProvider {
        case .shared:
            queue.async {
                callback(nil)
            }
        case .createNew:
            self.eventLoopGroup.shutdownGracefully(queue: queue, callback)
        }
    }


    /// Add shutdown listener. Called whenever the client is shutdown
    public func addShutdownListener(named name: String, _ listener: @escaping (Result<Void, Swift.Error>) -> Void) {
        self.shutdownListeners.addListener(named: name, listener: listener)
    }

    /// Remove named shutdown listener
    public func removeShutdownListener(named name: String) {
        self.shutdownListeners.removeListener(named: name)
    }

    public func closeFuture() -> EventLoopFuture<Void>? {
        return self._connection?.closeFuture()
    }

    deinit {
        guard isShutdown.load(ordering: .relaxed) else {
            preconditionFailure("Client not shut down before the deinit. Please call client.syncShutdownGracefully() when no longer needed.")
        }
    }
}