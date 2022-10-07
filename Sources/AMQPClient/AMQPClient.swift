import Atomics
import NIO
import Dispatch
import NIOConcurrencyHelpers

public class AMQPClient {
    let eventLoopGroup: EventLoopGroup
    let eventLoopGroupProvider: NIOEventLoopGroupProvider
    private let isShutdown = ManagedAtomic(false)
    let config: Configuration
    var shutdownListeners = AMQPListeners<Void>()
    private var _connection: AMQPConnection?
    private var lock = NIOLock()
    
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

    internal init(eventLoopGroupProvider: NIOEventLoopGroupProvider, config: Configuration) {
        self.config = config
        self.eventLoopGroupProvider = eventLoopGroupProvider
        switch eventLoopGroupProvider {
        case .createNew:
            self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        case .shared(let elg):
            self.eventLoopGroup = elg
        }
    }

    public func connect() -> EventLoopFuture<Void> {
        let connectFuture = AMQPConnection.create(use: self.eventLoopGroup, from: self.config)
        return connectFuture
            .map { connection  in 
                self.connection = connection
                ()
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

    //TODO: remove after client will be used normally
    public func closeFuture() -> EventLoopFuture<Void>? {
        return self._connection?.closeFuture()
    }

    deinit {
        guard isShutdown.load(ordering: .relaxed) else {
            preconditionFailure("Client not shut down before the deinit. Please call client.syncShutdownGracefully() when no longer needed.")
        }
    }
}