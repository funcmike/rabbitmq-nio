import Foundation
import AMQPProtocol

public extension AMQPClient {
    func connect() async throws {
        return try await self.connect().get()
    }

    func openChannel(id: Frame.ChannelID) async throws -> AMQPChannel {
        return try await self.openChannel(id: id).get()
    }

    func shutdown(queue: DispatchQueue = .global()) async throws {
        return try await withUnsafeThrowingContinuation { cont in
            shutdown(queue: queue) { error in
                if let error = error {
                    cont.resume(throwing: error)
                } else {
                    cont.resume()
                }
            }
        }
    }
}
