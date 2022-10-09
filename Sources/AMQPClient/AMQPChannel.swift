import NIO
import NIOConcurrencyHelpers
import AMQPProtocol

public class AMQPChannel {
    let channelID: Frame.ChannelID

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

    init(channelID: Frame.ChannelID, connection: AMQPConnection, channelCloseFuture: EventLoopFuture<Void>) {
        self.channelID = channelID
        self.connection = connection

        connection.closeFuture().whenComplete { result in
            if self.connection === connection {
                self.connection = nil
            }
        }

        channelCloseFuture.whenComplete { result in
                if self.connection === connection {
                self.connection = nil
            }
        }
    }

    public func ack(deliveryTag: UInt64, multiple: Bool = false) {

    }

    public func ack(message: AMQPMessage.Delivery,  multiple: Bool = false) {
        self.ack(deliveryTag: message.deliveryTag, multiple: multiple)
    }

    public func nack(deliveryTag: UInt64, multiple: Bool = false, requeue: Bool = false) {

    }

    public func nack(message: AMQPMessage.Delivery, multiple: Bool = false, requeue: Bool = false) {
        self.nack(deliveryTag: message.deliveryTag, multiple: multiple, requeue: requeue)
    }

    public func reject(deliveryTag: UInt64, requeue: Bool = false) {

    }

    public func reject(message: AMQPMessage.Delivery, requeue: Bool = false) {
        self.reject(deliveryTag: message.deliveryTag, requeue: requeue)
    }

    public func basicGet(queue: String, noAck: Bool = true) -> AMQPMessage.Get? {
        return nil
    }

    public func basicPublish(body: [UInt8], exchange: String, routingKey: String, mandatory: Bool = false,  immediate: Bool = false, properties: Properties = Properties()) async throws  {
        try await self.basicPublish(body: body, exchange: exchange, routingKey: routingKey, mandatory: mandatory, immediate: immediate, properties: properties).get()
    }


    public func basicPublish(body: ByteBuffer, exchange: String, routingKey: String, mandatory: Bool = false,  immediate: Bool = false, properties: Properties = Properties()) -> EventLoopFuture<Void> {
        let body = body.getBytes(at: 0, length: body.readableBytes)!
        return self.basicPublish(body: body, exchange: exchange, routingKey: routingKey, mandatory: mandatory,  immediate: immediate, properties: properties)
    }


    public func basicPublish(body: [UInt8], exchange: String, routingKey: String, mandatory: Bool = false,  immediate: Bool = false, properties: Properties = Properties()) -> EventLoopFuture<Void> {
        let publish = Frame.method(self.channelID, .basic(.publish(.init(reserved1: 0, exchange: exchange, routingKey: routingKey, mandatory: mandatory, immediate: immediate))))
        let header = Frame.header(self.channelID, .init(classID: 60, weight: 0, bodySize: UInt64(body.count), properties: properties))
        let body = Frame.body(self.channelID, body: body)

        return self.connection!.sendFrames(frames: [publish, header, body], immediate: true)
    }

    public func close(reason: String = "", code: UInt16 = 200) -> EventLoopFuture<Void> {
        return self.connection!.sendFrame(frame: .method(self.channelID, .channel(.close(.init(replyCode: code, replyText: reason, classID: 0, methodID: 0)))))
        .flatMapThrowing { response in
            guard case .channel(let channel) = response, case .closed = channel else {
                throw ClientError.invalidResponse(response)
            }
            ()
        }
    }
}
