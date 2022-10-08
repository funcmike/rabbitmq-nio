import AMQPProtocol

public struct AMQPChannel {
    let channelID: Frame.ChannelID

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

    func basicGet(queue: String, noAck: Bool = true) -> AMQPMessage.Get? {
        return nil
    }
}
