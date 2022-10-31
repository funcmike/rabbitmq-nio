//===----------------------------------------------------------------------===//
//
// This source file is part of the RabbitMQNIO project
//
// Copyright (c) 2022 Krzysztof Majk
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIO
import AMQPProtocol

public enum AMQPResponse {
    case channel(Channel)
    case connection(Connection)

    public enum Channel {
        case opened(Opened)
        case closed(Frame.ChannelID)
        case message(Message)
        case queue(Queue)
        case exchange(Exchange)
        case basic(Basic)
        case confirm(Confirm)
        case tx(Tx)
        case flowed(active: Bool)

        public struct Opened {
            public let channelID: Frame.ChannelID
            let notifier: Notifiable

            internal init(channelID: Frame.ChannelID, notifier: Notifiable) {
                self.channelID = channelID
                self.notifier = notifier
            }
        }

        public enum Queue {
            case declared(queueName: String, messageCount: UInt32, consumerCount: UInt32)
            case binded
            case purged(messageCount: UInt32)
            case deleted(messageCount: UInt32)
            case unbinded
        }
        
        public enum Exchange {
            case declared
            case deleted
            case binded
            case unbinded
        }

        public enum Basic {
            case recovered
            case qosOk
            case consumeOk(ConsumeOk)
            case canceled
            case publishConfirm(PublishConfirm)

            public enum PublishConfirm {
                case ack(deliveryTag: UInt64, multiple: Bool)
                case nack(deliveryTag: UInt64, multiple: Bool)
            }

            public struct ConsumeOk {
                public let consumerTag: String
            }
        }

        public enum Confirm {
            case selected
            case alreadySelected
        }

        public enum Tx {
            case selected
            case alreadySelected
            case committed
            case rollbacked
        }

        public enum Message {
            case delivery(Delivery)
            case get(Get? = nil)
            case `return`(Return)

            public struct Delivery {
                public let exchange: String
                public let routingKey: String
                public let deliveryTag: UInt64
                public let properties: Properties
                public let redelivered: Bool
                public let body: ByteBuffer
            }

            public struct Get {
                public let message: Delivery
                public let messageCount: UInt32
            }

            public struct Return  {
                public let replyCode: UInt16
                public let replyText: String
                public let exchange: String
                public let routingKey: String
                public let properties: Properties
                public let body: ByteBuffer
            }
        }
    }

    public enum Connection {
        case connected(channelMax: UInt16)
        case blocked
        case closed
    }
}
