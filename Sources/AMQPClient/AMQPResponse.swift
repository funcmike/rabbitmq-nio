//===----------------------------------------------------------------------===//
//
// This source file is part of the RabbitMQNIO project
//
// Copyright (c) 2023 RabbitMQNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIOCore
import AMQPProtocol

public enum AMQPResponse: Sendable {
    case channel(Channel)
    case connection(Connection)

    public enum Channel: Sendable {
        case opened(Frame.ChannelID)
        case closed(Frame.ChannelID)
        case message(Message)
        case queue(Queue)
        case exchange(Exchange)
        case basic(Basic)
        case confirm(Confirm)
        case tx(Tx)
        case flowed(Flowed)


        public enum Queue: Sendable {
            case declared(Declared)
            case binded
            case purged(Purged)
            case deleted(Deleted)
            case unbinded

            public struct Declared: Sendable {
                public let queueName: String
                public let messageCount: UInt32
                public let consumerCount: UInt32
            }

            public struct Purged: Sendable {
                public let messageCount: UInt32
            }

            public struct Deleted: Sendable {
                public let messageCount: UInt32
            }
        }
        
        public enum Exchange: Sendable {
            case declared
            case deleted
            case binded
            case unbinded
        }

        public enum Basic: Sendable {
            case recovered
            case qosOk
            case consumeOk(ConsumeOk)
            case canceled
            case publishConfirm(PublishConfirm)
            case published(Published)

            public enum PublishConfirm: Sendable {
                case ack(deliveryTag: UInt64, multiple: Bool)
                case nack(deliveryTag: UInt64, multiple: Bool)
            }

            public struct ConsumeOk: Sendable {
                public let consumerTag: String
            }

            public struct Published: Sendable {
                public let deliveryTag: UInt64
            }
        }

        public enum Confirm: Sendable {
            case selected
        }

        public enum Tx: Sendable {
            case selected
            case committed
            case rollbacked
        }

        public struct Flowed: Sendable {
            public let active: Bool
        }

        public enum Message: Sendable {
            case delivery(Delivery)
            case get(Get? = nil)
            case `return`(Return)

            public struct Delivery: Sendable {
                public let exchange: String
                public let routingKey: String
                public let deliveryTag: UInt64
                public let properties: Properties
                public let redelivered: Bool
                public let body: ByteBuffer
            }

            public struct Get: Sendable {
                public let message: Delivery
                public let messageCount: UInt32
            }

            public struct Return: Sendable {
                public let replyCode: UInt16
                public let replyText: String
                public let exchange: String
                public let routingKey: String
                public let properties: Properties
                public let body: ByteBuffer
            }
        }
    }

    public enum Connection: Sendable {
        case connected(Connected)
        case closed

        public struct Connected: Sendable {
            public let channelMax: UInt16
            public let frameMax: UInt32
        }
    }
}
