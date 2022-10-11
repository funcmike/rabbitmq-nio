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

import NIOCore

public struct Properties: PayloadDecodable, PayloadEncodable  {
    enum Flag {
        static let contentType      = UInt16(0x8000)
        static let contentEncoding  = UInt16(0x4000)
        static let headers          = UInt16(0x2000)
        static let deliveryMode     = UInt16(0x1000)
        static let priority         = UInt16(0x0800)
        static let correlationID    = UInt16(0x0400)
        static let replyTo          = UInt16(0x0200)
        static let expiration       = UInt16(0x0100)
        static let messageID        = UInt16(0x0080)
        static let timestamp        = UInt16(0x0040)
        static let type             = UInt16(0x0020)
        static let userID           = UInt16(0x0010)
        static let appID            = UInt16(0x0008)
        static let reserved1        = UInt16(0x0004)
    }

    public let contentType: String?
    public let contentEncoding: String?
    public let headers: Table?
    public let deliveryMode: UInt8?
    public let priority: UInt8?
    public let correlationID: String?
    public let replyTo: String?
    public let expiration: String?
    public let messageID: String?
    public let timestamp: Int64?
    public let type: String?
    public let userID: String?
    public let appID: String?
    public let reserved1: String?

    public init(contentType: String? = nil, contentEncoding: String? = nil, headers: Table? = nil, deliveryMode: UInt8? = nil, priority: UInt8? = nil, correlationID: String? = nil, replyTo: String? = nil, expiration: String? = nil, messageID: String? = nil, timestamp: Int64? = nil, type: String? = nil, userID: String? = nil, appID: String? = nil, reserved1: String? = nil) {
        self.contentType = contentType
        self.contentEncoding = contentEncoding
        self.headers = headers
        self.deliveryMode = deliveryMode
        self.priority = priority
        self.correlationID = correlationID
        self.replyTo = replyTo
        self.expiration = expiration
        self.messageID = messageID
        self.timestamp = timestamp
        self.type = type
        self.userID = userID
        self.appID = appID
        self.reserved1 = reserved1
    }

    public static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let flags = buffer.readInteger(as: UInt16.self) else {
            throw ProtocolError.decode(type: UInt16.self, context: self)
        }

        var invalid = false || flags & 1 << 0 > 0
        invalid = invalid || flags & 2 << 0 > 0

        guard !invalid else {
            throw ProtocolError.invalid(value: flags, context: self)
        }

        var contentType: String? = nil
        
        if flags & Flag.contentType > 0 {
            (contentType, _) = try buffer.readShortString()
        }

        var contentEncoding: String? = nil

        if flags & Flag.contentEncoding > 0 {
            (contentEncoding, _) =  try buffer.readShortString()
        }

        var headers: Table? = nil

        if flags & Flag.headers > 0 {
            headers = try Table.decode(from: &buffer)
        }

        var deliveryMode: UInt8? = nil

        if flags & Flag.deliveryMode > 0 {
            guard let v = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            deliveryMode = v
        }

        var priority: UInt8? = nil

        if flags & Flag.priority > 0 {
            guard let v = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            priority = v
        }

        var correlationID: String? = nil
        
        if flags & Flag.correlationID > 0 {
            (correlationID, _) = try buffer.readShortString()
        }

        var replyTo: String? = nil
        
        if flags & Flag.replyTo > 0 {
            (replyTo, _) = try buffer.readShortString()
        }

        var expiration: String? = nil
        
        if flags & Flag.expiration > 0 {
            (expiration, _) = try buffer.readShortString()
        }

        var messageID: String? = nil
        
        if flags & Flag.messageID > 0 {
            (messageID, _) = try buffer.readShortString() 
        }

        var timestamp : Int64? = nil

        if flags & Flag.timestamp > 0 {
            guard let v = buffer.readInteger(as: Int64.self) else {
                throw ProtocolError.decode(type: Int64.self, context: self)
            }
            timestamp = v
        }

        var type: String? = nil
        
        if flags & Flag.type > 0 {
            (type, _) =  try buffer.readShortString()
        }

        var userID: String?  = nil
        
        if flags & Flag.userID > 0 {
            (userID, _ ) = try buffer.readShortString()
        }

        var appID: String? = nil
        
        if flags & Flag.appID > 0 {
            (appID, _) = try buffer.readShortString()
        }

        var reserved1: String? = nil
        
        if flags & Flag.reserved1 > 0 {
            (reserved1, _ ) = try buffer.readShortString()
        }

        return Properties(
                contentType: contentType,
                contentEncoding: contentEncoding,
                headers: headers,
                deliveryMode: deliveryMode,
                priority: priority,
                correlationID: correlationID,
                replyTo: replyTo,
                expiration: expiration,
                messageID: messageID,
                timestamp: timestamp,
                type: type,
                userID: userID,
                appID: appID,
                reserved1: reserved1)
    }

    public func encode(into buffer: inout NIOCore.ByteBuffer) throws {
        var flags = UInt16(0)

        if contentType != nil {
            flags = flags | Flag.contentType
        }

        if contentEncoding != nil {
            flags = flags | Flag.contentEncoding
        }

        if headers != nil {
            flags = flags | Flag.headers
        }

        if deliveryMode != nil {
            flags = flags | Flag.deliveryMode
        }

        if priority != nil {
            flags = flags | Flag.priority
        }

        if correlationID != nil {
            flags = flags | Flag.correlationID
        }

        if replyTo != nil {
            flags = flags | Flag.replyTo
        }

        if expiration != nil {
            flags = flags | Flag.expiration
        }

        if messageID != nil {
            flags = flags | Flag.messageID
        }

        if timestamp != nil {
            flags = flags | Flag.timestamp
        }

        if type != nil {
            flags = flags | Flag.type
        }

        if userID != nil {
            flags = flags | Flag.userID
        }

        if appID != nil {
            flags = flags | Flag.appID
        }

        if reserved1 != nil {
            flags = flags | Flag.reserved1
        }

        buffer.writeInteger(flags)

        if let contentType = contentType {
            try buffer.writeShortString(contentType)
        }

        if let contentEncoding = contentEncoding{
            try buffer.writeShortString(contentEncoding)
        }

        if let headers = headers {
            try headers.encode(into: &buffer)
        }

        if let deliveryMode = deliveryMode {
            buffer.writeInteger(deliveryMode)
        }

        if let priority = priority {
            buffer.writeInteger(priority)
        }

        if let correlationID = correlationID{
            try buffer.writeShortString(correlationID)
        }

        if let replyTo = replyTo {
            try buffer.writeShortString(replyTo)
        }

        if let expiration = expiration {
            try buffer.writeShortString(expiration)
        }

        if let messageID = messageID {
            try buffer.writeShortString(messageID)
        }

        if let timestamp = timestamp {
            buffer.writeInteger(timestamp)
        }

        if let type = type {
            try buffer.writeShortString(type)
        }

        if let userID = userID {
            try buffer.writeShortString(userID)
        }

        if let appID = appID {
            try buffer.writeShortString(appID)
        }

        if let reserved1 = reserved1 {
            try buffer.writeShortString(reserved1)
        }
    }
}
