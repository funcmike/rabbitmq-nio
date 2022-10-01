import NIOCore

public struct Properties: PayloadDecodable, PayloadEncodable  {
    public enum Flag {
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

    let contentType: String?
    let contentEncoding: String?
    let headers: Table?
    let deliveryMode: UInt8?
    let priority: UInt8?
    let correlationID: String?
    let replyTo: String?
    let expiration: String?
    let messageID: String?
    let timestamp : Int64?
    let type: String?
    let userID: String?
    let appID: String?
    let reserved1: String?

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let flags = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self)
        }

        var invalid = true || flags & 1 << 0 > 0
        invalid = invalid || flags & 2 << 0 > 0

        guard invalid else {
            throw DecodeError.unsupported(value: flags, message: "invalid property flags")
        }

        var contentType: String? = nil
        
        if flags & Flag.contentType > 0 {
           guard let (v, _) = readShortStr(from: &buffer) else {
                throw DecodeError.value(type: String.self)
           }
           contentType = v
        }

        var contentEncoding: String? = nil

        if flags & Flag.contentEncoding > 0 {
           guard let (v, _) = readShortStr(from: &buffer) else {
                throw DecodeError.value(type: String.self)
           }
           contentEncoding = v
        }

        var headers: Table? = nil

        if flags & Flag.headers > 0 {
            do {
               (headers, _) = try readDictionary(from: &buffer)
            }
            catch let error as DecodeError {
                throw DecodeError.value(type: Table.self, inner: error)
            }
        }

        var deliveryMode: UInt8? = nil

        if flags & Flag.deliveryMode > 0 {
            guard let v = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
           deliveryMode = v
        }

        var priority: UInt8? = nil

        if flags & Flag.priority > 0 {
            guard let v = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
           priority = v
        }

        var correlationID: String? = nil
        
        if flags & Flag.correlationID > 0 {
           guard let (v, _) = readShortStr(from: &buffer) else {
                throw DecodeError.value(type: String.self)
           }
           correlationID = v
        }

        var replyTo: String? = nil
        
        if flags & Flag.replyTo > 0 {
           guard let (v, _) = readShortStr(from: &buffer) else {
                throw DecodeError.value(type: String.self)
           }
           replyTo = v
        }

        var expiration: String? = nil
        
        if flags & Flag.expiration > 0 {
           guard let (v, _) = readShortStr(from: &buffer) else {
                throw DecodeError.value(type: String.self)
           }
           expiration = v
        }

        var messageID: String? = nil
        
        if flags & Flag.messageID > 0 {
           guard let (v, _) = readShortStr(from: &buffer) else {
                throw DecodeError.value(type: String.self)
           }
           messageID = v
        }

        var timestamp : Int64? = nil

        if flags & Flag.timestamp > 0 {
            guard let v = buffer.readInteger(as: Int64.self) else {
                throw DecodeError.value(type: Int64.self)
            }
            timestamp = v
        }

        var type: String? = nil
        
        if flags & Flag.type > 0 {
           guard let (v, _) = readShortStr(from: &buffer) else {
                throw DecodeError.value(type: String.self)
           }
           type = v
        }

        var userID: String?  = nil
        
        if flags & Flag.userID > 0 {
           guard let (v, _) = readShortStr(from: &buffer) else {
                throw DecodeError.value(type: String.self)
           }
           userID = v
        }

        var appID: String? = nil
        
        if flags & Flag.appID > 0 {
           guard let (v, _) = readShortStr(from: &buffer) else {
                throw DecodeError.value(type: String.self)
           }
           appID = v
        }

        var reserved1: String? = nil
        
        if flags & Flag.reserved1 > 0 {
           guard let (v, _) = readShortStr(from: &buffer) else {
                throw DecodeError.value(type: String.self)
           }
           reserved1 = v
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

    func encode(into buffer: inout NIOCore.ByteBuffer) throws {
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
           writeShortStr(value: contentType, into: &buffer)
        }

        if let contentEncoding = contentEncoding{
            writeShortStr(value: contentEncoding, into: &buffer)
        }

        if let headers = headers {
            do
            {
                try writeDictionary(values: headers, into: &buffer)
            } catch let error as EncodeError {
                throw EncodeError.value(type: Table.self, inner: error)
            }
        }

        if let deliveryMode = deliveryMode {
            buffer.writeInteger(deliveryMode)
        }

        if let priority = priority {
            buffer.writeInteger(priority)
        }

        if let correlationID = correlationID{
            writeShortStr(value: correlationID, into: &buffer)
        }

        if let replyTo = replyTo {
            writeShortStr(value: replyTo, into: &buffer)
        }

        if let expiration = expiration {
            writeShortStr(value: expiration, into: &buffer)
        }

        if let messageID = messageID {
            writeShortStr(value: messageID, into: &buffer)
        }

        if let timestamp = timestamp {
            buffer.writeInteger(timestamp)
        }

        if let type = type {
            writeShortStr(value: type, into: &buffer)
        }

        if let userID = userID {
            writeShortStr(value: userID, into: &buffer)
        }

        if let appID = appID {
            writeShortStr(value: appID, into: &buffer)
        }

        if let reserved1 = reserved1 {
            writeShortStr(value: reserved1, into: &buffer)
        }
    }
}