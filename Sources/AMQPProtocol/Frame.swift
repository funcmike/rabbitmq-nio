import NIOCore

public protocol PayloadDecodable {
    static func decode(from buffer: inout ByteBuffer) throws -> Self
}

public protocol PayloadEncodable {
    func encode(into buffer: inout ByteBuffer) throws
}

public enum Frame: PayloadDecodable, PayloadEncodable {
    public typealias ChannelID = UInt16

    case method(ChannelID, Method)
    case header(ChannelID, Header)
    case body(ChannelID, body: [UInt8])
    case heartbeat(ChannelID)

    var kind: Kind {
        switch self {
        case .method:
            return .method
        case .header:
            return .header
        case .body:
            return .body
        case .heartbeat:
            return .heartbeat
        }
    }

    public enum Kind {
        case method
        case header
        case body
        case heartbeat

        public init?(rawValue: UInt8)
        {
            switch rawValue {
            case 1:
                self = .method
            case 2:
                self = .header
            case 3:
                self = .body
            case 8:
                self = .heartbeat
            default:
                return nil
            }
        }

        var rawValue: UInt8 {
            switch self {
            case .method:
                return 1
            case .header:
                return 2
            case .body:
                return 3
            case .heartbeat:
                return 8
            }
        }
    }

    public static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let (type, channelId, size) = buffer.readMultipleIntegers(as: (UInt8, ChannelID, UInt32).self) else {
            throw ProtocolError.decode(type: (UInt8, ChannelID, UInt32).self, context: self)
        }

        guard let kind = Kind(rawValue: type) else {
            throw ProtocolError.unsupported(value: type, context: self)
        }

        let frame: Frame

        switch kind {
        case .method:
            frame = try Self.method(channelId, .decode(from: &buffer))
        case .header:
            frame = try Self.header(channelId, .decode(from: &buffer))
        case .body:
            guard let body = buffer.readBytes(length: Int(size)) else {
                throw ProtocolError.decode(type: [UInt8].self, context: self)
            }
            frame = Self.body(channelId, body: body)
        case .heartbeat:
            frame = Self.heartbeat(channelId)
        }

        guard let frameEnd = buffer.readInteger(as: UInt8.self) else {
            throw ProtocolError.decode(type: UInt8.self, context: self)
        }

        guard frameEnd == 206 else {
            throw ProtocolError.invalid(value: frameEnd, context: self)
        }

        return frame
    }

    public func encode(into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(self.kind.rawValue)
        
        switch self {
        case .method(let channelID, let method):
            buffer.writeInteger(channelID)

            let startIndex: Int = buffer.writerIndex
            buffer.writeInteger(UInt32(0)) // placeholder for size
                            
            try method.encode(into: &buffer)

            let size = UInt32(buffer.writerIndex - startIndex - 4)
            buffer.setInteger(size, at: startIndex)
        case .header(let channelID, let header):
            buffer.writeInteger(channelID)

            let startIndex: Int = buffer.writerIndex
            buffer.writeInteger(UInt32(0)) // placeholder for size
                            
            try header.encode(into: &buffer)

            let size = UInt32(buffer.writerIndex - startIndex - 4)
            buffer.setInteger(size, at: startIndex)
        case .body(let channelID, let body):
            let size = UInt32(body.count)
            buffer.writeMultipleIntegers(channelID, size)
        case .heartbeat(let channelID):
            let size = UInt32(0)
            buffer.writeMultipleIntegers(channelID, size)
        }

        buffer.writeInteger(UInt8(206)) // endMarker
    }

    public struct Header: PayloadDecodable, PayloadEncodable {
        public let classID: UInt16
        public let weight: UInt16
        public let bodySize: UInt64
        public let properties: Properties

        public init(classID: UInt16, weight: UInt16, bodySize: UInt64, properties: Properties) {
            self.classID = classID
            self.weight = weight
            self.bodySize = bodySize
            self.properties = properties
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let (classID, weight, bodySize) = buffer.readMultipleIntegers(as: (UInt16, UInt16, UInt64).self) else {
                throw ProtocolError.decode(type: (UInt16, UInt16, UInt64).self, context: self)
            }
            
            let properties = try Properties.decode(from: &buffer)

            return Header(classID: classID, weight: weight, bodySize: bodySize, properties: properties)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeMultipleIntegers(classID, weight, bodySize)
            try properties.encode(into: &buffer)
        }
    }
}

public enum Method: PayloadDecodable, PayloadEncodable {
    case connection(Connection)
    case channel(Channel)
    case exchange(Exchange)
    case queue(Queue)
    case basic(Basic)
    case confirm(Confirm)
    case tx(Tx)

    var kind: Kind {
        switch self {
        case .connection:
            return .connection
        case .channel:
            return .channel
        case .exchange:
            return .exchange
        case .queue:
            return .queue
        case .basic:
            return .basic
        case .confirm:
            return .confirm
        case .tx:
            return .tx
        }
    }

    public enum Kind {
        case connection
        case channel
        case exchange
        case queue
        case basic
        case confirm
        case tx

        public init?(rawValue: UInt16)
        {
            switch rawValue {
            case 10:
                self = .connection
            case 20:
                self = .channel
            case 40:
                self = .exchange
            case 50:
                self = .queue
            case 60:
                self = .basic
            case 85:
                self = .confirm
            case 90:
                self = .tx
            default:
                return nil
            }
        }

        var rawValue: UInt16 {
            switch self {
            case .connection:
                return 10
            case .channel:
                return 20
            case .exchange:
                return 40
            case .queue:
                return 50
            case .basic:
                return 60
            case .confirm:
                return 85
            case .tx:
                return 90
            }
        }
    }

    public static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let classID = buffer.readInteger(as: UInt16.self) else {
            throw ProtocolError.decode(type: UInt16.self, context: self)
        }

        guard let kind = Kind(rawValue: classID) else {
            throw ProtocolError.unsupported(value: classID, context: self)
        }

        switch kind {
            case .connection:
                return try .connection(.decode(from: &buffer))
            case .channel:
                return try .channel(.decode(from: &buffer))
            case .exchange:
                return try .exchange(.decode(from: &buffer))
            case .queue:
                return try .queue(.decode(from: &buffer))
            case .basic:
                return try .basic(.decode(from: &buffer))
            case .confirm:
                return try .confirm(.decode(from: &buffer))
            case .tx:
                return try .tx(.decode(from: &buffer))
        }
    }

    public func encode(into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(self.kind.rawValue)

        switch self {
        case .connection(let connection):
            try connection.encode(into: &buffer)
        case .channel(let channel):
            try channel.encode(into: &buffer)
        case .exchange(let exchange):
            try exchange.encode(into: &buffer)
        case .queue(let queue): 
            try queue.encode(into: &buffer)
        case .basic(let basic):
            try basic.encode(into: &buffer)
        case .confirm(let confirm):
            try confirm.encode(into: &buffer)
        case .tx(let tx): 
            try tx.encode(into: &buffer)
        }
    }
}

public enum Connection: PayloadDecodable, PayloadEncodable {
    case start(Start)
    case startOk(StartOk)
    case secure(challenge: String)
    case secureOk(response: String)
    case tune(channelMax: UInt16 = 0, frameMax: UInt32 = 131072, heartbeat: UInt16 = 0)
    case tuneOk(channelMax: UInt16 = 0, frameMax: UInt32 = 131072, heartbeat: UInt16 = 60)
    case open(Open)
    case openOk(reserved1: String)
    case close(Close)
    case closeOk
    case blocked(reason: String)
    case unblocked


    var kind: Kind {
        switch self {
        case .start:
            return .start
        case .startOk:
            return .startOk
        case .secure:
            return .secure
        case .secureOk:
            return .secureOk
        case .tune:
            return .tune
        case .tuneOk:
            return .tuneOk
        case .open:
            return .open
        case .openOk:
            return .openOk
        case .close: 
            return .close
        case .closeOk: 
            return .closeOk
        case .blocked: 
            return .blocked
        case .unblocked: 
            return .unblocked
        }
    }

    public enum Kind {
        case start
        case startOk
        case secure
        case secureOk
        case tune
        case tuneOk
        case open
        case openOk
        case close
        case closeOk
        case blocked
        case unblocked

        public init?(rawValue: UInt16)
        {
            switch rawValue {
            case 10:
                self = .start
            case 11:
                self = .startOk
            case 20:
                self = .secure
            case 21:
                self = .secureOk
            case 30:
                self = .tune
            case 31:
                self = .tuneOk
            case 40:
                self = .open
            case 41:
                self = .openOk
            case 50:
                self = .close
            case 51:
                self = .closeOk
            case 60:
                self = .blocked
            case 61:
                self = .unblocked
            default:
                return nil
            }
        }

        var rawValue: UInt16 {
            switch self {
            case .start:
                return 10
            case .startOk:
                return 11
            case .secure:
                return 20
            case .secureOk:
                return 21
            case .tune:
                return 30
            case .tuneOk:
                return 31
            case .open:
                return 40
            case .openOk:
                return 41
            case .close: 
                return 50
            case .closeOk: 
                return 51
            case .blocked: 
                return 60
            case .unblocked: 
                return 61
            }
        }
    }

    public static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let methodID = buffer.readInteger(as: UInt16.self) else {
            throw ProtocolError.decode(type: UInt16.self, context: self)
        }

        guard let kind = Kind(rawValue: methodID) else {
            throw ProtocolError.unsupported(value: methodID, context: self)
        }

        switch kind {
        case .start:
            return try .start(.decode(from: &buffer))
        case .startOk:
            return try .startOk(.decode(from: &buffer))
        case .secure:
            let (challenge, _) = try buffer.readLongString()
            return .secure(challenge: challenge)
        case .secureOk:
            let (response, _) = try buffer.readLongString()
            return .secureOk(response: response)            
        case .tune:
            guard let (channelMax, frameMax, heartbeat) = buffer.readMultipleIntegers(as: (UInt16, UInt32, UInt16).self) else {
                throw ProtocolError.decode(type: (UInt16, UInt32, UInt16).self, context: self)
            }
            return .tune(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat)
        case .tuneOk:
            guard let (channelMax, frameMax, heartbeat) = buffer.readMultipleIntegers(as: (UInt16, UInt32, UInt16).self) else {
                throw ProtocolError.decode(type: (UInt16, UInt32, UInt16).self, context: self)
            }
            return .tuneOk(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat)
        case .open:
            return try .open(.decode(from: &buffer))
        case .openOk:
            let (reserved1, _) = try buffer.readShortString()
            return .openOk(reserved1: reserved1)
        case .close:
            return try .close(.decode(from: &buffer))
        case .closeOk:
            return .closeOk
        case .blocked:
            let (reason, _) = try buffer.readShortString()
            return .blocked(reason: reason)
        case .unblocked:
            return .unblocked
        }
    }

    public func encode(into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(self.kind.rawValue)

        switch self {
        case .start(let connectionStart):
            try connectionStart.encode(into: &buffer)
        case .startOk(let connectionStartOk):
            try connectionStartOk.encode(into: &buffer)
        case .secure(let challenge):
            try buffer.writeLongString(challenge)
        case .secureOk(let response):
            try buffer.writeLongString(response)
        case .tune(let channelMax, let frameMax, let heartbeat):
            buffer.writeMultipleIntegers(channelMax, frameMax, heartbeat)
        case .tuneOk(let channelMax, let frameMax, let heartbeat):
            buffer.writeMultipleIntegers(channelMax, frameMax, heartbeat)
        case .open(let open): 
            try open.encode(into: &buffer)
        case .openOk(let reserved1): 
            try buffer.writeShortString(reserved1)
        case .close(let close):
            try close.encode(into: &buffer)
        case .closeOk: 
            break
        case .blocked(let reason):
            try buffer.writeShortString(reason)
        case .unblocked:
            break
        }
    }

    public struct Start: PayloadDecodable {
        public let versionMajor: UInt8
        public let versionMinor: UInt8
        public let serverProperties: Table
        public let mechanisms: String
        public let locales: String

        public init(versionMajor: UInt8 = 0, versionMinor: UInt8 = 9, serverProperties: Table = [
                "capabilities": .table([
                    "publisher_confirms":           .bool(true),
                    "exchange_exchange_bindings":   .bool(true),
                    "basic.nack":                   .bool(true),
                    "per_consumer_qos":             .bool(true),
                    "authentication_failure_close": .bool(true),
                    "consumer_cancel_notify":       .bool(true),
                    "connection.blocked":           .bool(true),
                ])
            ], mechanisms: String = "AMQPLAIN PLAIN", locales: String = "en_US")
        {
            self.versionMajor = versionMajor
            self.versionMinor = versionMinor
            self.serverProperties = serverProperties
            self.mechanisms = mechanisms
            self.locales = locales
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let (versionMajor, versionMinor)  = buffer.readMultipleIntegers(as: (UInt8, UInt8).self) else {
                throw ProtocolError.decode(type: (UInt8, UInt8).self, context: self)
            }

            let serverProperties = try Table.decode(from: &buffer)
            let (mechanisms, _) = try buffer.readLongString()
            let (locales, _) = try buffer.readLongString()

            return Start(versionMajor: versionMajor, versionMinor: versionMinor, serverProperties: serverProperties, mechanisms: mechanisms, locales: locales)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeMultipleIntegers(versionMajor, versionMinor)
            try serverProperties.encode(into: &buffer)
            try buffer.writeLongString(mechanisms)
            try buffer.writeLongString(locales)
        }
    }

    public struct StartOk: PayloadDecodable, PayloadEncodable {
        public let clientProperties: Table
        public let mechanism: String
        public let response: String
        public let locale: String

        public init(clientProperties: Table, mechanism: String, response: String, locale: String) {
            self.clientProperties = clientProperties
            self.mechanism = mechanism
            self.response = response
            self.locale = locale
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let clientProperties = try Table.decode(from: &buffer)
            let (mechanism, _) =  try buffer.readShortString()
            let (response, _) = try buffer.readLongString()
            let (locale, _) = try buffer.readShortString()

            return StartOk(clientProperties: clientProperties, mechanism: mechanism, response: response, locale: locale)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            try clientProperties.encode(into: &buffer)
            try buffer.writeShortString(mechanism)
            try buffer.writeLongString(response)
            try buffer.writeShortString(locale)
        }
    }

    public struct Open: PayloadDecodable, PayloadEncodable {
        public let vhost: String
        public let reserved1: String
        public let reserved2: Bool

        public init(vhost: String = "/", reserved1: String = "", reserved2: Bool = false)
        {
            self.vhost = vhost
            self.reserved1 = reserved1
            self.reserved2 = reserved2
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let (vhost, _) = try buffer.readShortString()
            let (reserved1, _) = try buffer.readShortString()

            guard let reserved2 = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }

            return Open(vhost: vhost, reserved1: reserved1, reserved2: reserved2 > 0)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            try buffer.writeShortString(vhost)
            try buffer.writeShortString(reserved1)
            buffer.writeInteger(reserved2 ? UInt8(1) : UInt8(0))
        }
    }


    public struct Close: PayloadDecodable, PayloadEncodable {
        public let replyCode: UInt16
        public let replyText: String
        public let failingClassID: UInt16
        public let failingMethodID: UInt16

        public init(replyCode: UInt16, replyText: String, failingClassID: UInt16, failingMethodID: UInt16) {
            self.replyCode = replyCode
            self.replyText = replyText
            self.failingClassID = failingClassID
            self.failingMethodID = failingMethodID
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let replyCode = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (replyText, _) = try buffer.readShortString()

            guard let (failingClassID, failingMethodID) = buffer.readMultipleIntegers(as: (UInt16, UInt16).self) else {
                throw ProtocolError.decode(type: (UInt16, UInt16).self, context: self)
            }

            return Close(replyCode: replyCode, replyText: replyText, failingClassID: failingClassID, failingMethodID: failingMethodID)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(replyCode)
            try buffer.writeShortString(replyText)
            buffer.writeMultipleIntegers(failingClassID, failingMethodID)
        }
    }
}

public enum Channel: PayloadDecodable, PayloadEncodable {
    case open(reserved1: String)
    case openOk(reserved1: String)
    case flow(active: Bool)
    case flowOk(active: Bool)
    case close(Close)
    case closeOk

    var kind: Kind {
        switch self {
        case .open:
            return .open
        case .openOk:
            return .openOk
        case .flow:
            return .flow
        case .flowOk:
            return .flowOk
        case .close:
            return .close
        case .closeOk:
            return .closeOk
        }
    }

    public enum Kind {
        case open
        case openOk
        case flow
        case flowOk
        case close
        case closeOk

        public init?(rawValue: UInt16)
        {
            switch rawValue {
            case 10:
                self = .open
            case 11:
                self = .openOk
            case 20:
                self = .flow
            case 21:
                self = .flowOk
            case 40:
                self = .close
            case 41:
                self = .closeOk
            default:
                return nil
            }
        }

        var rawValue: UInt16 {
            switch self {
            case .open:
                return 10
            case .openOk:
                return 11
            case .flow:
                return 20
            case .flowOk:
                return 21
            case .close:
                return 40
            case .closeOk:
                return 41
            }
        }
    }

    public static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let methodID = buffer.readInteger(as: UInt16.self) else {
            throw ProtocolError.decode(type: UInt16.self, context: self)
        }

        guard let kind = Kind(rawValue: methodID) else {
            throw ProtocolError.unsupported(value: methodID, context: self)
        }

        switch kind {
        case .open:
           let (reserved1, _) = try buffer.readShortString()
            return .open(reserved1: reserved1)
        case .openOk:
           let (reserved1, _) = try buffer.readLongString()
            return .open(reserved1: reserved1)                
        case .flow:
            guard let active = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            return .flow(active: active > 0)               
        case .flowOk:
            guard let active = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            return .flowOk(active: active > 0)       
        case .close: 
            return try .close(.decode(from: &buffer))
        case .closeOk:
            return .closeOk
        }
    }

    public func encode(into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(self.kind.rawValue)

        switch self {
        case .open(let reserved1):
            try buffer.writeShortString(reserved1) 
        case .openOk(let reserved1):
            try buffer.writeLongString(reserved1) 
        case .flow(let active):
            buffer.writeInteger(active ? UInt8(1) : UInt8(0))
        case .flowOk(let active):
            buffer.writeInteger(active ? UInt8(1) : UInt8(0))
        case .close(let close):
            try close.encode(into: &buffer)
        case .closeOk:
            break
        }
    }

    public struct Close: PayloadDecodable, PayloadEncodable {
        public let replyCode: UInt16
        public let replyText: String
        public let classID: UInt16
        public let methodID: UInt16

        public init(replyCode: UInt16, replyText: String, classID: UInt16, methodID: UInt16) {
            self.replyCode = replyCode
            self.replyText = replyText
            self.classID = classID
            self.methodID = methodID
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let replyCode = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (replyText, _) = try buffer.readShortString()

            guard let (classID, methodID) = buffer.readMultipleIntegers(as: (UInt16, UInt16).self) else {
                throw ProtocolError.decode(type: (UInt16, UInt16).self, context: self)
            }

            return Close(replyCode: replyCode, replyText: replyText, classID: classID, methodID: methodID)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(replyCode)
            try buffer.writeShortString(replyText)
            buffer.writeMultipleIntegers(classID, methodID)
        }
    }
}

public enum Exchange: PayloadDecodable, PayloadEncodable{
    case declare(Declare)
    case declareOk
    case delete(Delete)
    case deleteOk
    case bind(Bind)
    case bindOk
    case unbind(Unbind)
    case unbindOk

    var kind: Kind {
        switch self {
        case .declare:
            return .declare
        case .declareOk:
            return .declareOk
        case .delete:
            return .delete
        case .deleteOk:
            return .deleteOk
        case .bind:
            return .bind
        case .bindOk:
            return .bindOk
        case .unbind:
            return .unbind
        case .unbindOk:
            return .unbindOk
        }
    }

    public enum Kind {
        case declare
        case declareOk
        case delete
        case deleteOk
        case bind
        case bindOk
        case unbind
        case unbindOk

        public init?(rawValue: UInt16)
        {
            switch rawValue {
            case 10:
                self = .declare
            case 11:
                self = .declareOk
            case 20:
                self = .delete
            case 21:
                self = .deleteOk
            case 30:
                self = .bind
            case 31:
                self = .bindOk
            case 40:
                self = .unbind
            case 51:
                self = .unbindOk
            default:
                return nil
            }
        }

        var rawValue: UInt16 {
            switch self {
            case .declare:
                return 10
            case .declareOk:
                return 11
            case .delete:
                return 20
            case .deleteOk:
                return 21
            case .bind:
                return 30
            case .bindOk:
                return 31
            case .unbind:
                return 40
            case .unbindOk:
                return 51
            }
        }
    }

    public static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let methodID = buffer.readInteger(as: UInt16.self) else {
            throw ProtocolError.decode(type: UInt16.self, context: self)
        }

        guard let kind = Kind(rawValue: methodID) else {
            throw ProtocolError.unsupported(value: methodID, context: self)
        }

        switch kind {
        case .declare:
            return try .declare(.decode(from: &buffer))
        case .declareOk:
            return .declareOk
        case .delete:
            return try .delete(.decode(from: &buffer))
        case .deleteOk:
            return .deleteOk
        case .bind:
            return try .bind(.decode(from: &buffer))
        case .bindOk:
            return .bindOk
        case .unbind:
            return try .unbind(.decode(from: &buffer))
        case .unbindOk:
            return .unbindOk
        }
    }

    public func encode(into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(self.kind.rawValue)

        switch self {
        case .declare(let declare):
            try declare.encode(into: &buffer)
        case .declareOk:
            break
        case .delete(let deleteOk):
            try deleteOk.encode(into: &buffer)
        case .deleteOk:
            break
        case .bind(let bind):
            try bind.encode(into: &buffer)
        case .bindOk:
            break
        case .unbind(let unbind):
            try unbind.encode(into: &buffer)
        case .unbindOk:
            break
        }
    }

    public struct Declare: PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let exchangeName: String
        public let exchangeType: String
        public let passive: Bool
        public let durable: Bool
        public let autoDelete: Bool
        public let `internal`: Bool
        public let noWait: Bool
        public let arguments: Table

        public init(reserved1: UInt16, exchangeName: String, exchangeType: String, passive: Bool, durable: Bool, autoDelete: Bool, `internal`: Bool, noWait: Bool, arguments: Table) {
            self.reserved1 = reserved1
            self.exchangeName = exchangeName
            self.exchangeType = exchangeType
            self.passive = passive
            self.durable = durable
            self.autoDelete = autoDelete
            self.`internal` = `internal`
            self.noWait = noWait
            self.arguments = arguments
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (exchangeName, _) = try buffer.readShortString()
            let (exchangeType, _) = try buffer.readShortString()

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            
            let passive = bits.isBitSet(pos: 0)
            let durable = bits.isBitSet(pos: 1)
            let autoDelete = bits.isBitSet(pos: 2)
            let `internal` = bits.isBitSet(pos: 3)
            let noWait = bits.isBitSet(pos: 4)
            let arguments = try Table.decode(from: &buffer)

            return Declare(reserved1: reserved1, exchangeName: exchangeName, exchangeType: exchangeType, passive: passive, durable: durable, autoDelete: autoDelete, internal: `internal`, noWait: noWait, arguments: arguments)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(exchangeName)
            try buffer.writeShortString(exchangeType)

            var bits = UInt8(0)
            
            if passive {
                bits = bits | (1 << 0)
            }

            if durable {
                bits = bits | (1 << 1)
            }

            if autoDelete {
                bits = bits | (1 << 2)
            }

            if `internal` {
                bits = bits | (1 << 3)
            }

            if `noWait` {
                bits = bits | (1 << 4)
            }

            buffer.writeInteger(bits)
            try arguments.encode(into: &buffer)
        }
    }

    public struct Delete: PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let exchangeName: String
        public let ifUnused : Bool
        public let noWait: Bool

        public init(reserved1: UInt16, exchangeName: String, ifUnused: Bool, noWait: Bool) {
            self.reserved1 = reserved1
            self.exchangeName = exchangeName
            self.ifUnused = ifUnused
            self.noWait = noWait
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (exchangeName, _) = try buffer.readShortString()

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            
            let ifUnused = bits.isBitSet(pos: 0)
            let noWait = bits.isBitSet(pos: 1)

            return Delete(reserved1: reserved1, exchangeName: exchangeName, ifUnused: ifUnused, noWait: noWait)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(exchangeName)

            var bits = UInt8(0)
            
            if ifUnused {
                bits = bits | (1 << 0)
            }

            if noWait {
                bits = bits | (1 << 1)
            }

            buffer.writeInteger(bits)
        }
    }

    public struct Bind: PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let destination: String
        public let source: String
        public let routingKey: String
        public let noWait: Bool
        public let arguments: Table

        public init(reserved1: UInt16, destination: String, source: String, routingKey: String, noWait: Bool, arguments: Table) {
            self.reserved1 = reserved1
            self.destination = destination
            self.source = source
            self.routingKey = routingKey
            self.noWait = noWait
            self.arguments = arguments
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (destination, _) = try buffer.readShortString()
            let (source, _) = try buffer.readShortString()
            let (routingKey, _) = try buffer.readShortString()

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            
            let noWait = bits.isBitSet(pos: 0)
            let arguments = try Table.decode(from: &buffer)

            return Bind(reserved1: reserved1, destination: destination, source: source, routingKey: routingKey, noWait: noWait, arguments: arguments)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(destination)
            try buffer.writeShortString(source)
            try buffer.writeShortString(routingKey)
            buffer.writeInteger(noWait ? UInt8(1) : UInt8(0))
            try arguments.encode(into: &buffer)
        }
    }

    public struct Unbind: PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let destination: String
        public let source: String
        public let routingKey: String
        public let noWait: Bool
        public let arguments: Table

        public init(reserved1: UInt16, destination: String, source: String, routingKey: String, noWait: Bool, arguments: Table) {
            self.reserved1 = reserved1
            self.destination = destination
            self.source = source
            self.routingKey = routingKey
            self.noWait = noWait
            self.arguments = arguments
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (destination, _) = try buffer.readShortString()
            let (source, _) = try buffer.readShortString()
            let (routingKey, _) = try buffer.readShortString()

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            
            let noWait = bits.isBitSet(pos: 0)
            let arguments = try Table.decode(from: &buffer)

            return Unbind(reserved1: reserved1, destination: destination, source: source, routingKey: routingKey, noWait: noWait, arguments: arguments)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(destination)
            try buffer.writeShortString(source)
            try buffer.writeShortString(routingKey)
            buffer.writeInteger(noWait ? UInt8(1) : UInt8(0))
            try arguments.encode(into: &buffer)
        }
    }
}

public enum Queue: PayloadDecodable, PayloadEncodable{
    case declare(Declare)
    case declareOk(DeclareOk)
    case bind(Bind)
    case bindOk
    case purge(Purge)
    case purgeOk(messageCount: UInt32)
    case delete(Delete)
    case deleteOk(messageCount: UInt32)
    case unbind(Unbind)
    case unbindOk

    var kind: Kind {
        switch self {
        case .declare:
            return .declare
        case .declareOk:
            return .declareOk
        case .bind:
            return .bind
        case .bindOk:
            return .bindOk
        case .purge:
            return .purge
        case .purgeOk:
            return .purgeOk
        case .delete:
            return .delete
        case .deleteOk:
            return .deleteOk
        case .unbind:
            return .unbind
        case .unbindOk:
            return .unbindOk
        }
    }

    public enum Kind {
        case declare
        case declareOk
        case bind
        case bindOk
        case purge
        case purgeOk
        case delete
        case deleteOk
        case unbind
        case unbindOk

        public init?(rawValue: UInt16)
        {
            switch rawValue {
            case 10:
                self = .declare
            case 11:
                self = .declareOk
            case 20:
                self = .bind
            case 21:
                self = .bindOk
            case 30:
                self = .purge
            case 31:
                self = .purgeOk
            case 40:
                self = .delete
            case 41:
                self = .deleteOk
            case 50:
                self = .unbind
            case 51:
                self = .unbindOk
            default:
                return nil
            }
        }

        var rawValue: UInt16 {
            switch self {
            case .declare:
                return 10
            case .declareOk:
                return 11
            case .bind:
                return 20
            case .bindOk:
                return 21
            case .purge:
                return 30
            case .purgeOk:
                return 31
            case .delete:
                return 40
            case .deleteOk:
                return 41
            case .unbind:
                return 50
            case .unbindOk:
                return 51
            }
        }
    }

    public static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let methodID = buffer.readInteger(as: UInt16.self) else {
            throw ProtocolError.decode(type: UInt16.self, context: self)
        }

        guard let kind = Kind(rawValue: methodID) else {
            throw ProtocolError.unsupported(value: methodID, context: self)
        }

        switch kind {
        case .declare:
            return try .declare(.decode(from: &buffer))
        case .declareOk:
            return try .declareOk(.decode(from: &buffer))
        case .bind:
            return try .bind(.decode(from: &buffer))
        case .bindOk:
            return .bindOk
        case .purge:
            return try .purge(.decode(from: &buffer))
        case .purgeOk:
            guard let messageCount = buffer.readInteger(as: UInt32.self) else {
                throw ProtocolError.decode(type: UInt32.self, context: self)
            }            
            return .purgeOk(messageCount: messageCount)            
        case .delete:
            return try .delete(.decode(from: &buffer))
        case .deleteOk:
            guard let messageCount = buffer.readInteger(as: UInt32.self) else {
                throw ProtocolError.decode(type: UInt32.self, context: self)
            }            
            return .deleteOk(messageCount: messageCount)
        case .unbind:
            return try .unbind(.decode(from: &buffer))
        case .unbindOk:
            return .unbindOk
        }
    }

    public func encode(into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(self.kind.rawValue)
        
        switch self  {       
        case .bind(let bind):
            try bind.encode(into: &buffer)
        case .bindOk:
            break
        case .declare(let declare):
            try declare.encode(into: &buffer)
        case .declareOk(let declareOk):
            try declareOk.encode(into: &buffer)
        case .purge(let purge):
            try purge.encode(into: &buffer)
        case .purgeOk(let messageCount):
            buffer.writeInteger(messageCount)
        case .delete(let delete):
            try delete.encode(into: &buffer)
        case .deleteOk(let messageCount):
            buffer.writeInteger(messageCount)
        case .unbind(let unbind):
            try unbind.encode(into: &buffer)
        case .unbindOk:
            break
        }
    }

    public struct Declare: PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let queueName : String
        public let passive: Bool
        public let durable: Bool
        public let exclusive: Bool
        public let autoDelete: Bool
        public let noWait: Bool
        public let arguments: Table

        public init(reserved1: UInt16, queueName: String, passive: Bool, durable: Bool, exclusive: Bool, autoDelete: Bool, noWait: Bool, arguments: Table) {
            self.reserved1 = reserved1
            self.queueName = queueName
            self.passive = passive
            self.durable = durable
            self.exclusive = exclusive
            self.autoDelete = autoDelete
            self.noWait = noWait
            self.arguments = arguments
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (queueName, _) = try buffer.readShortString()

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            
            let passive = bits.isBitSet(pos: 0)
            let durable = bits.isBitSet(pos: 1)
            let exclusive = bits.isBitSet(pos: 2)
            let autoDelete = bits.isBitSet(pos: 3)
            let noWait = bits.isBitSet(pos: 4)
            let arguments = try Table.decode(from: &buffer)

            return Declare(reserved1: reserved1, queueName: queueName, passive: passive, durable: durable, exclusive: exclusive, autoDelete: autoDelete, noWait: noWait, arguments: arguments)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(queueName)

            var bits = UInt8(0)
            
            if passive {
                bits = bits | (1 << 0)
            }

            if durable {
                bits = bits | (1 << 1)
            }

            if exclusive {
                bits = bits | (1 << 2)
            }

            if autoDelete {
                bits = bits | (1 << 3)
            }

            if `noWait` {
                bits = bits | (1 << 4)
            }

            buffer.writeInteger(bits)
            try arguments.encode(into: &buffer)
        }
    }

    public struct DeclareOk: PayloadDecodable, PayloadEncodable {
        public let queueName : String
        public let messageCount: UInt32
        public let consumerCount: UInt32

        public init(queueName: String, messageCount: UInt32, consumerCount: UInt32) {
            self.queueName = queueName
            self.messageCount = messageCount
            self.consumerCount = consumerCount
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let (queueName, _) = try buffer.readShortString()

            guard let (messageCount, consumerCount) = buffer.readMultipleIntegers(as: (UInt32, UInt32).self) else {
                throw ProtocolError.decode(type: (UInt32, UInt32).self, context: self)
            }

            return DeclareOk(queueName: queueName, messageCount: messageCount, consumerCount: consumerCount)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            try buffer.writeShortString(queueName)
            buffer.writeMultipleIntegers(messageCount, consumerCount)
        }
    }

    public struct Bind: PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let queueName : String
        public let exchangeName: String
        public let routingKey : String
        public let noWait: Bool
        public let arguments: Table

        internal init(reserved1: UInt16, queueName: String, exchangeName: String, routingKey: String, noWait: Bool, arguments: Table) {
            self.reserved1 = reserved1
            self.queueName = queueName
            self.exchangeName = exchangeName
            self.routingKey = routingKey
            self.noWait = noWait
            self.arguments = arguments
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (queueName, _) = try buffer.readShortString()
            let (exchangeName, _) = try buffer.readShortString()
            let (routingKey, _) = try buffer.readShortString()

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }

            let noWait = bits.isBitSet(pos: 0)
            let arguments = try Table.decode(from: &buffer)

            return Bind(reserved1: reserved1, queueName: queueName, exchangeName: exchangeName, routingKey: routingKey, noWait: noWait, arguments: arguments)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(queueName)
            try buffer.writeShortString(exchangeName)
            try buffer.writeShortString(routingKey)
            buffer.writeInteger(noWait ? UInt8(1) : UInt8(0))
            try arguments.encode(into: &buffer)
        }
    }

    public struct Purge: PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let queueName : String
        public let noWait: Bool

        public init(reserved1: UInt16, queueName: String, noWait: Bool) {
            self.reserved1 = reserved1
            self.queueName = queueName
            self.noWait = noWait
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (queueName, _) = try buffer.readShortString()

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }

            let noWait = bits.isBitSet(pos: 0)

            return Purge(reserved1: reserved1, queueName: queueName, noWait: noWait)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(queueName)
            buffer.writeInteger(noWait ? UInt8(1) : UInt8(0))
        }
    }


    public struct Delete: PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let queueName : String
        public let ifUnused: Bool
        public let ifEmpty : Bool
        public let noWait: Bool

        public init(reserved1: UInt16, queueName: String, ifUnused: Bool, ifEmpty: Bool, noWait: Bool) {
            self.reserved1 = reserved1
            self.queueName = queueName
            self.ifUnused = ifUnused
            self.ifEmpty = ifEmpty
            self.noWait = noWait
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (queueName, _) = try buffer.readShortString()

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }

            let ifUnused = bits.isBitSet(pos: 0)
            let ifEmpty = bits.isBitSet(pos: 1)
            let noWait = bits.isBitSet(pos: 2)

            return Delete(reserved1: reserved1, queueName: queueName, ifUnused: ifUnused, ifEmpty: ifEmpty, noWait: noWait)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(queueName)

            var bits = UInt8(0)

            if ifUnused {
                bits = bits | (1 << 0)
            }

            if ifEmpty {
                bits = bits | (1 << 1)
            }

            if noWait {
                bits = bits | (1 << 2)
            }

            buffer.writeInteger(bits)
        }
    }

    public struct Unbind: PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let queueName : String
        public let exchangeName: String
        public let routingKey : String
        public let arguments: Table

        public init(reserved1: UInt16, queueName: String, exchangeName: String, routingKey: String, arguments: Table) {
            self.reserved1 = reserved1
            self.queueName = queueName
            self.exchangeName = exchangeName
            self.routingKey = routingKey
            self.arguments = arguments
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (queueName, _) = try buffer.readShortString()
            let (exchangeName, _) = try buffer.readShortString()
            let (routingKey, _) = try buffer.readShortString()
            let arguments = try Table.decode(from: &buffer)

            return Unbind(reserved1: reserved1, queueName: queueName, exchangeName: exchangeName, routingKey: routingKey, arguments: arguments)
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(queueName)
            try buffer.writeShortString(exchangeName)
            try buffer.writeShortString(routingKey)
            try arguments.encode(into: &buffer)
        }
    }
}

public enum Basic: PayloadDecodable, PayloadEncodable {
    case qos(prefetchSize: UInt32, prefetchCount: UInt16, global: Bool)
    case qosOk
    case consume(Consume)
    case consumeOk(consumerTag: String)
    case cancel(Cancel)
    case cancelOk(consumerTag: String)
    case publish(Publish)
    case `return`(Return)
    case deliver(Deliver)
    case get(Get)
    case getOk(GetOk)
    case getEmpty(reserved1: String)
    case ack(deliveryTag: UInt64, multiple: Bool)
    case reject(deliveryTag: UInt64, requeue: Bool)
    case recoverAsync(requeue: Bool)
    case recover(requeue: Bool)
    case recoverOk
    case nack(Nack)

    var kind: Kind {
        switch self {
        case .qos:
            return .qos
        case .qosOk:
            return .qosOk
        case .consume:
            return .consume
        case .consumeOk:
            return .consumeOk
        case .cancel:
            return .cancel
        case .cancelOk:
            return .cancelOk
        case .publish:
            return .publish
        case .`return`:
            return .return
        case .deliver:
            return .deliver
        case .get:
            return .get
        case .getOk:
            return .getOk
        case .getEmpty:
            return .getEmpty
        case .ack:
            return .ack
        case .reject:
            return .reject
        case .recoverAsync:
            return .recoverAsync
        case .recover:
            return .recover
        case .recoverOk:
            return .recoverOk
        case .nack:
            return .nack
        }
    }

    public enum Kind {
        case qos
        case qosOk
        case consume
        case consumeOk
        case cancel
        case cancelOk
        case publish
        case `return`
        case deliver
        case get
        case getOk
        case getEmpty
        case ack
        case reject
        case recoverAsync
        case recover
        case recoverOk
        case nack

        public init?(rawValue: UInt16) {
            switch rawValue {
            case 10:
                self = .qos
            case 11:
                self = .qosOk
            case 20:
                self = .consume
            case 21:
                self = .consumeOk
            case 30:
                self = .cancel
            case 31:
                self = .cancelOk
            case 40:
                self = .publish
            case 50:
                self = .return
            case 60:
                self = .deliver
            case 70:
                self = .get
            case 71:
                self = .getOk
            case 72:
                self = .getEmpty
            case 80:
                self = .ack
            case 90:
                self = .reject
            case 100:
                self = .recoverAsync
            case 110:
                self = .recover
            case 111:
                self = .recoverOk
            case 120:
                self = .nack
            default:
                return nil
            }
        }

        var rawValue: UInt16 {
            switch self {
            case .qos:
                return 10
            case .qosOk:
                return 11
            case .consume:
                return 20
            case .consumeOk:
                return 21
            case .cancel:
                return 30
            case .cancelOk:
                return 31
            case .publish:
                return 40
            case .`return`:
                return 50
            case .deliver:
                return 60
            case .get:
                return 70
            case .getOk:
                return 71
            case .getEmpty:
                return 72
            case .ack:
                return 80
            case .reject:
                return 90
            case .recoverAsync:
                return 100
            case .recover:
                return 110
            case .recoverOk:
                return 111
            case .nack:
                return 120
            }
        }
    }

    public static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let methodID = buffer.readInteger(as: UInt16.self) else {
            throw ProtocolError.decode(type: UInt16.self, context: self)
        }

        guard let kind = Kind(rawValue: methodID) else {
            throw ProtocolError.unsupported(value: methodID, context: self)
        }

        switch kind {
        case .qos:
            guard let (prefetchSize, prefetchCount, global) = buffer.readMultipleIntegers(as: (UInt32, UInt16, UInt8).self) else {
                throw ProtocolError.decode(type:  (UInt32, UInt16, UInt8).self, context: self)
            }
            return .qos(prefetchSize: prefetchSize, prefetchCount: prefetchCount, global: global > 0)
        case .qosOk:
            return .qosOk
        case .consume:
            return try .consume(.decode(from: &buffer))
        case .consumeOk:
            let (consumerTag, _) = try buffer.readShortString() 
            return .consumeOk(consumerTag: consumerTag)
        case .cancel:
            return try .cancel(.decode(from: &buffer))
        case .cancelOk:
            let (consumerTag, _) = try buffer.readShortString()
            return .cancelOk(consumerTag: consumerTag)            
        case .publish:
            return try .publish(.decode(from: &buffer))
        case .`return`:
            return try .return(.decode(from: &buffer))
        case .deliver:
            return try .deliver(.decode(from: &buffer))
        case .get:
            return try .get(.decode(from: &buffer))
        case .getOk:
            return try .getOk(.decode(from: &buffer))
        case .getEmpty:
            let (reserved1, _) = try buffer.readShortString()
            return .getEmpty(reserved1: reserved1)  
        case .ack:
            guard let (deliveryTag, multiple)  = buffer.readMultipleIntegers(as: (UInt64, UInt8).self) else {
                throw ProtocolError.decode(type: (UInt64, UInt8).self, context: self)
            }
            return .ack(deliveryTag: deliveryTag, multiple: multiple > 0)
        case .reject:
            guard let (deliveryTag, requeue)  = buffer.readMultipleIntegers(as: (UInt64, UInt8).self) else {
                throw ProtocolError.decode(type: (UInt64, UInt8).self, context: self)
            }
            return .reject(deliveryTag: deliveryTag, requeue: requeue > 0)
        case .recoverAsync:
            guard let requeue = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }        
            return .recoverAsync(requeue: requeue > 0)    
        case .recover:
            guard let requeue = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }        
            return .recover(requeue: requeue > 0)    
        case .recoverOk:
            return .recoverOk
        case .nack:
            return try.nack(.decode(from: &buffer))
        }
    }

    public func encode(into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(self.kind.rawValue)

        switch self {
        case .qos(let prefetchSize, let prefetchCount, let global):
            buffer.writeMultipleIntegers(prefetchSize, prefetchCount, global ? UInt8(1) : UInt8(0))
        case .qosOk:
            break
        case .consume(let consume):
            try consume.encode(into: &buffer)
        case .consumeOk(let consumerTag):
            try buffer.writeShortString(consumerTag)
        case .cancel(let cancel):
            try cancel.encode(into: &buffer)
        case .cancelOk(let consumerTag):
            try buffer.writeShortString(consumerTag)
        case .publish(let publish):
            try publish.encode(into: &buffer)
        case .`return`(let `return`):
            try `return`.encode(into: &buffer)
        case .deliver(let deliver):
            try deliver.encode(into: &buffer)
        case .get(let get):
            try get.encode(into: &buffer)
        case .getOk(let getOk):
            try getOk.encode(into: &buffer)
        case .getEmpty(let reserved1):
            try buffer.writeShortString(reserved1)
        case .ack(let deliveryTag, let multiple):
            buffer.writeMultipleIntegers(deliveryTag, multiple ? UInt8(1) : UInt8(0))
        case .reject(let deliveryTag, let requeue):
            buffer.writeMultipleIntegers(deliveryTag, requeue ? UInt8(1) : UInt8(0))
        case .recoverAsync(let requeue):
            buffer.writeInteger(requeue ? UInt8(1): UInt8(0))
        case .recover(let requeue):
            buffer.writeInteger(requeue ? UInt8(1): UInt8(0))
        case .recoverOk:
            break
        case .nack(let nack):
            try nack.encode(into: &buffer)
        }
    }

    public struct Consume: PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let queue: String
        public let consumerTag: String
        public let noLocal: Bool
        public let noAck: Bool
        public let exclusive: Bool
        public let noWait: Bool
        public let arguments: Table

        public init(reserved1: UInt16, queue: String, consumerTag: String, noLocal: Bool, noAck: Bool, exclusive: Bool, noWait: Bool, arguments: Table) {
            self.reserved1 = reserved1
            self.queue = queue
            self.consumerTag = consumerTag
            self.noLocal = noLocal
            self.noAck = noAck
            self.exclusive = exclusive
            self.noWait = noWait
            self.arguments = arguments
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (queue, _) = try buffer.readShortString()
            let (consumerTag, _) = try buffer.readShortString()

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            
            let noLocal = bits.isBitSet(pos: 0)
            let noAck = bits.isBitSet(pos: 1)
            let exclusive = bits.isBitSet(pos: 2)
            let noWait = bits.isBitSet(pos: 3)
            let arguments = try Table.decode(from: &buffer)

            return Consume (reserved1: reserved1, queue: queue, consumerTag: consumerTag, noLocal: noLocal, noAck: noAck, exclusive: exclusive, noWait: noWait, arguments: arguments)       
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(queue)
            try buffer.writeShortString(consumerTag)

            var bits = UInt8(0)
            
            if noLocal {
                bits = bits | (1 << 0)
            }

            if noAck {
                bits = bits | (1 << 1)
            }

            if exclusive {
                bits = bits | (1 << 2)
            }

            if `noWait` {
                bits = bits | (1 << 3)
            }

            buffer.writeInteger(bits)
            try arguments.encode(into: &buffer)
        }
    }

    public struct Cancel: PayloadDecodable, PayloadEncodable {
        public let consumerTag: String
        public let noWait: Bool

        public init(consumerTag: String, noWait: Bool) {
            self.consumerTag = consumerTag
            self.noWait = noWait
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let (consumerTag, _) = try buffer.readShortString()

            guard let noWait = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }  

            return Cancel(consumerTag: consumerTag, noWait: noWait > 0)       
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            try buffer.writeShortString(consumerTag)
            buffer.writeInteger(noWait ? UInt8(1) : UInt8(0))
        }
    }

    public struct Publish : PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let exchange: String
        public let routingKey: String
        public let mandatory : Bool
        public let immediate : Bool

        public init(reserved1: UInt16, exchange: String, routingKey: String, mandatory: Bool, immediate: Bool) {
            self.reserved1 = reserved1
            self.exchange = exchange
            self.routingKey = routingKey
            self.mandatory = mandatory
            self.immediate = immediate
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

           let (exchange, _) = try buffer.readShortString()
           let (routingKey, _) = try buffer.readShortString()

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }
            
            let mandatory = bits.isBitSet(pos: 0)
            let immediate = bits.isBitSet(pos: 1)

            return Publish (reserved1: reserved1, exchange: exchange, routingKey: routingKey, mandatory: mandatory, immediate: immediate)       
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(exchange)
            try buffer.writeShortString(routingKey)

            var bits = UInt8(0)
            
            if mandatory {
                bits = bits | (1 << 0)
            }

            if immediate {
                bits = bits | (1 << 1)
            }

            buffer.writeInteger(bits)
        }
    }

    public struct Return: PayloadDecodable, PayloadEncodable {
        public let replyCode: UInt16
        public let replyText: String
        public let exchange: String
        public let routingKey: String

        public init(replyCode: UInt16, replyText: String, exchange: String, routingKey: String) {
            self.replyCode = replyCode
            self.replyText = replyText
            self.exchange = exchange
            self.routingKey = routingKey
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let replyCode = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (replyText, _) = try buffer.readShortString()
            let (exchange, _) = try buffer.readShortString()
            let (routingKey, _) = try buffer.readShortString()

            return Return(replyCode: replyCode, replyText: replyText, exchange: exchange, routingKey: routingKey)       
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(replyCode)
            try buffer.writeShortString(replyText)
            try buffer.writeShortString(exchange)
            try buffer.writeShortString(routingKey)
        }
    }

    public struct Deliver: PayloadDecodable, PayloadEncodable {
        public let consumerTag : String
        public let deliveryTag : UInt64
        public let redelivered: Bool
        public let exchange: String
        public let routingKey: String

        public init(consumerTag: String, deliveryTag: UInt64, redelivered: Bool, exchange: String, routingKey: String) {
            self.consumerTag = consumerTag
            self.deliveryTag = deliveryTag
            self.redelivered = redelivered
            self.exchange = exchange
            self.routingKey = routingKey
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let (consumerTag, _) = try buffer.readShortString()

            guard let deliveryTag = buffer.readInteger(as: UInt64.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }

            guard let redelivered = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }

            let (exchange, _) = try buffer.readShortString()
            let (routingKey, _) = try buffer.readShortString()

            return Deliver(consumerTag: consumerTag, deliveryTag: deliveryTag, redelivered: redelivered > 0, exchange: exchange, routingKey: routingKey)       
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            try buffer.writeShortString(consumerTag)
            buffer.writeInteger(deliveryTag)
            buffer.writeInteger(redelivered ? UInt8(1) : UInt8(0))
            try buffer.writeShortString(exchange)
            try buffer.writeShortString(routingKey)
        }
    }

    public struct Get: PayloadDecodable, PayloadEncodable {
        public let reserved1: UInt16
        public let queue: String
        public let noAck: Bool

        public init(reserved1: UInt16, queue: String, noAck: Bool) {
            self.reserved1 = reserved1
            self.queue = queue
            self.noAck = noAck
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw ProtocolError.decode(type: UInt16.self, context: self)
            }

            let (queue, _) = try buffer.readShortString()

            guard let noAck = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }

            return Get(reserved1: reserved1, queue: queue, noAck: noAck > 0)       
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try buffer.writeShortString(queue)
            buffer.writeInteger(noAck ? UInt8(1) : UInt8(0))
        }
    }

    public struct GetOk: PayloadDecodable, PayloadEncodable {
        public let deliveryTag : UInt64
        public let redelivered: Bool
        public let exchange: String
        public let routingKey: String
        public let messageCount: UInt32

        public init(deliveryTag: UInt64, redelivered: Bool, exchange: String, routingKey: String, messageCount: UInt32) {
            self.deliveryTag = deliveryTag
            self.redelivered = redelivered
            self.exchange = exchange
            self.routingKey = routingKey
            self.messageCount = messageCount
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let deliveryTag = buffer.readInteger(as: UInt64.self) else {
                throw ProtocolError.decode(type: UInt64.self, context: self)
            }

            guard let redelivered = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }

            let (exchange, _) = try buffer.readShortString()
            let (routingKey, _) = try buffer.readShortString()

            guard let messageCount = buffer.readInteger(as: UInt32.self) else {
                throw ProtocolError.decode(type: UInt32.self, context: self)
            }

            return GetOk(deliveryTag: deliveryTag, redelivered: redelivered > 0,  exchange: exchange, routingKey: routingKey, messageCount: messageCount)       
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(deliveryTag)
            buffer.writeInteger(redelivered ? UInt8(1) : UInt8(0))
            try buffer.writeShortString(exchange)
            try buffer.writeShortString(routingKey)
            buffer.writeInteger(messageCount)
        }
    }

    public struct Nack: PayloadDecodable, PayloadEncodable {
        public let deliveryTag : UInt64
        public let multiple: Bool
        public let requeue: Bool

        public init(deliveryTag: UInt64, multiple: Bool, requeue: Bool) {
            self.deliveryTag = deliveryTag
            self.multiple = multiple
            self.requeue = requeue
        }

        public static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let (deliveryTag, bits)  = buffer.readMultipleIntegers(as: (UInt64,  UInt8).self) else {
                throw ProtocolError.decode(type: (UInt64,  UInt8).self, context: self)
            }
            
            let multiple = bits.isBitSet(pos: 0)
            let requeue = bits.isBitSet(pos: 1)

            return Nack(deliveryTag: deliveryTag, multiple: multiple, requeue: requeue)       
        }

        public func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(deliveryTag)

            var bits = UInt8(0)
            
            if multiple {
                bits = bits | (1 << 0)
            }

            if requeue {
                bits = bits | (1 << 1)
            }

            buffer.writeInteger(bits)
        }
    }
}

public enum Confirm: PayloadDecodable, PayloadEncodable {
    case select(noWait: Bool)
    case selectOk

    var kind: Kind {
        switch self {
        case .select:
            return .select
        case .selectOk:
            return .selectOk
        }
    }

    public enum Kind {
        case select
        case selectOk

        public init?(rawValue: UInt16) {
            switch rawValue {
            case 10:
                self = .select
            case 11:
                self = .selectOk
            default:
                return nil
            }
        }

        var rawValue: UInt16 {
            switch self {
            case .select:
                return 10
            case .selectOk:
                return 11
            }
        }
    }

    public static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let methodID = buffer.readInteger(as: UInt16.self) else {
            throw ProtocolError.decode(type: UInt16.self, context: self)
        }

        guard let kind = Kind(rawValue: methodID) else {
            throw ProtocolError.unsupported(value: methodID, context: self)
        }

        switch kind {
        case .select:
            guard let noWait = buffer.readInteger(as: UInt8.self) else {
                throw ProtocolError.decode(type: UInt8.self, context: self)
            }        
            return .select(noWait: noWait > 0)    
        case .selectOk:
            return .selectOk
        }
    }

    public func encode(into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(self.kind.rawValue)

        switch self {
        case .select(let noWait):
            buffer.writeInteger(noWait ? UInt8(1): UInt8(0))
        case .selectOk:
            break
        }
    }
}

public enum Tx: PayloadDecodable, PayloadEncodable {
    case select
    case selectOk
    case commit
    case commitOk
    case rollback
    case rollbackOk

    var kind: Kind {
        switch self {
        case .select:
            return .select
        case .selectOk:
            return .selectOk
        case .commit:
            return .commit
        case .commitOk:
            return .commitOk
        case .rollback:
            return .rollback
        case .rollbackOk:
            return .rollbackOk
        }
    }

    public enum Kind {
        case select
        case selectOk
        case commit
        case commitOk
        case rollback
        case rollbackOk

        public init?(rawValue: UInt16) {
            switch rawValue {
            case 10:
                self = .select
            case 11:
                self = .selectOk
            case 20:
                self = .commit
            case 21:
                self = .commitOk
            case 30:
                self = .rollback
            case 31:
                self = .rollbackOk
            default:
                return nil
            }
        }

        var rawValue: UInt16 {
            switch self {
            case .select:
                return 10
            case .selectOk:
                return 11
            case .commit:
                return 20
            case .commitOk:
                return 21
            case .rollback:
                return 30
            case .rollbackOk:
                return 31
            }
        }
    }

    public static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let methodID = buffer.readInteger(as: UInt16.self) else {
            throw ProtocolError.decode(type: UInt16.self, context: self)
        }

        guard let kind = Kind(rawValue: methodID) else {
            throw ProtocolError.unsupported(value: methodID, context: self)
        }

        switch kind {
        case .select:     
            return .select  
        case .selectOk:
            return .selectOk
        case .commit:
            return .commit
        case .commitOk:
            return .commitOk
        case .rollback:
            return .rollback
        case .rollbackOk:
            return .rollbackOk
        }
    }

    public func encode(into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(self.kind.rawValue)
    }
}
