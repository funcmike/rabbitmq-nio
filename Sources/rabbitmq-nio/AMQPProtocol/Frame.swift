import NIOCore

protocol PayloadDecodable {
    static func decode(from buffer: inout ByteBuffer) throws -> Self
}

protocol PayloadEncodable {
    func encode(into buffer: inout ByteBuffer) throws
}

public enum Frame: PayloadDecodable, PayloadEncodable {
    public typealias ChannelID = UInt16

    case method(ChannelID, Method)
    case header(ChannelID, Header)
    case body(ChannelID, body: [UInt8])
    case heartbeat(ChannelID)

    enum `Type` {
        case method
        case header
        case body
        case heartbeat

        init?(rawValue: UInt8)
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

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawType = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.value(type: UInt8.self)
        }

        guard let channelId = buffer.readInteger(as: ChannelID.self) else {
            throw DecodeError.value(type: ChannelID.self)
        }

        guard let size = buffer.readInteger(as: UInt32.self) else {
            throw DecodeError.value(type: UInt32.self)
        }

        let frame: Frame
        
        switch Type(rawValue: rawType) {
        case .method:
            frame = Self.method(channelId, try Method.decode(from: &buffer))
        case .header:
            frame = Self.header(channelId, try Header.decode(from: &buffer))
        case .body:
            guard let body = buffer.readBytes(length: Int(size)) else {
                throw DecodeError.value(type: [UInt8].self)
            }

            frame = Self.body(channelId, body: body)
        case .heartbeat:
            frame = Self.heartbeat(channelId)
        default:
            throw DecodeError.unsupported(value: rawType)
        }

        guard let endFrame = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.value(type: UInt8.self)
        }

        guard endFrame == 206 else {
            throw DecodeError.unsupported(value: endFrame)
        }

        return frame
    }

    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
        case .method(let channelID, let method):
            buffer.writeInteger(`Type`.method.rawValue)
            buffer.writeInteger(channelID)
            
            let startIndex: Int = buffer.writerIndex
            buffer.writeInteger(UInt32(0)) // placeholder for size
                            
            try method.encode(into: &buffer)

            let size = UInt32(buffer.writerIndex - startIndex - 4)
            buffer.setInteger(size, at: startIndex)

            buffer.writeInteger(UInt8(206)) // endMarker
        case .header(let channelID, let header):
            buffer.writeInteger(`Type`.header.rawValue)
            buffer.writeInteger(channelID)

            let startIndex: Int = buffer.writerIndex
            buffer.writeInteger(UInt32(0)) // placeholder for size
                            
            try header.encode(into: &buffer)

            let size = UInt32(buffer.writerIndex - startIndex - 4)
            buffer.setInteger(size, at: startIndex)

            buffer.writeInteger(UInt8(206)) // endMarker
        case .body(let channelID, let body):
            buffer.writeInteger(`Type`.body.rawValue)
            buffer.writeInteger(channelID)
            buffer.writeInteger(body.count)
            buffer.writeBytes(body)
            buffer.writeInteger(UInt8(206)) // endMarker
        case .heartbeat(let channelID):
            buffer.writeInteger(`Type`.heartbeat.rawValue)
            buffer.writeInteger(channelID)
            buffer.writeInteger(UInt32(0))
            buffer.writeInteger(UInt8(206)) // endMarker
        }
    }

    public struct Header: PayloadDecodable, PayloadEncodable {
        let classID: UInt16
        let weight: UInt16
        let bodySize: UInt64
        let properties: Properties

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let classID = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            guard let weight = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            guard let bodySize = buffer.readInteger(as: UInt64.self) else {
                throw DecodeError.value(type: UInt64.self)
            }
            
            let properties = try Properties.decode(from: &buffer)

            return Header(classID: classID, weight: weight, bodySize: bodySize, properties: properties)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(classID)
            buffer.writeInteger(weight)
            buffer.writeInteger(bodySize)
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

    enum ID {
        case connection
        case channel
        case exchange
        case queue
        case basic
        case confirm
        case tx

        init?(rawValue: UInt16)
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

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self)
        }
    
        switch ID(rawValue: rawID) {
            case .connection:
                return .connection(try Connection.decode(from: &buffer))
            case .channel:
                return .channel(try Channel.decode(from: &buffer))
            case .exchange:
                return .exchange(try Exchange.decode(from: &buffer))
            case .queue:
                return .queue(try Queue.decode(from: &buffer))
            case .basic:
                return .basic(try Basic.decode(from: &buffer))
            case .confirm:
                return .confirm(try Confirm.decode(from: &buffer))
            case .tx:
                return .tx(try Tx.decode(from: &buffer))
            default:
                throw DecodeError.unsupported(value: rawID)
        }
    }

    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
        case .connection(let connection):
            buffer.writeInteger(ID.connection.rawValue)
            try connection.encode(into: &buffer)
        case .channel(let channel):
            buffer.writeInteger(ID.channel.rawValue)
            try channel.encode(into: &buffer)
        case .exchange(let exchange):
            buffer.writeInteger(ID.exchange.rawValue)
            try exchange.encode(into: &buffer)
        case .queue(let queue): 
            buffer.writeInteger(ID.queue.rawValue)
            try queue.encode(into: &buffer)
        case .basic(let basic):
            buffer.writeInteger(ID.basic.rawValue)
            try basic.encode(into: &buffer)
        case .confirm(let confirm):
            buffer.writeInteger(ID.confirm.rawValue)
            try confirm.encode(into: &buffer)
        case .tx(let tx): 
            buffer.writeInteger(ID.confirm.rawValue)
            try tx.encode(into: &buffer)
        }
    }
}

public enum Connection: PayloadDecodable, PayloadEncodable {
    case start(Start)
    case startOk(StartOk)
    case tune(Tune)
    case tuneOk(TuneOk)
    case open(Open)
    case openOk(reserved1: String)
    case close(Close)
    case closeOk
    case blocked(reason: String)
    case unblocked


    public enum ID {
        case start
        case startOk
        case tune
        case tuneOk
        case open
        case openOk
        case close
        case closeOk
        case blocked
        case unblocked

        init?(rawValue: UInt16)
        {
            switch rawValue {
            case 10:
                self = .start
            case 11:
                self = .startOk
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
            case .tune:
                return 30
            case .tuneOk:
                return 31
            case .open:
                return 40
            case .openOk:
                return 41
            case .blocked: 
                return 50
            case .unblocked: 
                return 51
            case .close: 
                return 60
            case .closeOk: 
                return 61
            }
        }
    }

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self)
        }
    
        switch ID(rawValue: rawID) {
        case .start:
            return .start(try Start.decode(from: &buffer))
        case .startOk:
            return .startOk(try StartOk.decode(from: &buffer))
        case .tune:
            return .tune(try Tune.decode(from: &buffer))
        case .tuneOk:
            return .tuneOk(try TuneOk.decode(from: &buffer))
        case .open:
            return .open(try Open.decode(from: &buffer))
        case .openOk:
            let (reserved1, _) = try readShortStr(from: &buffer)
            return .openOk(reserved1: reserved1)
        case .close:
            return .close(try Close.decode(from: &buffer))
        case .closeOk:
            return .closeOk
        case .blocked:
            let (reason, _) = try readShortStr(from: &buffer)
            return .blocked(reason: reason)
        case .unblocked:
            return .unblocked
        default:
            throw DecodeError.unsupported(value: rawID)
        }
    }

    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
        case .start(let connectionStart):
            buffer.writeInteger(ID.start.rawValue)
            try connectionStart.encode(into: &buffer)
        case .startOk(let connectionStartOk):
            buffer.writeInteger(ID.startOk.rawValue)
            try connectionStartOk.encode(into: &buffer)
        case .tune(let tune):
            buffer.writeInteger(ID.tune.rawValue)
            try tune.encode(into: &buffer)
        case .tuneOk(let tuneOk):
            buffer.writeInteger(ID.tuneOk.rawValue)
            try tuneOk.encode(into: &buffer)
        case .open(let open): 
            buffer.writeInteger(ID.open.rawValue)
            try open.encode(into: &buffer)
        case .openOk(let reserved1): 
            buffer.writeInteger(ID.openOk.rawValue)
            try writeShortStr(value: reserved1, into: &buffer)
        case .close(let close):
            buffer.writeInteger(ID.close.rawValue)
            try close.encode(into: &buffer)
        case .closeOk: 
            buffer.writeInteger(ID.closeOk.rawValue)
        case .blocked(let reason):
            buffer.writeInteger(ID.blocked.rawValue)
            try writeShortStr(value: reason, into: &buffer)
        case .unblocked:
            buffer.writeInteger(ID.unblocked.rawValue)
        }
    }

    public struct Start: PayloadDecodable {
        let versionMajor: UInt8
        let versionMinor: UInt8
        let serverProperties: Table
        let mechanisms: String
        let locales: String

        init(versionMajor: UInt8 = 0, versionMinor: UInt8 = 9, serverProperties: Table = [
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

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let versionMajor = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            guard let versionMinor = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            let (serverProperties, _) = try readTable(from: &buffer)
            let (mechanisms, _) = try readLongStr(from: &buffer)
            let (locales, _) = try readLongStr(from: &buffer)

            return Start(versionMajor: versionMajor, versionMinor: versionMinor, serverProperties: serverProperties, mechanisms: mechanisms, locales: locales)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(versionMajor)
            buffer.writeInteger(versionMinor)
            try writeTable(values: serverProperties, into: &buffer)
            try writeLongStr(value: mechanisms, into: &buffer)
            try writeLongStr(value: locales, into: &buffer)
        }
    }

    public struct StartOk: PayloadDecodable, PayloadEncodable {
        let clientProperties: Table
        let mechanism: String
        let response: String
        let locale: String

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let (clientProperties, _) = try readTable(from: &buffer)
            let (mechanism, _) =  try readShortStr(from: &buffer)
            let (response, _) = try readLongStr(from: &buffer)
            let (locale, _) = try readShortStr(from: &buffer)

            return StartOk(clientProperties: clientProperties, mechanism: mechanism, response: response, locale: locale)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            try writeTable(values: clientProperties, into: &buffer)
            try writeShortStr(value: mechanism, into: &buffer)
            try writeLongStr(value: response, into: &buffer)
            try writeShortStr(value: locale, into: &buffer)
        }
    }

    public struct Tune: PayloadDecodable, PayloadEncodable {
        let channelMax: UInt16
        let frameMax: UInt32
        let heartbeat: UInt16

        init(channelMax: UInt16 = 0, frameMax: UInt32 = 131072, heartbeat: UInt16 = 0)
        {
            self.channelMax = channelMax
            self.frameMax = frameMax
            self.heartbeat = heartbeat
        }

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let channelMax = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            guard let frameMax = buffer.readInteger(as: UInt32.self) else {
                throw DecodeError.value(type: UInt32.self)
            }

            guard let heartbeat = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            return Tune(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(channelMax)
            buffer.writeInteger(frameMax)
            buffer.writeInteger(heartbeat)
        }
    }


    public struct TuneOk: PayloadDecodable, PayloadEncodable {
        let channelMax: UInt16
        let frameMax: UInt32
        let heartbeat: UInt16

        init(channelMax: UInt16 = 0, frameMax: UInt32 = 131072, heartbeat: UInt16 = 60)
        {
            self.channelMax = channelMax
            self.frameMax = frameMax
            self.heartbeat = heartbeat
        }

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let channelMax = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            guard let frameMax = buffer.readInteger(as: UInt32.self) else {
                throw DecodeError.value(type: UInt32.self)
            }

            guard let heartbeat = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            return TuneOk(channelMax: channelMax, frameMax: frameMax, heartbeat: heartbeat)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(channelMax)
            buffer.writeInteger(frameMax)
            buffer.writeInteger(heartbeat)
        }
    }


    public struct Open: PayloadDecodable, PayloadEncodable {
        let vhost: String
        let reserved1: String
        let reserved2: Bool

        init(vhost: String = "/", reserved1: String = "", reserved2: Bool = false)
        {
            self.vhost = vhost
            self.reserved1 = reserved1
            self.reserved2 = reserved2
        }

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let (vhost, _) = try readShortStr(from: &buffer)
            let (reserved1, _) = try readShortStr(from: &buffer)

            guard let reserved2 = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            return Open(vhost: vhost, reserved1: reserved1, reserved2: reserved2 > 0 ? true : false)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            try writeShortStr(value: vhost, into: &buffer)
            try writeShortStr(value: reserved1, into: &buffer)
            buffer.writeInteger(reserved2 ? UInt8(1) : UInt8(0))
        }
    }


    public struct Close: PayloadDecodable, PayloadEncodable {
        let replyCode: UInt16
        let replyText: String
        let failingClassID: UInt16
        let failingMethodID:  UInt16

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let replyCode = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            let (replyText, _) = try readShortStr(from: &buffer)

            guard let failingClassID = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            guard let failingMethodID = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            return Close(replyCode: replyCode, replyText: replyText, failingClassID: failingClassID, failingMethodID: failingMethodID)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(replyCode)
            try writeShortStr(value: replyText, into: &buffer)
            buffer.writeInteger(failingClassID)
            buffer.writeInteger(failingMethodID)
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

    public enum ID {
        case open
        case openOk
        case flow
        case flowOk
        case close
        case closeOk

        init?(rawValue: UInt16)
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

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self)
        }

        switch ID(rawValue: rawID) {
        case .open:
           let (reserved1, _) = try readShortStr(from: &buffer)
            return .open(reserved1: reserved1)
        case .openOk:
           let (reserved1, _) = try readLongStr(from: &buffer)
            return .open(reserved1: reserved1)                
        case .flow:
            guard let active = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
            return .flow(active: active == 1)               
        case .flowOk:
            guard let active = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
            return .flowOk(active: active == 1)       
        case .close: 
            return .close(try Close.decode(from: &buffer))
        case .closeOk:
            return .closeOk
        default:
            throw DecodeError.unsupported(value: rawID)
        }
    }

    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
        case .open(let reserved1):
            buffer.writeInteger(ID.open.rawValue)
            try writeShortStr(value: reserved1, into: &buffer) 
        case .openOk(let reserved1):
            buffer.writeInteger(ID.openOk.rawValue)
            try writeLongStr(value: reserved1, into: &buffer) 
        case .flow(let active):
            buffer.writeInteger(ID.flow.rawValue)
            buffer.writeInteger(active ? UInt8(1) : UInt8(0))
        case .flowOk(let active):
            buffer.writeInteger(ID.flowOk.rawValue)
            buffer.writeInteger(active ? UInt8(1) : UInt8(0))
        case .close(let close):
            buffer.writeInteger(ID.close.rawValue)
            try close.encode(into: &buffer)
        case .closeOk:
            buffer.writeInteger(ID.closeOk.rawValue)
        }
    }

    public struct Close: PayloadDecodable, PayloadEncodable {
        let replyCode: UInt16
        let replyText: String
        let classID: UInt16
        let methodID: UInt16

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let replyCode = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (replyText, _) = try readShortStr(from: &buffer)

            guard let classID = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }
    
            guard let methodID = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            return Close(replyCode: replyCode, replyText: replyText, classID: classID, methodID: methodID)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(replyCode)
            try writeShortStr(value: replyText, into: &buffer)
            buffer.writeInteger(classID)
            buffer.writeInteger(methodID)
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

    public enum ID {
        case declare
        case declareOk
        case delete
        case deleteOk
        case bind
        case bindOk
        case unbind
        case unbindOk

        init?(rawValue: UInt16)
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

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self)
        }
        
        switch ID(rawValue: rawID) {
        case .declare:
            return .declare(try Declare.decode(from: &buffer))
        case .declareOk:
            return .declareOk
        case .delete:
            return .delete(try Delete.decode(from: &buffer))
        case .deleteOk:
            return .deleteOk
        case .bind:
            return .bind(try Bind.decode(from: &buffer))
        case .bindOk:
            return .bindOk
        case .unbind:
            return .unbind(try Unbind.decode(from: &buffer))
        case .unbindOk:
            return .unbindOk
        default:
            throw DecodeError.unsupported(value: rawID)
        }
    }

    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
        case .declare(let declare):
            buffer.writeInteger(ID.declare.rawValue)
            try declare.encode(into: &buffer)
        case .declareOk:
            buffer.writeInteger(ID.declare.rawValue)
        case .delete(let deleteOk):
            buffer.writeInteger(ID.deleteOk.rawValue)
            try deleteOk.encode(into: &buffer)
        case .deleteOk:
            buffer.writeInteger(ID.deleteOk.rawValue)
        case .bind(let bind):
            buffer.writeInteger(ID.bind.rawValue)
            try bind.encode(into: &buffer)
        case .bindOk:
            buffer.writeInteger(ID.bindOk.rawValue)
        case .unbind(let unbind):
            buffer.writeInteger(ID.unbind.rawValue)
            try unbind.encode(into: &buffer)
        case .unbindOk:
            buffer.writeInteger(ID.unbindOk.rawValue)
        }
    }

    public struct Declare: PayloadDecodable, PayloadEncodable {
        let reserved1: UInt16
        let exchangeName: String
        let exchangeType: String
        let passive: Bool
        let durable: Bool
        let autoDelete: Bool
        let `internal`: Bool
        let noWait: Bool
        let arguments: Table

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (exchangeName, _) = try readShortStr(from: &buffer)
            let (exchangeType, _) = try readShortStr(from: &buffer)

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
            
            let passive = bits.isBitSet(pos: 0)
            let durable = bits.isBitSet(pos: 1)
            let autoDelete = bits.isBitSet(pos: 2)
            let `internal` = bits.isBitSet(pos: 3)
            let noWait = bits.isBitSet(pos: 4)
            let (arguments, _) = try readTable(from: &buffer)

            return Declare(reserved1: reserved1, exchangeName: exchangeName, exchangeType: exchangeType, passive: passive, durable: durable, autoDelete: autoDelete, internal: `internal`, noWait: noWait, arguments: arguments)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: exchangeName, into: &buffer)
            try writeShortStr(value: exchangeType, into: &buffer)

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
            try writeTable(values: arguments, into: &buffer)
        }
    }

    public struct Delete: PayloadDecodable, PayloadEncodable {
        let reserved1: UInt16
        let exchangeName: String
        let ifUnused : Bool
        let noWait: Bool

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (exchangeName, _) = try readShortStr(from: &buffer)

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
            
            let ifUnused = bits.isBitSet(pos: 0)
            let noWait = bits.isBitSet(pos: 1)

            return Delete(reserved1: reserved1, exchangeName: exchangeName, ifUnused: ifUnused, noWait: noWait)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: exchangeName, into: &buffer)

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
        let reserved1: UInt16
        let destination: String
        let source: String
        let routingKey: String
        let noWait: Bool
        let arguments: Table

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (destination, _) = try readShortStr(from: &buffer)
            let (source, _) = try readShortStr(from: &buffer)
            let (routingKey, _) = try readShortStr(from: &buffer)

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
            
            let noWait = bits.isBitSet(pos: 0)
            let (arguments, _) = try readTable(from: &buffer)

            return Bind(reserved1: reserved1, destination: destination, source: source, routingKey: routingKey, noWait: noWait, arguments: arguments)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: destination, into: &buffer)
            try writeShortStr(value: routingKey, into: &buffer)
            buffer.writeInteger(noWait ? UInt8(1) : UInt8(0))
            try writeTable(values: arguments, into: &buffer)
        }
    }

    public struct Unbind: PayloadDecodable, PayloadEncodable {
        let reserved1: UInt16
        let destination: String
        let source: String
        let routingKey: String
        let noWait: Bool
        let arguments: Table

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (destination, _) = try readShortStr(from: &buffer)
            let (source, _) = try readShortStr(from: &buffer)
            let (routingKey, _) = try readShortStr(from: &buffer)

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
            
            let noWait = bits.isBitSet(pos: 0)
            let (arguments, _) = try readTable(from: &buffer)

            return Unbind(reserved1: reserved1, destination: destination, source: source, routingKey: routingKey, noWait: noWait, arguments: arguments)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: destination, into: &buffer)
            try writeShortStr(value: routingKey, into: &buffer)
            buffer.writeInteger(noWait ? UInt8(1) : UInt8(0))
            try writeTable(values: arguments, into: &buffer)
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

    public enum ID {
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

        init?(rawValue: UInt16)
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

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self)
        }

        switch ID(rawValue: rawID) {
        case .declare:
            return .declare(try Declare.decode(from: &buffer))
        case .declareOk:
            return .declareOk(try DeclareOk.decode(from: &buffer))
        case .bind:
            return .bind(try Bind.decode(from: &buffer))
        case .bindOk:
            return .bindOk
        case .purge:
            return .purge(try Purge.decode(from: &buffer))
        case .purgeOk:
            guard let messageCount = buffer.readInteger(as: UInt32.self) else {
                throw DecodeError.value(type: UInt32.self)
            }            
            return .purgeOk(messageCount: messageCount)            
        case .delete:
            return .delete(try Delete.decode(from: &buffer))
        case .deleteOk:
            guard let messageCount = buffer.readInteger(as: UInt32.self) else {
                throw DecodeError.value(type: UInt32.self)
            }            
            return .deleteOk(messageCount: messageCount)
        case .unbind:
            return .unbind(try Unbind.decode(from: &buffer))
        case .unbindOk:
            return .unbindOk
        default:
            throw DecodeError.unsupported(value: rawID)
        }
    }

    func encode(into buffer: inout ByteBuffer) throws {
        switch self  {       
        case .bind(let bind):
            buffer.writeInteger(ID.bind.rawValue)
            try bind.encode(into: &buffer)
        case .bindOk:
            buffer.writeInteger(ID.bindOk.rawValue)
        case .declare(let declare):
            buffer.writeInteger(ID.declare.rawValue)
            try declare.encode(into: &buffer)
        case .declareOk(let declareOk):
            buffer.writeInteger(ID.declareOk.rawValue)
            try declareOk.encode(into: &buffer)
        case .purge(let purge):
            buffer.writeInteger(ID.purge.rawValue)
            try purge.encode(into: &buffer)
        case .purgeOk(let messageCount):
            buffer.writeInteger(ID.purgeOk.rawValue)
            buffer.writeInteger(messageCount)
        case .delete(let delete):
            buffer.writeInteger(ID.purgeOk.rawValue)
            try delete.encode(into: &buffer)
        case .deleteOk(let messageCount):
            buffer.writeInteger(ID.deleteOk.rawValue)
            buffer.writeInteger(messageCount)
        case .unbind(let unbind):
            buffer.writeInteger(ID.unbind.rawValue)
            try unbind.encode(into: &buffer)
        case .unbindOk:
            buffer.writeInteger(ID.unbindOk.rawValue)
        }
    }

    public struct Declare: PayloadDecodable, PayloadEncodable {
        let reserved1: UInt16
        let queueName : String
        let passive: Bool
        let durable: Bool
        let autoDelete: Bool
        let `internal`: Bool
        let noWait: Bool
        let arguments: Table

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (queueName, _) = try readShortStr(from: &buffer)

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
            
            let passive = bits.isBitSet(pos: 0)
            let durable = bits.isBitSet(pos: 1)
            let autoDelete = bits.isBitSet(pos: 2)
            let `internal` = bits.isBitSet(pos: 3)
            let noWait = bits.isBitSet(pos: 4)
            let (arguments, _) = try readTable(from: &buffer)

            return Declare(reserved1: reserved1, queueName: queueName, passive: passive, durable: durable, autoDelete: autoDelete, internal: `internal`, noWait: noWait, arguments: arguments)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: queueName, into: &buffer)

            var bits: UInt8 = UInt8(0)
            
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
            
            try writeTable(values: arguments, into: &buffer)
        }
    }

    public struct DeclareOk: PayloadDecodable, PayloadEncodable {
        let queueName : String
        let messageCount: UInt32
        let consumerCount: UInt32


        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let (queueName, _) = try readShortStr(from: &buffer)

            guard let messageCount = buffer.readInteger(as: UInt32.self) else {
                throw DecodeError.value(type: UInt32.self)
            }

            guard let consumerCount = buffer.readInteger(as: UInt32.self) else {
                throw DecodeError.value(type: UInt32.self)
            }

            return DeclareOk(queueName: queueName, messageCount: messageCount, consumerCount: consumerCount)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            try writeShortStr(value: queueName, into: &buffer)
            buffer.writeInteger(messageCount)
            buffer.writeInteger(consumerCount)
        }
    }

    public struct Bind: PayloadDecodable, PayloadEncodable {
        let reserved1: UInt16
        let queueName : String
        let exchangeName: String
        let routingKey : String
        let noWait: Bool
        let arguments: Table

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (queueName, _) = try readShortStr(from: &buffer)
            let (exchangeName, _) = try readShortStr(from: &buffer)
            let (routingKey, _) = try readShortStr(from: &buffer)

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            let noWait = bits.isBitSet(pos: 0)
            let (arguments, _) = try readTable(from: &buffer)

            return Bind(reserved1: reserved1, queueName: queueName, exchangeName: exchangeName, routingKey: routingKey, noWait: noWait, arguments: arguments)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: queueName, into: &buffer)
            try writeShortStr(value: exchangeName, into: &buffer)
            try writeShortStr(value: routingKey, into: &buffer)
            buffer.writeInteger(noWait ? UInt8(1) : UInt8(0))
            try writeTable(values: arguments, into: &buffer)
        }
    }

    public struct Purge: PayloadDecodable, PayloadEncodable {
        let reserved1: UInt16
        let queueName : String
        let noWait: Bool

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (queueName, _) = try readShortStr(from: &buffer)

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            let noWait = bits.isBitSet(pos: 0)

            return Purge(reserved1: reserved1, queueName: queueName, noWait: noWait)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: queueName, into: &buffer)
            buffer.writeInteger(noWait ? UInt8(1) : UInt8(0))
        }
    }


    public struct Delete: PayloadDecodable, PayloadEncodable {
        let reserved1: UInt16
        let queueName : String
        let ifUnused: Bool
        let ifEmpty : Bool
        let noWait: Bool

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (queueName, _) = try readShortStr(from: &buffer)

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            let ifUnused = bits.isBitSet(pos: 0)
            let ifEmpty = bits.isBitSet(pos: 1)
            let noWait = bits.isBitSet(pos: 2)

            return Delete(reserved1: reserved1, queueName: queueName, ifUnused: ifUnused, ifEmpty: ifEmpty, noWait: noWait)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: queueName, into: &buffer)

            var bits: UInt8 = UInt8(0)

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
        let reserved1: UInt16
        let queueName : String
        let exchangeName: String
        let routingKey : String
        let noWait: Bool
        let arguments: Table

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (queueName, _) = try readShortStr(from: &buffer)
            let (exchangeName, _) = try readShortStr(from: &buffer)
            let (routingKey, _) = try readShortStr(from: &buffer)

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            let noWait = bits.isBitSet(pos: 0)
            let (arguments, _) = try readTable(from: &buffer)

            return Unbind(reserved1: reserved1, queueName: queueName, exchangeName: exchangeName, routingKey: routingKey, noWait: noWait, arguments: arguments)
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: queueName, into: &buffer)
            try writeShortStr(value: exchangeName, into: &buffer)
            try writeShortStr(value: routingKey, into: &buffer)
            buffer.writeInteger(noWait ? UInt8(1) : UInt8(0))
            try writeTable(values: arguments, into: &buffer)
        }
    }
}

public enum Basic: PayloadDecodable, PayloadEncodable {
    case qos(Qos)
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
    case ack(Ack)
    case reject(Reject)
    case recoverAsync(requeue: Bool)
    case recover(requeue: Bool)
    case recoverOk
    case nack(Nack)

    public enum ID {
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

        init?(rawValue: UInt16) {
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

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self)
        }

        switch ID(rawValue: rawID) {
        case .qos:
            return .qos(try Qos.decode(from: &buffer))
        case .qosOk:
            return .qosOk
        case .consume:
            return .consume(try Consume.decode(from: &buffer))
        case .consumeOk:
            let (consumerTag, _) = try readShortStr(from: &buffer) 
            return .consumeOk(consumerTag: consumerTag)
        case .cancel:
            return .cancel(try Cancel.decode(from: &buffer))
        case .cancelOk:
            let (consumerTag, _) = try readShortStr(from: &buffer)
            return .cancelOk(consumerTag: consumerTag)            
        case .publish:
            return .publish(try Publish.decode(from: &buffer))
        case .`return`:
            return .return(try Return.decode(from: &buffer))
        case .deliver:
            return .deliver(try Deliver.decode(from: &buffer))
        case .get:
            return .get(try Get.decode(from: &buffer))
        case .getOk:
            return .getOk(try GetOk.decode(from: &buffer))
        case .getEmpty:
            let (reserved1, _) = try readShortStr(from: &buffer)
            return .getEmpty(reserved1: reserved1)  
        case .ack:
            return .ack(try Ack.decode(from: &buffer))
        case .reject:
            return .reject(try Reject.decode(from: &buffer))
        case .recoverAsync:
            guard let requeue = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }        
            return .recoverAsync(requeue: requeue == 1)    
        case .recover:
            guard let requeue = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }        
            return .recover(requeue: requeue == 1)    
        case .recoverOk:
            return .recoverOk
        case .nack:
            return .nack(try Nack.decode(from: &buffer))
        default:
            throw DecodeError.unsupported(value: rawID)
        }
    }

    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
        case .qos(let qos):
            buffer.writeInteger(ID.qos.rawValue)
            try qos.encode(into: &buffer)
        case .qosOk:
            buffer.writeInteger(ID.qosOk.rawValue)
        case .consume(let consume):
            buffer.writeInteger(ID.consume.rawValue)
            try consume.encode(into: &buffer)
        case .consumeOk(let consumerTag):
            buffer.writeInteger(ID.consumeOk.rawValue)
            try writeShortStr(value: consumerTag, into: &buffer)
        case .cancel(let cancel):
            buffer.writeInteger(ID.cancel.rawValue)
            try cancel.encode(into: &buffer)
        case .cancelOk(let consumerTag):
            buffer.writeInteger(ID.cancelOk.rawValue)
            try writeShortStr(value: consumerTag, into: &buffer)
        case .publish(let publish):
            buffer.writeInteger(ID.cancelOk.rawValue)
            try publish.encode(into: &buffer)
        case .`return`(let `return`):
            buffer.writeInteger(ID.return.rawValue)
            try `return`.encode(into: &buffer)
        case .deliver(let deliver):
            buffer.writeInteger(ID.deliver.rawValue)
            try deliver.encode(into: &buffer)
        case .get(let get):
            buffer.writeInteger(ID.get.rawValue)
            try get.encode(into: &buffer)
        case .getOk(let getOk):
            buffer.writeInteger(ID.getOk.rawValue)
            try getOk.encode(into: &buffer)
        case .getEmpty(let reserved1):
            buffer.writeInteger(ID.getEmpty.rawValue)
            try writeShortStr(value: reserved1, into: &buffer)
        case .ack(let ack):
            buffer.writeInteger(ID.ack.rawValue)
            try ack.encode(into: &buffer)
        case .reject(let reject):
            buffer.writeInteger(ID.reject.rawValue)
            try reject.encode(into: &buffer)
        case .recoverAsync(let requeue):
            buffer.writeInteger(ID.recover.rawValue)
            buffer.writeInteger(requeue ? UInt8(1): UInt8(0))
        case .recover(let requeue):
            buffer.writeInteger(ID.recover.rawValue)
            buffer.writeInteger(requeue ? UInt8(1): UInt8(0))
        case .recoverOk:
            buffer.writeInteger(ID.recoverOk.rawValue)
        case .nack(let nack):
            buffer.writeInteger(ID.nack.rawValue)
            try nack.encode(into: &buffer)
        }
    }

    public struct Qos: PayloadDecodable, PayloadEncodable {
        let prefetchSize: UInt32
        let prefetchCount: UInt16
        let global: Bool

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let prefetchSize = buffer.readInteger(as: UInt32.self) else {
                throw DecodeError.value(type: UInt32.self)
            }

            guard let prefetchCount = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            guard let global = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }  

            return Qos(prefetchSize: prefetchSize, prefetchCount: prefetchCount, global: global == 1)       
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(prefetchSize)
            buffer.writeInteger(prefetchCount)
            buffer.writeInteger(global ? UInt8(1) : UInt8(0))
        }
    }

    public struct Consume : PayloadDecodable, PayloadEncodable {
        let reserved1: UInt16
        let queue: String
        let consumerTag: String
        let noLocal: Bool
        let noAck: Bool
        let exclusive: Bool
        let noWait: Bool
        let arguments: Table

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (queue, _) = try readShortStr(from: &buffer)
            let (consumerTag, _) = try readShortStr(from: &buffer)

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
            
            let noLocal = bits.isBitSet(pos: 0)
            let noAck = bits.isBitSet(pos: 1)
            let exclusive = bits.isBitSet(pos: 2)
            let noWait = bits.isBitSet(pos: 3)
            let (arguments, _) = try readTable(from: &buffer)

            return Consume (reserved1: reserved1, queue: queue, consumerTag: consumerTag, noLocal: noLocal, noAck: noAck, exclusive: exclusive, noWait: noWait, arguments: arguments)       
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: queue, into: &buffer)
            try writeShortStr(value: consumerTag, into: &buffer)

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
            try writeTable(values: arguments, into: &buffer)
        }
    }

    public struct Cancel: PayloadDecodable, PayloadEncodable {
        let consumerTag: String
        let noWait: Bool

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let (consumerTag, _) = try readShortStr(from: &buffer)

            guard let noWait = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }  

            return Cancel(consumerTag: consumerTag, noWait: noWait == 1)       
        }

        func encode(into buffer: inout ByteBuffer) throws {
            try writeShortStr(value: consumerTag, into: &buffer)
            buffer.writeInteger(noWait ? UInt8(1) : UInt8(0))
        }
    }

    public struct Publish : PayloadDecodable, PayloadEncodable {
        let reserved1: UInt16
        let exchange: String
        let reoutingKey: String
        let mandatory : Bool
        let immediate : Bool

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

           let (exchange, _) = try readShortStr(from: &buffer)
           let (reoutingKey, _) = try readShortStr(from: &buffer)

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
            
            let mandatory = bits.isBitSet(pos: 0)
            let immediate = bits.isBitSet(pos: 1)

            return Publish (reserved1: reserved1, exchange: exchange, reoutingKey: reoutingKey, mandatory: mandatory, immediate: immediate)       
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: exchange, into: &buffer)
            try writeShortStr(value: reoutingKey, into: &buffer)

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
        let replyCode: UInt16
        let replyText: String
        let exchange: String
        let routingKey: String


        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let replyCode = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (replyText, _) = try readShortStr(from: &buffer)
            let (exchange, _) = try readShortStr(from: &buffer)
            let (routingKey, _) = try readShortStr(from: &buffer)

            return Return(replyCode: replyCode, replyText: replyText, exchange: exchange, routingKey: routingKey)       
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(replyCode)
            try writeShortStr(value: replyText, into: &buffer)
            try writeShortStr(value: exchange, into: &buffer)
            try writeShortStr(value: routingKey, into: &buffer)
        }
    }

    public struct Deliver: PayloadDecodable, PayloadEncodable {
        let consumerTag : String
        let deliveryTag : UInt64
        let redelivered: Bool
        let exchange: String
        let routingKey: String


        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            let (consumerTag, _) = try readShortStr(from: &buffer)

            guard let redelivered = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            guard let deliveryTag = buffer.readInteger(as: UInt64.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            let (exchange, _) = try readShortStr(from: &buffer)
            let (routingKey, _) = try readShortStr(from: &buffer)

            return Deliver(consumerTag: consumerTag, deliveryTag: deliveryTag, redelivered: redelivered == 1, exchange: exchange, routingKey: routingKey)       
        }

        func encode(into buffer: inout ByteBuffer) throws {
            try writeShortStr(value: consumerTag, into: &buffer)
            buffer.writeInteger(deliveryTag)
            buffer.writeInteger(redelivered ? UInt8(1) : UInt8(0))
            try writeShortStr(value: exchange, into: &buffer)
            try writeShortStr(value: routingKey, into: &buffer)
        }
    }

    public struct Get: PayloadDecodable, PayloadEncodable {
        let reserved1: UInt16
        let queue : String
        let noAck  : Bool

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let reserved1 = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.value(type: UInt16.self)
            }

            let (queue, _) = try readShortStr(from: &buffer)

            guard let noAck = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            return Get(reserved1: reserved1, queue: queue, noAck: noAck == 1)       
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(reserved1)
            try writeShortStr(value: queue, into: &buffer)
            buffer.writeInteger(noAck ? UInt8(1) : UInt8(0))
        }
    }

    public struct GetOk: PayloadDecodable, PayloadEncodable {
        let deliveryTag : UInt64
        let redelivered: Bool
        let exchange: String
        let routingKey: String
        let messageCount: UInt32


        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let deliveryTag = buffer.readInteger(as: UInt64.self) else {
                throw DecodeError.value(type: UInt64.self)
            }

            guard let redelivered = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }

            let (exchange, _) = try readShortStr(from: &buffer)
            let (routingKey, _) = try readShortStr(from: &buffer)

            guard let messageCount = buffer.readInteger(as: UInt32.self) else {
                throw DecodeError.value(type: UInt32.self)
            }

            return GetOk(deliveryTag: deliveryTag, redelivered: redelivered == 1,  exchange: exchange, routingKey: routingKey, messageCount: messageCount)       
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(deliveryTag)
            buffer.writeInteger(redelivered ? UInt8(1) : UInt8(0))
            try writeShortStr(value: exchange, into: &buffer)
            try writeShortStr(value: routingKey, into: &buffer)
            buffer.writeInteger(messageCount)
        }
    }

    public struct Ack: PayloadDecodable, PayloadEncodable {
        let deliveryTag : UInt64
        let multiple : Bool

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let deliveryTag  = buffer.readInteger(as: UInt64.self) else {
                throw DecodeError.value(type: UInt64.self)
            }  

            guard let multiple  = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }  

            return Ack(deliveryTag: deliveryTag, multiple: multiple == 1)       
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(deliveryTag)
            buffer.writeInteger(multiple ? UInt8(1) : UInt8(0))
        }
    }

    public struct Reject : PayloadDecodable, PayloadEncodable {
        let deliveryTag : UInt64
        let requeue: Bool

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let deliveryTag  = buffer.readInteger(as: UInt64.self) else {
                throw DecodeError.value(type: UInt64.self)
            }  

            guard let requeue  = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }  

            return Reject(deliveryTag: deliveryTag, requeue: requeue == 1)       
        }

        func encode(into buffer: inout ByteBuffer) throws {
            buffer.writeInteger(deliveryTag)
            buffer.writeInteger(requeue ? UInt8(1) : UInt8(0))
        }
    }

    public struct Nack: PayloadDecodable, PayloadEncodable {
        let deliveryTag : UInt64
        let multiple: Bool
        let requeue: Bool

        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            guard let deliveryTag  = buffer.readInteger(as: UInt64.self) else {
                throw DecodeError.value(type: UInt64.self)
            }  

            guard let bits = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }
            
            let multiple = bits.isBitSet(pos: 0)
            let requeue = bits.isBitSet(pos: 1)

            return Nack(deliveryTag: deliveryTag, multiple: multiple, requeue: requeue)       
        }

        func encode(into buffer: inout ByteBuffer) throws {
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


    public enum ID {
        case select
        case selectOk

        init?(rawValue: UInt16) {
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

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self)
        }

        switch ID(rawValue: rawID) {
        case .select:
            guard let noWait = buffer.readInteger(as: UInt8.self) else {
                throw DecodeError.value(type: UInt8.self)
            }        
            return .select(noWait: noWait == 1)    
        case .selectOk:
            return .selectOk
        default:
            throw DecodeError.unsupported(value: rawID)
        }
    }

    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
        case .select(let noWait):
            buffer.writeInteger(ID.select.rawValue)
            buffer.writeInteger(noWait ? UInt8(1): UInt8(0))
        case .selectOk:
            buffer.writeInteger(ID.selectOk.rawValue)
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


    public enum ID {
        case select
        case selectOk
        case commit
        case commitOk
        case rollback
        case rollbackOk

        init?(rawValue: UInt16) {
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

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self)
        }

        switch ID(rawValue: rawID) {
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
        default:
            throw DecodeError.unsupported(value: rawID)
        }
    }

    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
        case .select:
            buffer.writeInteger(ID.select.rawValue)
        case .selectOk:
            buffer.writeInteger(ID.selectOk.rawValue)
        case .commit:
            buffer.writeInteger(ID.commit.rawValue)
        case .commitOk:
            buffer.writeInteger(ID.commitOk.rawValue)
        case .rollback: 
            buffer.writeInteger(ID.rollback.rawValue)
        case .rollbackOk:
            buffer.writeInteger(ID.rollbackOk.rawValue)
        }
    }
}
