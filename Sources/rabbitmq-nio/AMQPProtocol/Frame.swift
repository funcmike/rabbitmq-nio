import NIOCore

public typealias ChannelId = UInt16

public typealias Table = [String:Any]

protocol PayloadDecodable {
    static func decode(from buffer: inout ByteBuffer) throws -> Self
}

protocol PayloadEncodable {
    func encode(into buffer: inout ByteBuffer) throws
}

public enum Frame: PayloadDecodable, PayloadEncodable {
    case method(ChannelId, Method)

    enum `Type` {
        case method

        init?(rawValue: UInt8)
        {
            switch rawValue {
            case UInt8(1):
                self = .method
            default:
                return nil
            }
        }

        var rawValue: UInt8 {
            switch self {
                case .method:
                    return UInt8(1)
            }
        }
    }

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawType = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.value(type: UInt8.self)
        }

        guard let channelId = buffer.readInteger(as: ChannelId.self) else {
            throw DecodeError.value(type: ChannelId.self)
        }

        // TODO(funcmike): use this later for Body frame
        guard let size = buffer.readInteger(as: UInt32.self) else {
            throw DecodeError.value(type: UInt32.self)
        }

        let frame: Frame
        
        switch Type(rawValue: rawType) {
        case .method:
            frame = Self.method(channelId, try! Method.decode(from: &buffer))
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
            case .method(let channelId, let method):
                buffer.writeInteger(`Type`.method.rawValue)

                buffer.writeInteger(channelId)
                
                let startIndex = buffer.writerIndex

                buffer.writeInteger(UInt32(0)) // placeholder for size
                                
                try! method.encode(into: &buffer)

                let size = UInt32(buffer.writerIndex - startIndex - 4)

                buffer.setInteger(size, at: startIndex)

                buffer.writeInteger(UInt8(206))
        }
    }
}

public enum Method: PayloadDecodable, PayloadEncodable {
    case connection(Connection)

    enum ID {
        case connection

        init?(rawValue: UInt16)
        {
            switch rawValue {
            case UInt16(10):
                self = .connection
            default:
                return nil
            }
        }

        var rawValue: UInt16 {
            switch self {
                case .connection:
                    return UInt16(10)
            }
        }
    }

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self)
        }
    
        switch ID(rawValue: rawID) {
            case .connection:
                return .connection(try! Connection.decode(from: &buffer))
            default:
                throw DecodeError.unsupported(value: rawID)
        }
    }

    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
            case .connection(let connection):
                buffer.writeInteger(ID.connection.rawValue)
                try! connection.encode(into: &buffer)
                return
        }
    }
}

public enum Connection: PayloadDecodable, PayloadEncodable {
    case start(ConnectionStart)
    case startOk(ConnnectionStartOk)
    case tune(Tune)
    case tuneOk(TuneOk)
    case open(Open)
    case openOk(OpenOk)
    case close
    case closeOk
    case blocked
    case unblocked


    public enum ID {
        case start
        case startOk
        case tune
        case tuneOk
        case open
        case openOk

        init?(rawValue: UInt16)
        {
            switch rawValue {
            case UInt16(10):
                self = .start
            case UInt16(11):
                self = .startOk
            case UInt16(30):
                self = .tune
            case UInt16(31):
                self = .tuneOk
            case UInt16(40):
                self = .open
            case UInt16(41):
                self = .openOk
            default:
                return nil
            }
        }

        var rawValue: UInt16 {
            switch self {
                case .start:
                    return UInt16(10)
                case .startOk:
                    return UInt16(11)
                case .tune:
                    return UInt16(30)
                case .tuneOk:
                    return UInt16(31)
                case .open:
                    return UInt16(40)
                case .openOk:
                    return UInt16(41)
            }
        }
    }

    static func decode(from buffer: inout ByteBuffer) throws -> Self {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.value(type: UInt16.self)
        }
    
        switch ID(rawValue: rawID) {
            case .start:
                return .start(try! ConnectionStart.decode(from: &buffer))
            case .startOk:
                return .startOk(try! ConnnectionStartOk.decode(from: &buffer))
            case .tune:
                return .tune(try! Tune.decode(from: &buffer))
            case .tuneOk:
                return .tuneOk(try! TuneOk.decode(from: &buffer))
            case .open:
                return .open(try! Open.decode(from: &buffer))
            case .openOk:
                return .openOk(try! OpenOk.decode(from: &buffer))
            default:
                throw DecodeError.unsupported(value: rawID)
        }
    }

    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
            case .start(let connectionStart):
                buffer.writeInteger(ID.start.rawValue)
                try! connectionStart.encode(into: &buffer)
            case .startOk(let connectionStartOk):
                buffer.writeInteger(ID.startOk.rawValue)
                try! connectionStartOk.encode(into: &buffer)
            case .tune(let tune):
                buffer.writeInteger(ID.tune.rawValue)
                try! tune.encode(into: &buffer)
            case .tuneOk(let tuneOk):
                buffer.writeInteger(ID.tuneOk.rawValue)
                try! tuneOk.encode(into: &buffer)
            case .open(let open): 
                buffer.writeInteger(ID.open.rawValue)
                try! open.encode(into: &buffer)
            case .openOk(let openOk): 
                buffer.writeInteger(ID.openOk.rawValue)
                try! openOk.encode(into: &buffer)
            case .close:
                return TODO("implement close")
            case .closeOk: 
                return TODO("implement closeOk")
            case .blocked:
                return TODO("implement blocked")
            case .unblocked: 
                return TODO("implement unblocked") 
        }
    }
}

public struct ConnectionStart: PayloadDecodable {
    let versionMajor: UInt8
    let versionMinor: UInt8
    let serverProperties: Table
    let mechanisms: String
    let locales: String

    init(versionMajor: UInt8 = 0, versionMinor: UInt8 = 9, serverProperties: Table = [
                           "capabilities": [
                             "publisher_confirms":           true,
                             "exchange_exchange_bindings":   true,
                             "basic.nack":                   true,
                             "per_consumer_qos":             true,
                             "authentication_failure_close": true,
                             "consumer_cancel_notify":       true,
                             "connection.blocked":           true,
                           ]
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

        let serverProperties: Table

        do {
            (serverProperties, _) = try readDictionary(from: &buffer)
        } catch let error as DecodeError {
            throw DecodeError.value(type: Table.self, inner: error)
        }

        guard let (mechanisms, _) = readLongStr(from: &buffer) else {
            throw DecodeError.value(type: String.self)
        }

        guard  let (locales, _) = readLongStr(from: &buffer)  else {
            throw DecodeError.value(type: String.self)
        }

        return ConnectionStart(versionMajor: versionMajor, versionMinor: versionMinor, serverProperties: serverProperties, mechanisms: mechanisms, locales: locales)
    }

    func encode(into buffer: inout ByteBuffer) throws {
        buffer.writeInteger(versionMajor)
        buffer.writeInteger(versionMinor)

        do {
            try writeDictionary(values: serverProperties, into: &buffer)
        } catch let error as EncodeError {
            throw EncodeError.value(type: Table.self, inner: error)
        }
        
        writeLongStr(value: mechanisms, into: &buffer)
        writeLongStr(value: locales, into: &buffer)
    }
}

public struct ConnnectionStartOk: PayloadDecodable, PayloadEncodable {
    let clientProperties: Table
    let mechanism: String
    let response: String
    let locale: String

    static func decode(from buffer: inout NIOCore.ByteBuffer) throws -> Self {
        let clientProperties: Table
 
        do {
            (clientProperties, _) = try readDictionary(from: &buffer)
        } catch let error as DecodeError {
            throw DecodeError.value(type: Table.self, inner: error)
        }

        guard let (mechanism, _) = readShortStr(from: &buffer) else {
            throw DecodeError.value(type: String.self)
        }

        guard  let (response, _) = readLongStr(from: &buffer)  else {
            throw DecodeError.value(type: String.self)
        }

        guard  let (locale, _) = readShortStr(from: &buffer)  else {
            throw DecodeError.value(type: String.self)
        }

        return ConnnectionStartOk(clientProperties: clientProperties, mechanism: mechanism, response: response, locale: locale)
    }

    func encode(into buffer: inout ByteBuffer) throws {
        do {
            try writeDictionary(values: clientProperties, into: &buffer)
        } catch let error as EncodeError {
            throw EncodeError.value(type: Table.self, inner: error)
        }

        writeShortStr(value: mechanism, into: &buffer)
        writeLongStr(value: response, into: &buffer)
        writeShortStr(value: locale, into: &buffer)
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
    
    static func decode(from buffer: inout NIOCore.ByteBuffer) throws -> Self {
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

    func encode(into buffer: inout NIOCore.ByteBuffer) throws {
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
    
    static func decode(from buffer: inout NIOCore.ByteBuffer) throws -> Self {
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

    func encode(into buffer: inout NIOCore.ByteBuffer) throws {
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
    
    static func decode(from buffer: inout NIOCore.ByteBuffer) throws -> Self {
        guard let (vhost, _) = readShortStr(from: &buffer) else {
            throw DecodeError.value(type: String.self)
        }

        guard let (reserved1, _) = readShortStr(from: &buffer) else {
            throw DecodeError.value(type: String.self)
        }

        guard let reserved2 = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.value(type: UInt8.self)
        }

        return Open(vhost: vhost, reserved1: reserved1, reserved2: reserved2 > 0 ? true : false)       
    }

    func encode(into buffer: inout NIOCore.ByteBuffer) throws {
        writeShortStr(value: vhost, into: &buffer)
        writeShortStr(value: reserved1, into: &buffer)
        buffer.writeInteger(reserved2 ? UInt8(1) : UInt8(0))
    }
}


public struct OpenOk: PayloadDecodable, PayloadEncodable {
    let reserved1: String

    init(reserved1: String = "")
    {
        self.reserved1 = reserved1
    }
    
    static func decode(from buffer: inout NIOCore.ByteBuffer) throws -> Self {
        guard let (reserved1, _) = readShortStr(from: &buffer) else {
            throw DecodeError.value(type: String.self)
        }

        return OpenOk(reserved1: reserved1)       
    }

    func encode(into buffer: inout NIOCore.ByteBuffer) throws {
        writeShortStr(value: reserved1, into: &buffer)
    }
}