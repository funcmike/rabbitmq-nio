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
        }
    }

    static func decode(from buffer: inout ByteBuffer) throws -> Frame {
        guard let rawType = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.frame(.type(Value: nil))
        }

        guard let channelId = buffer.readInteger(as: ChannelId.self) else {
            throw DecodeError.frame(.channelId)
        }

        // TODO(funcmike): use this later for Body frame
        guard let size = buffer.readInteger(as: UInt32.self) else {
            throw DecodeError.frame(.size)
        }

        switch Type(rawValue: rawType) {
        case .method:
            return .method(channelId, try! Method.decode(from: &buffer))
        default:
            throw DecodeError.frame(.type(Value: rawType))
        }
    }

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
}

public enum Method: PayloadDecodable, PayloadEncodable {
    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
            case .connection(let connection):
                buffer.writeInteger(ID.connection.rawValue)
                try! connection.encode(into: &buffer)
                return
        }
    }

    static func decode(from buffer: inout ByteBuffer) throws -> Method {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.frame(.method(.id(Value: nil)))
        }
    
        switch ID(rawValue: rawID) {
            case .connection:
                return .connection(try! Connection.decode(from: &buffer))
            default:
                throw DecodeError.frame(.method(.id(Value: rawID)))
        }
    }

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
}

public enum Connection: PayloadDecodable, PayloadEncodable {
    func encode(into buffer: inout ByteBuffer) throws {
        switch self {
            case .startOk(let connectionStart):
                buffer.writeInteger(ID.startOk.rawValue)
                try! connectionStart.encode(into: &buffer)
            default:
                return TODO("implement start")
        }
    }

    static func decode(from buffer: inout ByteBuffer) throws -> Connection {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
                throw DecodeError.frame(.method(.connection(.id(Value: nil))))
        }
    
        switch ID(rawValue: rawID) {
            case .start:
                return .start(try! ConnectionStart.decode(from: &buffer))
            case .startOk:
                return .startOk(try! ConnnectionStartOk.decode(from: &buffer))
            default:
                throw DecodeError.frame(.method(.connection(.id(Value: rawID))))
        }
    }

    case start(ConnectionStart)
    case startOk(ConnnectionStartOk)
    case tune
    case tuneOk
    case open
    case openOk
    case close
    case closeOk
    case blocked
    case unblocked


    public enum ID {
        case start
        case startOk

        init?(rawValue: UInt16)
        {
            switch rawValue {
            case UInt16(10):
                self = .start
            case UInt16(11):
                self = .startOk
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
            }
        }
    }
}

public struct ConnectionStart: PayloadDecodable {
    static func decode(from buffer: inout ByteBuffer) throws -> ConnectionStart {
        guard let versionMajor = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.frame(.method(.connection(.connectionStart(.versionMajor))))
        }

        guard let versionMinor = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.frame(.method(.connection(.connectionStart(.versionMinor))))
        }

        let serverProperties: Table

        do {
            (serverProperties, _) = try readDictionary(from: &buffer)
        } catch let error as DecodeError {
               throw DecodeError.frame(.method(.connection(.connectionStart(.serverProperties(Inner: error)))))
        }

        guard let (mechanisms, _) = readLongStr(from: &buffer) else {
            throw DecodeError.frame(.method(.connection(.connectionStart(.mechanisms))))
        }

        guard  let (locales, _) = readLongStr(from: &buffer)  else {
            throw DecodeError.frame(.method(.connection(.connectionStart(.locales))))
        }

        return ConnectionStart(versionMajor: versionMajor, versionMinor: versionMinor, serverProperties: serverProperties, mechanisms: mechanisms, locales: locales)
    }

    var versionMajor: UInt8
    var versionMinor: UInt8
    var serverProperties: Table
    var mechanisms: String
    var locales: String
}

public struct ConnnectionStartOk: PayloadDecodable, PayloadEncodable {
    var clientProperties: Table
    var mechanism: String
    var response: String
    var locale: String

    static func decode(from buffer: inout NIOCore.ByteBuffer) throws -> ConnnectionStartOk {
        let clientProperties: Table 
        do {
            (clientProperties, _) = try readDictionary(from: &buffer)
        } catch let error as DecodeError {
            throw DecodeError.frame(.method(.connection(.connectionStartOk(.clientProperties(Inner: error)))))
        }

        guard let (mechanism, _) = readShortStr(from: &buffer) else {
            throw DecodeError.frame(.method(.connection(.connectionStartOk(.mechanism))))
        }

        guard  let (response, _) = readLongStr(from: &buffer)  else {
            throw DecodeError.frame(.method(.connection(.connectionStartOk(.response))))
        }

        guard  let (locale, _) = readShortStr(from: &buffer)  else {
            throw DecodeError.frame(.method(.connection(.connectionStartOk(.locale))))
        }

        return ConnnectionStartOk(clientProperties: clientProperties, mechanism: mechanism, response: response, locale: locale)
    }


    func encode(into buffer: inout ByteBuffer) throws {
        try! writeDictionary(values: clientProperties, into: &buffer)
        writeShortStr(value: mechanism, into: &buffer)
        writeLongStr(value: response, into: &buffer)
        writeShortStr(value: locale, into: &buffer)
    }
}