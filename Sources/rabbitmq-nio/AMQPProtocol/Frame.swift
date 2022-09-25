import NIOCore

public typealias ChannelId = UInt16

public typealias Table = [String:Any]

protocol PayloadDecodable {
    static func decode(from buffer: inout ByteBuffer) throws -> Self
}

protocol PayloadEncodable {
    static func encode(to buffer: inout ByteBuffer) throws
}

public enum Frame: PayloadDecodable {
    static func decode(from buffer: inout ByteBuffer) throws -> Frame {
        guard let rawType = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.frame
        }

        guard let channelId = buffer.readInteger(as: ChannelId.self) else {
            throw DecodeError.frame
        }

        guard let size = buffer.readInteger(as: UInt32.self) else {
            throw DecodeError.frame
        }

        switch Type(rawValue: rawType) {
        case .method:
            return .method(channelId, try! Method.decode(from: &buffer))
        default:
            throw DecodeError.frame
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
    }
}

public enum Method: PayloadDecodable {
    static func decode(from buffer: inout ByteBuffer) throws -> Method {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.method
        }
    
        switch ID(rawValue: rawID) {
            case .connection:
                return .connection(try! Connection.decode(from: &buffer))
            default:
                throw DecodeError.method
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
    }
}

public enum Connection: PayloadDecodable {
    static func decode(from buffer: inout ByteBuffer) throws -> Connection {
        guard let rawID = buffer.readInteger(as: UInt16.self) else {
            throw DecodeError.connection
        }
    
        switch ID(rawValue: rawID) {
            case .start:
                return .start(try! ConnectionStart.decode(from: &buffer))
            default:
                throw DecodeError.connection 
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
    }
}

public struct ConnectionStart: PayloadDecodable {
    static func decode(from buffer: inout NIOCore.ByteBuffer) throws -> ConnectionStart {
        guard let versionMajor = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.connectionStart
        }

        guard let versionMinor = buffer.readInteger(as: UInt8.self) else {
            throw DecodeError.connectionStart
        }

        guard let (serverProperties, _) = readDictionary(from: &buffer) else {
            throw DecodeError.table
        }

        guard let (mechanisms, _) = readLongStr(from: &buffer) else {
            throw DecodeError.connectionStart
        }

        guard  let (locales, _) = readLongStr(from: &buffer)  else {
            throw DecodeError.connectionStart
        }

        return ConnectionStart(versionMajor: versionMajor, versionMinor: versionMinor, serverProperties: serverProperties, mechanisms: mechanisms, locales: locales)
    }

    var versionMajor: UInt8
    var versionMinor: UInt8
    var serverProperties: Table
    var mechanisms: String
    var locales: String
}

public struct ConnnectionStartOk {
    var clientProperties: Table
    var mechanism: String
    var response: String
    var locale: String
}