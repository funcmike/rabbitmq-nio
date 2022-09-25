enum DecodeError: Error {
    case frame(FrameError)
    case table(TableError)
    case array(ArrayError)
    case field(FieldError)
}

enum EncodeError: Error {
    case frame(FrameError)
    case table(TableError)
    case array(ArrayError)
    case field(FieldError)
}

enum FieldError: Error {
    case type
    case value(Type: UInt8)
    case unsupported(Type: UInt8)
    case unsupported(Value: Any)
}

enum FrameError: Error {
    case type(Value: UInt8?)
    case channelId
    case size
    case method(MethodError)
}

enum MethodError: Error {
    case id(Value: UInt16?)
    case connection(ConnectionError)
}

enum ConnectionError: Error {
    case id(Value: UInt16?)
    case connectionStart(ConnectionStartError)
    case connectionStartOk(ConnectionStartOkError)
    case table(TableError)
}

enum TableError: Error {
    case size
    case key
    case value(Key: String, Inner: Error)
}

enum ArrayError {
    case size
    case value(Inner: Error)
}

enum ConnectionStartError: Error {
    case versionMajor
    case versionMinor
    case serverProperties(Inner: Error)
    case mechanisms
    case locales
}

enum ConnectionStartOkError: Error {
    case clientProperties(Inner: Error)
    case mechanism
    case response
    case locale
}