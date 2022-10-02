enum DecodeError: Error {
    case value(type: Any.Type? = nil, amqpType: Character? = nil, message: String? = nil, inner: Error? = nil)
    case unsupported(value: Any, message: String? = nil)
}

enum EncodeError: Error {
    case value(type: Any.Type? = nil, value: Any? = nil, message: String? = nil, inner: Error? = nil)
    case unsupported(value: Any, message: String? = nil)
}
