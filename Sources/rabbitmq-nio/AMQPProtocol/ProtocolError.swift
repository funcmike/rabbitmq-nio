enum ProtocolError: Error {
    case decode(type: Any.Type? = nil, value: Any? = nil, kind: Field.Kind? = nil, message: String? = nil, context: Any.Type, inner: Error? = nil)
    case encode(value: Any? = nil, type: Any.Type? = nil, message: String? = nil, context: Any.Type, inner: Error? = nil)
    case invalid(value: Any,  message: String? = nil, context: Any.Type)
    case unsupported(value: Any, context: Any.Type)
}