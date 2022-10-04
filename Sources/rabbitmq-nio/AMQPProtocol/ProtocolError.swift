enum ProtocolError: Error {
    case decode(param: String, type: Any.Type? = nil, value: Any? = nil, kind: Field.Kind? = nil, message: String? = nil, inner: Error? = nil)
    case encode(param: String, value: Any? = nil, type: Any.Type? = nil, message: String? = nil, inner: Error? = nil)
    case invalid(param: String, value: Any, message: String? = nil)
    case unsupported(param: String, value: Any)
}