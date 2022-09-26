enum DecodeError: Error {
    case value(type: Any.Type, amqpType: Character? = nil, inner: Error? = nil)
    case dictionary(DictionaryError)
    case array(ArrayError)
    case unsupported(value: Any)
    case unsupported(amqpType: Character)

    enum DictionaryError {
        case size
        case key
        case value(key: String, inner: Error)
    }

    enum ArrayError {
        case size
        case value(inner: Error)
    }
}



enum EncodeError: Error {
    case value(type: Any.Type, inner: Error? = nil)
    case dictionary(DictionaryError)
    case array(ArrayError)
    case unsupported(value: Any)

    enum DictionaryError {
        case value(key: String, inner: Error)
    }

    enum ArrayError {
        case value(inner: Error)
    }
}
