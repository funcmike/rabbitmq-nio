//===----------------------------------------------------------------------===//
//
// This source file is part of the RabbitMQNIO project
//
// Copyright (c) 2022 Krzysztof Majk
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

public enum ProtocolError: Error, Sendable {
    case decode(type: Any.Type? = nil, kind: Field.Kind? = nil, message: String? = nil, context: Any.Type, inner: Error? = nil)
    case encode(value: Sendable? = nil, type: Any.Type? = nil, message: String? = nil, context: Any.Type, inner: Error? = nil)
    case invalid(value: Sendable,  message: String? = nil, context: Any.Type)
    case incomplete(type: Any.Type? = nil, expected: UInt32, got: Int)
}
