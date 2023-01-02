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

import Foundation

internal extension Date {
    @usableFromInline
    func toUnixEpoch() -> Int64 {
        return Int64(self.timeIntervalSince1970*1000)
    }
}
