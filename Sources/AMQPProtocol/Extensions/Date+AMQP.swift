//===----------------------------------------------------------------------===//
//
// This source file is part of the RabbitMQNIO project
//
// Copyright (c) 2023 RabbitMQNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import struct Foundation.Date

internal extension Date {
    @usableFromInline
    func toUnixEpoch() -> Int64 {
        return Int64(self.timeIntervalSince1970*1000)
    }
}
