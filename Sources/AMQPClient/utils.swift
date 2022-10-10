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

// Shamelessly taken from https://github.com/apple/swift-distributed-actors/blob/3483d424eb66d54d772b27622c2e2978514fc87e/Sources/DistributedActorsActor/utils.swift
/**
 * `undefined()` pretends to be able to produce a value of any type `T` which can
 * be very useful whilst writing a program. It happens that you need a value
 * (which can be a function as well) of a certain type but you can't produce it
 * just yet. However, you can always temporarily replace it by `undefined()`.
 *
 * Inspired by Haskell's
 * [undefined](http://hackage.haskell.org/package/base-4.7.0.2/docs/Prelude.html#v:undefined).
 *
 * Invoking `undefined()` will crash your program.
 *
 * Some examples:
 *
 *  - `let x : String = undefined()`
 *  - `let f : String -> Int? = undefined("string to optional int function")`
 *  - `return undefined() /* in any function */`
 *  - `let x : String = (undefined() as Int -> String)(42)`
 *  - ...
 *
 * What a crash looks like:
 *
 * `fatal error: undefined: main.swift, line 131`
 *
 * Originally from: Johannes Weiss (MIT licensed) https://github.com/weissi/swift-undefined
 */

public func undefined<T>(hint: String = "", file: StaticString = #file, line: UInt = #line) -> T {
	let message = hint == "" ? "" : ": \(hint)"
	fatalError("undefined \(T.self)\(message)", file: file, line: line)
}

public func TODO<T>(_ hint: String, file: StaticString = #file, line: UInt = #line) -> T {
	return undefined(hint: "TODO: \(hint)", file: file, line: line)
}

public func FIXME<T>(_ hint: String, file: StaticString = #file, line: UInt = #line) -> T {
	return undefined(hint: "FIXME: \(hint)", file: file, line: line)
}