// swift-tools-version: 5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "rabbitmq-nio",
    products: [
        .library(name: "AMQPProtocol", targets: ["AMQPProtocol"]),
        .library(name: "AMQPClient", targets: ["AMQPClient"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.43.1"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.22.1"),
        .package(url: "https://github.com/apple/swift-collections.git",  .upToNextMajor(from: "1.0.0"))
    ],
    targets: [
        .target(
            name: "AMQPProtocol",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
            ]),
        .target(
            name: "AMQPClient",
            dependencies: [
                "AMQPProtocol",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "Collections", package: "swift-collections"),
            ],
            linkerSettings: [.unsafeFlags(["-Xlinker", "-lm"])]),
        .testTarget(
            name: "AMQPClientTests",
            dependencies: ["AMQPClient"]),
    ]
)
