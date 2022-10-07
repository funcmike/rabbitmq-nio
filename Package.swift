// swift-tools-version: 5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "rabbitmq-nio",
    products: [
        .library(name: "AMQPProtocol", targets: ["AMQPProtocol"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.43.1"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.22.1")
    ],
    targets: [
        .target(name: "AMQPProtocol"),
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .executableTarget(
            name: "AMQPClient",
            dependencies: [
                "AMQPProtocol",
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
            ]),
        .testTarget(
            name: "rabbitmq-nioTests",
            dependencies: ["AMQPProtocol"]),
    ]
)
