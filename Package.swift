// swift-tools-version: 5.8
import PackageDescription

let package = Package(
    name: "rabbitmq-nio",
    products: [
        .library(name: "AMQPProtocol", targets: ["AMQPProtocol"]),
        .library(name: "AMQPClient", targets: ["AMQPClient"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.48.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.23.0"),
        .package(url: "https://github.com/apple/swift-collections.git", .upToNextMajor(from: "1.0.0")),
    ],
    targets: [
        .target(
            name: "AMQPProtocol",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
            ],
            swiftSettings: [
                SwiftSetting.unsafeFlags(["-Xfrontend", "-strict-concurrency=complete"]),
                .enableUpcomingFeature("StrictConcurrency"),
            ]
        ),
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
            swiftSettings: [
                SwiftSetting.unsafeFlags(["-Xfrontend", "-strict-concurrency=complete"]),
                .enableUpcomingFeature("StrictConcurrency"),
            ]
        ),
        .testTarget(
            name: "AMQPClientTests",
            dependencies: ["AMQPClient"],
            swiftSettings: [
                SwiftSetting.unsafeFlags(["-Xfrontend", "-strict-concurrency=complete"]),
                .enableUpcomingFeature("StrictConcurrency"),
            ]
        ),
    ]
)
