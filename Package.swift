// swift-tools-version: 5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "rabbitmq-nio",
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0")
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .executableTarget(
            name: "rabbitmq-nio",
            dependencies: [
                .product(name: "NIO", package: "swift-nio"),
            ]),
        .testTarget(
            name: "rabbitmq-nioTests",
            dependencies: ["rabbitmq-nio"]),
    ]
)
