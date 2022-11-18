import XCTest
import AMQPClient
@testable import AMQPClient

@available(macOS 13.0, *)
final class AMQPClientConfigurationTest: XCTestCase {

    func testFromPlainURL() throws {
        guard case let .plain(config) = try AMQPClientConfiguration(url: "amqp://myUser:myPass@myHost:1234/myVHost?some=junk") else { return XCTFail("plain expected") }

        XCTAssertEqual(config.host, "myHost")
        XCTAssertEqual(config.port, 1234)
        XCTAssertEqual(config.user, "myUser")
        XCTAssertEqual(config.password, "myPass")
        XCTAssertEqual(config.vhost, "myVHost")
    }
    
    func testFromPlainURLWithDefaults() throws {
        guard case let .plain(config) = try AMQPClientConfiguration(url: "amqp://myHost") else { return XCTFail("plain expected") }

        XCTAssertEqual(config.host, "myHost")
        XCTAssertEqual(config.port, 5672)
        XCTAssertEqual(config.user, "guest")
        XCTAssertEqual(config.password, "guest")
        XCTAssertEqual(config.vhost, "/")
    }
    
    func testFromTlsURLWithOnlyUserAndPassword() throws {
        guard case let .tls(_, _, config) = try AMQPClientConfiguration(url: "amqps://top:secret@") else { return XCTFail("tls expected") }

        XCTAssertEqual(config.host, "localhost")
        XCTAssertEqual(config.port, 5671)
        XCTAssertEqual(config.user, "top")
        XCTAssertEqual(config.password, "secret")
        XCTAssertEqual(config.vhost, "/")
    }
    
    func testWithEmpties() throws {
        let c = try AMQPClientConfiguration(url: "amqp://@:/")

        XCTAssertEqual(c.server.host, "localhost")
        XCTAssertEqual(c.server.port, 5672)
        XCTAssertEqual(c.server.user, "")
        XCTAssertEqual(c.server.password, "")
        XCTAssertEqual(c.server.vhost, "")
    }
    
    func testWithUrlEncodedVHost() throws {
        let c = try AMQPClientConfiguration(url: "amqps://hello@host:1234/%2f")

        XCTAssertEqual(c.server.host, "host")
        XCTAssertEqual(c.server.port, 1234)
        XCTAssertEqual(c.server.user, "hello")
        XCTAssertEqual(c.server.password, "")
        XCTAssertEqual(c.server.vhost, "/")
    }
    
    func testWithUrlEncodedEverything() throws {
        let c = try AMQPClientConfiguration(url: "amqp://user%61:%61pass@ho%61st:10000/v%2fhost")

        XCTAssertEqual(c.server.host, "hoast")
        XCTAssertEqual(c.server.port, 10000)
        XCTAssertEqual(c.server.user, "usera")
        XCTAssertEqual(c.server.password, "apass")
        XCTAssertEqual(c.server.vhost, "v/host")
    }
}
