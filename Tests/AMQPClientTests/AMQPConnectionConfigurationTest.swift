import XCTest
import AMQPClient
@testable import AMQPClient

@available(macOS 13.0, *)
final class AMQPClientConfigurationTest: XCTestCase {
    func testFromPlainURL() throws {
        let config = try AMQPConnectionConfiguration(url: "amqp://myUser:myPass@myHost:1234/myVHost?some=junk")
        guard case .plain = config.connection else { return XCTFail("plain expected") }

        XCTAssertEqual(config.server.host, "myHost")
        XCTAssertEqual(config.server.port, 1234)
        XCTAssertEqual(config.server.user, "myUser")
        XCTAssertEqual(config.server.password, "myPass")
        XCTAssertEqual(config.server.vhost, "myVHost")
    }
    
    func testFromPlainURLWithDefaults() throws {
        let config = try AMQPConnectionConfiguration(url: "amqp://myHost")
        guard case .plain = config.connection else { return XCTFail("plain expected") }

        XCTAssertEqual(config.server.host, "myHost")
        XCTAssertEqual(config.server.port, 5672)
        XCTAssertEqual(config.server.user, "guest")
        XCTAssertEqual(config.server.password, "guest")
        XCTAssertEqual(config.server.vhost, "/")
    }
    
    func testFromTlsURLWithOnlyUserAndPassword() throws {
        let config = try AMQPConnectionConfiguration(url: "amqps://top:secret@")

        guard case .tls(_, _) = config.connection else { return XCTFail("tls expected") }

        XCTAssertEqual(config.server.host, "localhost")
        XCTAssertEqual(config.server.port, 5671)
        XCTAssertEqual(config.server.user, "top")
        XCTAssertEqual(config.server.password, "secret")
        XCTAssertEqual(config.server.vhost, "/")
    }
    
    func testWithEmpties() throws {
        let c = try AMQPConnectionConfiguration(url: "amqp://@:/")

        XCTAssertEqual(c.server.host, "localhost")
        XCTAssertEqual(c.server.port, 5672)
        XCTAssertEqual(c.server.user, "")
        XCTAssertEqual(c.server.password, "")
        XCTAssertEqual(c.server.vhost, "")
    }
    
    func testWithUrlEncodedVHost() throws {
        let c = try AMQPConnectionConfiguration(url: "amqps://hello@host:1234/%2f")

        XCTAssertEqual(c.server.host, "host")
        XCTAssertEqual(c.server.port, 1234)
        XCTAssertEqual(c.server.user, "hello")
        XCTAssertEqual(c.server.password, "")
        XCTAssertEqual(c.server.vhost, "/")
    }
    
    func testWithUrlEncodedEverything() throws {
        let c = try AMQPConnectionConfiguration(url: "amqp://user%61:%61pass@ho%61st:10000/v%2fhost")

        XCTAssertEqual(c.server.host, "hoast")
        XCTAssertEqual(c.server.port, 10000)
        XCTAssertEqual(c.server.user, "usera")
        XCTAssertEqual(c.server.password, "apass")
        XCTAssertEqual(c.server.vhost, "v/host")
    }
}
