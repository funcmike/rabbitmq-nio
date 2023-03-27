import XCTest
@testable import AMQPClient

final class AMQPChannelsTest: XCTestCase {
    
    func testReservesChannelIds() async throws {
        var channels = AMQPChannels(channelMax: 10)
        
        let c1 = channels.reserveNext()
        let c2 = channels.reserveNext()
        
        XCTAssertEqual(c1, 1)
        XCTAssertEqual(c2, 2)
    }
    
    func testReusesChannelIdAfterRemoving() async throws {
        var channels = AMQPChannels(channelMax: 10)
        
        let c1 = channels.reserveNext()
        channels.remove(id: c1!)
        let c2 = channels.reserveNext()
        
        XCTAssertEqual(c1, 1)
        XCTAssertEqual(c2, 1)
    }
    
    func testDoesNotGoPastChannelMax() async throws {
        var channels = AMQPChannels(channelMax: 1)
        
        let c1 = channels.reserveNext()
        let c2 = channels.reserveNext()
        
        XCTAssertEqual(c1, 1)
        XCTAssertEqual(c2, nil)
    }
}
