import Foundation

internal extension Date {
    @usableFromInline
    func toUnixEpoch() -> Int64 {
        return Int64(self.timeIntervalSince1970*1000)
    }
}
