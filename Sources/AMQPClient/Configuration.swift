import NIOSSL
import NIO

public enum Configuration {
    case tls(TLSConfiguration?, sniServerName: String?, Server)
    case plain(Server)

    public struct Server {
        let host: String
        let port: Int
        let user: String
        let password: String
        let vhost: String
        let timeout: TimeAmount

        public init(host: String = "localhost", port: Int = 5672, user: String = "guest", password: String = "guest", vhost: String = "/", timeout: TimeAmount = TimeAmount.seconds(60)) {
            self.host = host
            self.port = port
            self.user = user
            self.password = password
            self.vhost = vhost
            self.timeout = timeout
        }
    }
}
