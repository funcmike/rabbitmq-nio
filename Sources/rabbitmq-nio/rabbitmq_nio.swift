@main
public struct rabbitmq_nio {
    public private(set) var text = "Hello, World!"

    public static func main() {
        print(rabbitmq_nio().text)
        setupEventloop(arguments: CommandLine.arguments)
    }
}