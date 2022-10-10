# RabbitMQNIO

A Swift implementation of AMQP 0.9.1 protocol: decoder + encoder (AMQPProtocol) and non-blocking client (AMQPClient).

Heavy inspired by projects: amq-protocol (https://github.com/cloudamqp/amq-protocol.cr) and amqp-client (https://github.com/cloudamqp/amqp-client.cr).

Swift-NIO related code is based on other NIO projects like:
* https://github.com/wapor/postgres-nio
* https://github.com/swift-server-community/mqtt-nio
* https://github.com/Mordil/RediStack

## State of the project

**!!! WARNING !!!** <br>
This project is in very early stage and still under heavy development so everything can change in the near future. <br>
Please don't use it until first release! <br>
**!!! WARNING !!!**

AMQPProtocol library currently should cover all of AMQP 0.9.1 specification.

AMQPClient is in prototyping phase: work on architecture using NIO and tests of AMQP basic operations:
* establishing connection with heartbeats
* channel creation
* basic publish
