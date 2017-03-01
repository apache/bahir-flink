# Flink Netty Connector

This connector provides tcp source and http source for receiving push data, implemented by [Netty](http://netty.io). 

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/linking.html).

## Data Flow

```
+-------------+      (2)    +------------------------+
| user system |    <-----   | Third Register Service |           
+-------------+             +------------------------+
       |                                ^
       | (3)                            |
       |                                |
       V                                |
+--------------------+                  |
| Flink Netty Source |  ----------------+
+--------------------+         (1)
```

There are three components:

*   User System - where the data stream is coming from
*   Third Register Service - receive `Flink Netty Source`'s register request (ip and port)
*   Flink Netty Source - Netty Server for receiving pushed streaming data from `User System`


## Maven Dependency
To use this connector, add the following dependency to your project:

```
<dependency>
  <groupId>org.apache.bahir</groupId>
  <artifactId>flink-connector-netty_2.11</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

## Usage

*Tcp Source:*

```
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.addSource(new TcpReceiverSource("msg", 7070, Some("http://localhost:9090/cb")))
```
>paramKey:  the http query param key
>tryPort:   try to use this point, if this point is used then try a new port
>callbackUrl:   register connector's ip and port to a `Third Register Service`

*Http Source:*

```
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.addSource(new TcpReceiverSource(7070, Some("http://localhost:9090/cb")))
```
>tryPort:   try to use this port, if this point is used then try a new port
>callbackUrl:   register connector's ip and port to a `Third Register Service`

## Full Example 

There are two example to get started:

*   [StreamSqlExample](https://github.com/apache/bahir-flink/blob/master/flink-connector-netty/src/test/scala/org/apache/flink/streaming/connectors/netty/example/StreamSqlExample.scala)
*   [TcpSourceExample](https://github.com/apache/bahir-flink/blob/master/flink-connector-netty/src/test/scala/org/apache/flink/streaming/connectors/netty/example/TcpSourceExample.scala)
