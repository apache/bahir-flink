#   Flink Netty Connector

This connector provide tcp source and http source for receiving push data, implemented by [Netty](http://netty.io). 

##  Data Flow

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

There are three component:

*   User System - where the data streaming come from
*   Third Register Service - receive `Flink Netty Source`'s register request(ip and port)
*   Flink Netty Source - Netty Server for receiving pushed streaming data from `User System`


##   Maven Dependency
To use this connector, add the following dependency to your project:

```
<dependency>
  <groupId>org.apache.bahir</groupId>
  <artifactId>flink-connector-netty_2.11</artifactId>
  <version>1.0</version>
</dependency>
```

##  Usage

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
>tryPort:   try to use this point, if this point is used then try a new port
>callbackUrl:   register connector's ip and port to a `Third Register Service`

##  full example 

There are two example for get start:

*   [StreamSqlExample](https://github.com/apache/bahir-flink/blob/master/flink-connector-netty/src/test/scala/org/apache/flink/streaming/connectors/netty/example/StreamSqlExample.scala)
*   [TcpSourceExample](https://github.com/apache/bahir-flink/blob/master/flink-connector-netty/src/test/scala/org/apache/flink/streaming/connectors/netty/example/TcpSourceExample.scala)

