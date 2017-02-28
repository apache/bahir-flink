# Flink Flume connector


This connector provides a Sink that can send data to [Apache Flume](https://flume.apache.org/)™. To use this connector, add the
following dependency to your project:


    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-flume_2.11</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>

*Version Compatibility*: This module is compatible with Flume 1.5.0.

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.


To create a `FlumeSink` instantiate the following constructor:

    FlumeSink(String host, int port, SerializationSchema<IN> schema)
    