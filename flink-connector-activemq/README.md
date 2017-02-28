# Flink ActiveMQ connector


This connector provides a source and sink to [Apache ActiveMQ](http://activemq.apache.org/)™
To use this connector, add the following dependency to your project:


    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-activemq_2.11</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>

*Version Compatibility*: This module is compatible with ActiveMQ 5.14.0.

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.


The source class is called `AMQSource`, the sink is `AMQSink`.
