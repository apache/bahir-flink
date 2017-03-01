# Flink ActiveMQ Connector

This connector provides a source and sink to [Apache ActiveMQ](http://activemq.apache.org/)™
To use this connector, add the following dependency to your project:

    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-activemq_2.11</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>

*Version Compatibility*: This module is compatible with ActiveMQ 5.14.0.

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/linking.html).

The source class is called `AMQSource`, and the sink is `AMQSink`.
