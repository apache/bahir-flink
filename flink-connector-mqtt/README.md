# Flink Mqtt Connector

This connector provides a source and sink to [MQTT](https://mqtt.org/)™
To use this connector, add the following dependency to your project:

    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-mqtt_2.11</artifactId>
      <version>1.1-SNAPSHOT</version>
    </dependency>

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/linking.html).

The source class is called `MqttSource`, and the sink is `MqttSink`.
