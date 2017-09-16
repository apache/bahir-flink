# Flink InfluxDB Connector

This connector provides a sink that can send data to [InfluxDB](https://www.influxdata.com/). To use this connector, add the
following dependency to your project:

    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-influxdb_2.11</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>

*Version Compatibility*: This module is compatible with InfluxDB 1.3.x   
*Requirements*: Java 1.8+

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/linking.html).
 
## Installing InfluxDB
Follow the instructions from the [InfluxDB download page](https://portal.influxdata.com/downloads#influxdb).
  
## Examples

### JAVA API

    DataStream<InfluxDBPoint> dataStream = ...
    InfluxDBConfig influxDBConfig = InfluxDBConfig.builder(String host, String username, String password, String dbName)
    dataStream.addSink(new InfluxDBSink(influxDBConfig));


See end-to-end examples at [InfluxDB Examples](https://github.com/apache/bahir-flink/tree/master/flink-connector-influxdb/examples)

    