<!--
{% comment %}
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
--> 

# Flink InfluxDB Connector

This connector provides a sink that can send data to [InfluxDB](https://www.influxdata.com/). To use this connector, add the
following dependency to your project:

    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-influxdb_2.11</artifactId>
      <version>1.1-SNAPSHOT</version>
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


