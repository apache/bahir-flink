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

# Flink Akka Connector

This connector provides a sink to [Akka](http://akka.io/) source actors in an ActorSystem.
To use this connector, add the following dependency to your project:

    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-akka_2.11</artifactId>
      <version>1.1-SNAPSHOT</version>
    </dependency>
    
*Version Compatibility*: This module is compatible with Akka 2.0+.

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/linking.html).
    
## Configuration
    
The configurations for the Receiver Actor System in Flink Akka connector can be created using the standard typesafe `Config (com.typesafe.config.Config)` object.
    
To enable acknowledgements, the custom configuration `akka.remote.auto-ack` can be used.

The user can set any of the default configurations allowed by Akka as well as custom configurations allowed by the connector.
   
A sample configuration can be defined as follows:
    
    String configFile = getClass().getClassLoader()
          .getResource("feeder_actor.conf").getFile();
    Config config = ConfigFactory.parseFile(new File(configFile));    
    
## Message Types
    
There are 3 different kind of message types which the receiver Actor in Flink Akka connector can receive.
    
- message containing `Iterable<Object>` data
   
- message containing generic `Object` data
   
- message containing generic `Object` data and a `Timestamp` value passed as `Tuple2<Object, Long>`.
