# Flink Akka connector

This connector provides a sink to [Akka](http://akka.io/) source actors in an ActorSystem.
To use this connector, add the following dependency to your project:


    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-akka_2.11</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>
    
*Version Compatibility*: This module is compatible with Akka 2.0+.
    
## Configuration
    
The configurations for the Receiver Actor System in Flink Akka connector can be created using the standard typesafe `Config (com.typesafe.config.Config)` object.
    
To enable acknowledgements, the custom configuration `akka.remote.auto-ack` can be used.

The user can set any of the default configurations allowed by Akka as well as custom configurations allowed by the connector.
   
A sample configuration can be defined as follows:
    
    String configFile = getClass().getClassLoader()
          .getResource("feeder_actor.conf").getFile();
    Config config = ConfigFactory.parseFile(new File(configFile));    
    
## Message Types
    
There are 3 different kind of message types which the receiver Actor in flink akka connector can receive.
    
- message containing `Iterable<Object>` data
   
- message containing generic `Object` data
   
- message containing generic `Object` data and a `Timestamp` value passed as `Tuple2<Object, Long>`.   