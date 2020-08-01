# Flink Redis Connector

This connector provides a Sink that can write to [Redis](http://redis.io/) and also can publish data 
to [Redis PubSub](http://redis.io/topics/pubsub). To use this connector, add the
following dependency to your project:

    <dependency>
      <groupId>org.apache.bahir</groupId>
      <artifactId>flink-connector-redis_2.11</artifactId>
      <version>1.1-SNAPSHOT</version>
    </dependency>

*Version Compatibility*: This module is compatible with Redis 2.8.5.

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/linking.html).

## Installing Redis

Follow the instructions from the [Redis download page](http://redis.io/download).


## Redis Sink

A class providing an interface for sending data to Redis.
The sink can use three different methods for communicating with different type of Redis environments:

1. Single Redis Server
2. Redis Cluster
3. Redis Sentinel

This code shows how to create a sink that communicate to a single redis server:

**Java:**


    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
    FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

    DataStream<String> stream = ...;
    stream.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper());



**Scala:**

    class RedisExampleMapper extends RedisMapper[(String, String)]{
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME")
      }

      override def getKeyFromData(data: (String, String)): String = data._1

      override def getValueFromData(data: (String, String)): String = data._2
    }
    val conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build()
    stream.addSink(new RedisSink[(String, String)](conf, new RedisExampleMapper))



This example code does the same, but for Redis Cluster:

**Java:**

    FlinkJedisPoolConfig conf = new FlinkJedisClusterConfig.Builder()
        .setNodes(new HashSet<InetSocketAddress>(Arrays.asList(new InetSocketAddress(5601)))).build();

    DataStream<String> stream = ...;
    stream.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper());

**Scala:**


    val conf = new FlinkJedisClusterConfig.Builder().setNodes(...).build()
    stream.addSink(new RedisSink[(String, String)](conf, new RedisExampleMapper))


This example shows when the Redis environment is with Sentinels:

Java:

    FlinkJedisSentinelConfig conf = new FlinkJedisSentinelConfig.Builder()
        .setMasterName("master").setSentinels(...).build();

    DataStream<String> stream = ...;
    stream.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper());
 

Scala:

    val conf = new FlinkJedisSentinelConfig.Builder().setMasterName("master").setSentinels(...).build()
    stream.addSink(new RedisSink[(String, String)](conf, new RedisExampleMapper))


This section gives a description of all the available data types and what Redis command used for that.

<table class="table table-bordered" style="width: 75%">
    <thead>
        <tr>
          <th class="text-center" style="width: 20%">Data Type</th>
          <th class="text-center" style="width: 25%">Redis Command [Sink]</th>
        </tr>
      </thead>
      <tbody>
        <tr>
            <td>HASH</td><td><a href="http://redis.io/commands/hset">HSET</a></td>
        </tr>
        <tr>
            <td>LIST</td><td>
                <a href="http://redis.io/commands/rpush">RPUSH</a>,
                <a href="http://redis.io/commands/lpush">LPUSH</a>
            </td>
        </tr>
        <tr>
            <td>SET</td><td><a href="http://redis.io/commands/sadd">SADD</a></td>
        </tr>
        <tr>
            <td>PUBSUB</td><td><a href="http://redis.io/commands/publish">PUBLISH</a></td>
        </tr>
        <tr>
            <td>STRING</td><td><a href="http://redis.io/commands/set">SET</a></td>
        </tr>
        <tr>
            <td>HYPER_LOG_LOG</td><td><a href="http://redis.io/commands/pfadd">PFADD</a></td>
        </tr>
        <tr>
            <td>SORTED_SET</td><td><a href="http://redis.io/commands/zadd">ZADD</a></td>
        </tr>
        <tr>
            <td>SORTED_SET</td><td><a href="http://redis.io/commands/zrem">ZREM</a></td>
        </tr>
      </tbody>
</table>
