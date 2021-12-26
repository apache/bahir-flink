/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.redis.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.redis.RedisITCaseBase;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Optional;

public class RedisSinkZIncrByTest extends RedisITCaseBase {

  private static final String REDIS_CLUSTER_HOSTS = "redis-01:7000,redis-02:7000,redis-03:7000";

  private static final HashSet<InetSocketAddress> NODES = new HashSet<InetSocketAddress>();

  @Before
  public void before() throws Exception {
    String[] hostList = REDIS_CLUSTER_HOSTS.split(",", -1);
    for (String host : hostList) {
      String[] parts = host.split(":", 2);
      if (parts.length > 1) {
        NODES.add(InetSocketAddress.createUnresolved(parts[0], Integer.valueOf(parts[1])));
      } else {
        throw new MalformedURLException("invalid redis hosts format");
      }
    }
  }

  @Test
  public void redisSinkTest() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    FlinkJedisClusterConfig jedisClusterConfig = new FlinkJedisClusterConfig.Builder()
        .setNodes(NODES).build();
    DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new TestSourceFunction());

    RedisSink<Tuple2<String, Integer>> redisSink = new RedisSink<>(jedisClusterConfig, new RedisTestMapper());

    source.addSink(redisSink);

    env.execute("Redis Sink Test");
  }

  @After
  public void after() throws Exception {

  }


  private static class TestSourceFunction implements SourceFunction<Tuple2<String, Integer>> {
    private static final long serialVersionUID = 1L;

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
      for (int i = 0; i < 10 && running; i++) {
        ctx.collect(new Tuple2<>("test_" + i, i));
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }


  private static class RedisTestMapper implements RedisMapper<Tuple2<String, Integer>> {
    private static final String ZINCRBY_NAME_PREFIX = "RANKING";

    @Override
    public RedisCommandDescription getCommandDescription() {
      return new RedisCommandDescription(RedisCommand.ZINCRBY, ZINCRBY_NAME_PREFIX);
    }

    @Override
    public String getKeyFromData(Tuple2<String, Integer> data) {
      return data.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, Integer> data) {
      return data.f1.toString();
    }

    @Override
    public Optional<String> getAdditionalKey(Tuple2<String, Integer> data) {
      String key = ZINCRBY_NAME_PREFIX + ":" + "TEST";
      return Optional.of(key);
    }
  }
}
