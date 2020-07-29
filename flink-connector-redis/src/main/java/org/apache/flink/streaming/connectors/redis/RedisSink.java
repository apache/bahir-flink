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

package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * A sink that delivers data to a Redis channel using the Jedis client.
 * <p> The sink takes two arguments {@link FlinkJedisConfigBase} and {@link RedisMapper}.
 * <p> When {@link FlinkJedisPoolConfig} is passed as the first argument,
 * the sink will create connection using {@link redis.clients.jedis.JedisPool}. Please use this when
 * you want to connect to a single Redis server.
 * <p> When {@link FlinkJedisSentinelConfig} is passed as the first argument, the sink will create connection
 * using {@link redis.clients.jedis.JedisSentinelPool}. Please use this when you want to connect to Sentinel.
 * <p> Please use {@link FlinkJedisClusterConfig} as the first argument if you want to connect to
 * a Redis Cluster.
 *
 * <p>Example:
 *
 * <pre>
 *{@code
 *public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {
 *
 *    private RedisCommand redisCommand;
 *
 *    public RedisExampleMapper(RedisCommand redisCommand){
 *        this.redisCommand = redisCommand;
 *    }
 *    public RedisCommandDescription getCommandDescription() {
 *        return new RedisCommandDescription(redisCommand, REDIS_ADDITIONAL_KEY);
 *    }
 *    public String getKeyFromData(Tuple2<String, String> data) {
 *        return data.f0;
 *    }
 *    public String getValueFromData(Tuple2<String, String> data) {
 *        return data.f1;
 *    }
 *}
 *JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder()
 *    .setHost(REDIS_HOST).setPort(REDIS_PORT).build();
 *new RedisSink<String>(jedisPoolConfig, new RedisExampleMapper(RedisCommand.LPUSH));
 *}</pre>
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class RedisSink<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

    /**
     * This additional key needed for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET}.
     * Other {@link RedisDataType} works only with two variable i.e. name of the list and value to be added.
     * But for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET} we need three variables.
     * <p>For {@link RedisDataType#HASH} we need hash name, hash key and element.
     * {@code additionalKey} used as hash name for {@link RedisDataType#HASH}
     * <p>For {@link RedisDataType#SORTED_SET} we need set name, the element and it's score.
     * {@code additionalKey} used as set name for {@link RedisDataType#SORTED_SET}
     */
    private String additionalKey;

    /**
     * This additional time to live is optional for {@link RedisDataType#HASH} and required for {@link RedisCommand#SETEX}.
     * It sets the TTL for a specific key.
     */
    private Integer additionalTTL;

    private RedisMapper<IN> redisSinkMapper;
    private RedisCommand redisCommand;

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    /**
     * Creates a new {@link RedisSink} that connects to the Redis server.
     *
     * @param flinkJedisConfigBase The configuration of {@link FlinkJedisConfigBase}
     * @param redisSinkMapper This is used to generate Redis command and key value from incoming elements.
     */
    public RedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper) {
        Objects.requireNonNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");
        Objects.requireNonNull(redisSinkMapper.getCommandDescription(), "Redis Mapper data type description can not be null");

        this.flinkJedisConfigBase = flinkJedisConfigBase;

        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription = redisSinkMapper.getCommandDescription();

        this.redisCommand = redisCommandDescription.getCommand();
        this.additionalTTL = redisCommandDescription.getAdditionalTTL();
        this.additionalKey = redisCommandDescription.getAdditionalKey();
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Redis channel.
     * Depending on the specified Redis data type (see {@link RedisDataType}),
     * a different Redis command will be applied.
     * Available commands are RPUSH, LPUSH, SADD, PUBLISH, SET, SETEX, PFADD, HSET, ZADD.
     *
     * @param input The incoming data
     */
    @Override
    public void invoke(IN input, Context context) throws Exception {
        String key = redisSinkMapper.getKeyFromData(input);
        String value = redisSinkMapper.getValueFromData(input);

        Optional<String> optAdditionalKey = redisSinkMapper.getAdditionalKey(input);
        Optional<Integer> optAdditionalTTL = redisSinkMapper.getAdditionalTTL(input);

        switch (redisCommand) {
            case RPUSH:
                this.redisCommandsContainer.rpush(key, value);
                break;
            case LPUSH:
                this.redisCommandsContainer.lpush(key, value);
                break;
            case SADD:
                this.redisCommandsContainer.sadd(key, value);
                break;
            case SET:
                this.redisCommandsContainer.set(key, value);
                break;
            case SETEX:
                this.redisCommandsContainer.setex(key, value, optAdditionalTTL.orElse(this.additionalTTL));
                break;
            case PFADD:
                this.redisCommandsContainer.pfadd(key, value);
                break;
            case PUBLISH:
                this.redisCommandsContainer.publish(key, value);
                break;
            case ZADD:
                this.redisCommandsContainer.zadd(optAdditionalKey.orElse(this.additionalKey), value, key);
                break;
            case ZINCRBY:
                this.redisCommandsContainer.zincrBy(optAdditionalKey.orElse(this.additionalKey), value, key);
                break;
            case ZREM:
                this.redisCommandsContainer.zrem(optAdditionalKey.orElse(this.additionalKey), key);
                break;
            case HSET:
                this.redisCommandsContainer.hset(optAdditionalKey.orElse(this.additionalKey), key, value,
                        optAdditionalTTL.orElse(this.additionalTTL));
                break;
            case HINCRBY:
                this.redisCommandsContainer.hincrBy(optAdditionalKey.orElse(this.additionalKey), key, Long.valueOf(value), optAdditionalTTL.orElse(this.additionalTTL));
                break;
            case INCRBY:
                this.redisCommandsContainer.incrBy(key, Long.valueOf(value));
                break;
            case INCRBY_EX:
                this.redisCommandsContainer.incrByEx(key, Long.valueOf(value), optAdditionalTTL.orElse(this.additionalTTL));
                break;
            case DECRBY:
                this.redisCommandsContainer.decrBy(key, Long.valueOf(value));
                break;
            case DESCRBY_EX:
                this.redisCommandsContainer.decrByEx(key, Long.valueOf(value), optAdditionalTTL.orElse(this.additionalTTL));
                break;
            default:
                throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
        }
    }

    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     *
     * @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and jedisSentinelConfig are all null
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
            this.redisCommandsContainer.open();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    /**
     * Closes commands container.
     * @throws IOException if command container is unable to close.
     */
    @Override
    public void close() throws IOException {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }
}
