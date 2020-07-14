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

package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/**
 * base row redis mapper implement.
 */
public abstract class RowRedisMapper implements RedisMapper<Tuple2<Boolean, Row>>, RedisMapperHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowRedisMapper.class);

    private Integer ttl;

    private RedisCommand redisCommand;

    public Integer getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    public void setRedisCommand(RedisCommand redisCommand) {
        this.redisCommand = redisCommand;
    }

    public RowRedisMapper() {
    }

    public RowRedisMapper(int ttl, RedisCommand redisCommand) {
        this.ttl = ttl;
        this.redisCommand = redisCommand;
    }

    public RowRedisMapper(RedisCommand redisCommand) {
        this.redisCommand = redisCommand;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        if (ttl != null) {
            return new RedisCommandDescription(redisCommand, ttl);
        }
        return new RedisCommandDescription(redisCommand);
    }

    @Override
    public String getKeyFromData(Tuple2<Boolean, Row> data) {
        return data.f1.getField(0).toString();
    }

    @Override
    public String getValueFromData(Tuple2<Boolean, Row> data) {
        return data.f1.getField(1).toString();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_COMMAND, getRedisCommand().name());
        return require;
    }

    @Override
    public boolean equals(Object obj) {
        RedisCommand redisCommand = ((RowRedisMapper) obj).redisCommand;
        return this.redisCommand == redisCommand;
    }

    @Override
    public Optional<Integer> getAdditionalTTL(Tuple2<Boolean, Row> data) {
        return Optional.ofNullable(getTtl());
    }
}
