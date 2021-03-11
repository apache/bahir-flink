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

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_CLUSTER;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_KEY_TTL;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_NODES;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_CLUSTER_PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.connectors.redis.common.mapper.row.SetExMapper;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class RedisHandlerTest extends AbstractTestBase {
    public static final Map<String, String> properties = new HashMap<>();

    @BeforeClass
    public static void setUp() {
        properties.put(REDIS_MODE, REDIS_CLUSTER);
        properties.put(REDIS_COMMAND, RedisCommand.SETEX.name());
        properties.put(REDIS_NODES, "localhost:8080");
        properties.put(REDIS_KEY_TTL, "1000");
        properties.put(REDIS_CLUSTER_PASSWORD, "test-pwd");
    }

    @Test
    public void testRedisMapper() {
        RedisMapper redisMapper = RedisHandlerServices.findRedisHandler(RedisMapperHandler.class, properties)
                .createRedisMapper(properties);
        SetExMapper expectedMapper = new SetExMapper(1000);
        assertEquals(redisMapper, expectedMapper);
    }

    @Test
    public void testFlinkJedisConfigHandler() {
        FlinkJedisConfigBase flinkJedisConfigBase =  RedisHandlerServices
                .findRedisHandler(FlinkJedisConfigHandler.class, properties)
                .createFlinkJedisConfig(properties);
        assertTrue(flinkJedisConfigBase instanceof FlinkJedisClusterConfig);
    }

    @Test
    public void testFlinkJedisConfigHasPassword() {
        FlinkJedisConfigBase flinkJedisConfigBase =  RedisHandlerServices
                .findRedisHandler(FlinkJedisConfigHandler.class, properties)
                .createFlinkJedisConfig(properties);
        assertNotNull(flinkJedisConfigBase.getPassword());
        assertEquals("test-pwd", flinkJedisConfigBase.getPassword());
    }
}
