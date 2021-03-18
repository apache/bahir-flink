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
package org.apache.flink.streaming.connectors.redis.common.container;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;

public class RedisCommandsContainerBuilderTest extends AbstractTestBase {

    @Test
    public void testNotTestWhileIdle() {
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("host").setPort(0).setDatabase(0).build();
        GenericObjectPoolConfig genericObjectPoolConfig = RedisCommandsContainerBuilder.getGenericObjectPoolConfig(flinkJedisPoolConfig);
        assertFalse(genericObjectPoolConfig.getTestWhileIdle());
        assertEqualConfig(flinkJedisPoolConfig, genericObjectPoolConfig);
    }

    @Test
    public void testTestWhileIdle() {
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("host").setPort(0).setDatabase(0).setTestWhileIdle(true).build();
        GenericObjectPoolConfig genericObjectPoolConfig = RedisCommandsContainerBuilder.getGenericObjectPoolConfig(flinkJedisPoolConfig);
        assertTrue(genericObjectPoolConfig.getTestWhileIdle());
        assertEqualConfig(flinkJedisPoolConfig, genericObjectPoolConfig);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        assertEquals(genericObjectPoolConfig.getMinEvictableIdleTimeMillis(), jedisPoolConfig.getMinEvictableIdleTimeMillis());
        assertEquals(genericObjectPoolConfig.getTimeBetweenEvictionRunsMillis(), jedisPoolConfig.getTimeBetweenEvictionRunsMillis());
        assertEquals(genericObjectPoolConfig.getNumTestsPerEvictionRun(), jedisPoolConfig.getNumTestsPerEvictionRun());
    }

    private void assertEqualConfig(FlinkJedisPoolConfig flinkJedisPoolConfig, GenericObjectPoolConfig genericObjectPoolConfig) {
        assertEquals(genericObjectPoolConfig.getMaxIdle(), flinkJedisPoolConfig.getMaxIdle());
        assertEquals(genericObjectPoolConfig.getMinIdle(), flinkJedisPoolConfig.getMinIdle());
        assertEquals(genericObjectPoolConfig.getMaxTotal(), flinkJedisPoolConfig.getMaxTotal());
        assertEquals(genericObjectPoolConfig.getTestWhileIdle(), flinkJedisPoolConfig.getTestWhileIdle());
        assertEquals(genericObjectPoolConfig.getTestOnBorrow(), flinkJedisPoolConfig.getTestOnBorrow());
        assertEquals(genericObjectPoolConfig.getTestOnReturn(), flinkJedisPoolConfig.getTestOnReturn());
    }
}
