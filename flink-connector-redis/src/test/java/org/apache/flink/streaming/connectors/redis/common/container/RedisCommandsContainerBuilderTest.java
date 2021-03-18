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
