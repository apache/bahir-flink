package org.apache.flink.streaming.connectors.redis.common;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_CLUSTER;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_COMMAND;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_KEY_TTL;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
}
