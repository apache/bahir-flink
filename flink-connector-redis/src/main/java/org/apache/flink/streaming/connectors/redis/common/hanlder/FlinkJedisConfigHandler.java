package org.apache.flink.streaming.connectors.redis.common.hanlder;

import java.util.Map;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;

/**
 * @author Ameng .
 * handler to create flink jedis config.
 */
public interface FlinkJedisConfigHandler extends RedisHandler  {

    /**
     * create flink jedis config use sepecified properties.
     * @param properties used to create flink jedis config
     * @return flink jedis config
     */
    FlinkJedisConfigBase createFlinkJedisConfig(Map<String, String> properties);
}
