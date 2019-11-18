package org.apache.flink.streaming.connectors.redis.common.config.handler;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_MASTER_NAME;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_SENTINEL;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.SENTINELS_INFO;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.SENTINELS_PASSWORD;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;

public class FlinkJedisSentinelConfigHandler implements FlinkJedisConfigHandler {

    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(Map<String, String> properties) {
        String masterName = properties.computeIfAbsent(REDIS_MASTER_NAME, null);
        String sentinelsInfo = properties.computeIfAbsent(SENTINELS_INFO, null);
        Objects.requireNonNull(masterName, "master should not be null in sentinel mode");
        Objects.requireNonNull(sentinelsInfo, "sentinels should not be null in sentinel mode");
        Set<String> sentinels = Arrays.asList(sentinelsInfo.split(","))
                .stream().collect(Collectors.toSet());
        String sentinelsPassword = properties.computeIfAbsent(SENTINELS_PASSWORD, null);
        if (sentinelsPassword != null && sentinelsPassword.trim().isEmpty()) {
            sentinelsPassword = null;
        }
        FlinkJedisSentinelConfig flinkJedisSentinelConfig = new FlinkJedisSentinelConfig.Builder()
                .setMasterName(masterName).setSentinels(sentinels).setPassword(sentinelsPassword)
                .build();
        return flinkJedisSentinelConfig;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_SENTINEL);
        return require;
    }

    public FlinkJedisSentinelConfigHandler() {

    }
}
