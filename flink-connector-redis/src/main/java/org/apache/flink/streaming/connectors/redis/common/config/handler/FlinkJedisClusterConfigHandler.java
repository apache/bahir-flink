package org.apache.flink.streaming.connectors.redis.common.config.handler;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_CLUSTER;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_NODES;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.util.Preconditions;

/**
 * @author Ameng .
 * jedis cluster config handler to find and create jedis cluster config use meta.
 */
public class FlinkJedisClusterConfigHandler implements FlinkJedisConfigHandler {

    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(Map<String, String> properties) {
        Preconditions.checkArgument(properties.containsKey(REDIS_NODES), "nodes should not be null in cluster mode");
        String nodesInfo = properties.get(REDIS_NODES);
        Set<InetSocketAddress> nodes = Arrays.asList(nodesInfo.split(",")).stream().map(r -> {
            String[] arr = r.split(":");
            return new InetSocketAddress(arr[0].trim(), Integer.parseInt(arr[1].trim()));
        }).collect(Collectors.toSet());
        return new FlinkJedisClusterConfig.Builder()
                .setNodes(nodes).build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_CLUSTER);
        return require;
    }

    public FlinkJedisClusterConfigHandler() {
    }
}
