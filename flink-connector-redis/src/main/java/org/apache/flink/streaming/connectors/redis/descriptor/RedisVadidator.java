package org.apache.flink.streaming.connectors.redis.descriptor;

/**
 * @author Ameng .
 * redis validator for validate redis descriptor.
 */
public class RedisVadidator {
    public static final String REDIS = "redis";
    public static final String REDIS_MODE = "redis-mode";
    public static final String REDIS_NODES = "cluster-nodes";
    public static final String REDIS_CLUSTER = "cluster";
    public static final String REDIS_SENTINEL = "sentinel";
    public static final String REDIS_COMMAND = "command";
    public static final String REDIS_MASTER_NAME = "master.name";
    public static final String SENTINELS_INFO = "sentinels.info";
    public static final String SENTINELS_PASSWORD = "sentinels.password";
    public static final String REDIS_KEY_TTL = "key.ttl";

}
