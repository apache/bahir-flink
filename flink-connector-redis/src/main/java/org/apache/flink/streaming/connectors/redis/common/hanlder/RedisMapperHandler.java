package org.apache.flink.streaming.connectors.redis.mapper;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_KEY_TTL;

import java.lang.reflect.Constructor;
import java.util.Map;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface RedisMapperHandler extends RedisHandler {

    Logger LOGGER = LoggerFactory.getLogger(RedisMapperHandler.class);

    default RedisHandler create(Map<String, String> properties) {
        String ttl = properties.get(REDIS_KEY_TTL);
        try {
            Class redisMapper = Class.forName(this.getClass().getCanonicalName());

            if (ttl == null) {
                return (RedisHandler) redisMapper.newInstance();
            }
            Constructor c = redisMapper.getConstructor(Integer.class);
            return (RedisHandler) c.newInstance(Integer.parseInt(ttl));
        } catch (Exception e) {
            LOGGER.error("create redis mapper failed", e);
            throw new RuntimeException(e);
        }
    }


}
