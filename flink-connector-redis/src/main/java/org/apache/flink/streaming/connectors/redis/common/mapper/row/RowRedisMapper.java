package org.apache.flink.streaming.connectors.redis.mapper;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisVadidator.REDIS_COMMAND;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RowRedisMapper implements RedisMapper<Row>, RedisMapperHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowRedisMapper.class);

    private int ttl;

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public RowRedisMapper() {
    }

    public RowRedisMapper(int ttl) {
        this.ttl = ttl;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return null;
    }

    @Override
    public String getKeyFromData(Row data) {
        return data.getField(0).toString();
    }

    @Override
    public String getValueFromData(Row data) {
        return data.getField(1).toString();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_COMMAND, getCommandDescription().getCommand().name());
        return require;
    }

}
