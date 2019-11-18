package org.apache.flink.streaming.connectors.redis.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;

public class SetExMapper extends RowRedisMapper {

    public SetExMapper() {
    }

    public SetExMapper(Integer ttl) {
        super(ttl);
    }

    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SETEX, getTtl());
    }
}
