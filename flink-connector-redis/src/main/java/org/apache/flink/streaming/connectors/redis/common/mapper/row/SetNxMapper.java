package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

public class SetNxMapper extends RowRedisMapper{

    public SetNxMapper() {
        super(RedisCommand.SETNX);
    }

    public SetNxMapper(Integer ttl) {
        super(ttl, RedisCommand.SETNX);
    }
}
