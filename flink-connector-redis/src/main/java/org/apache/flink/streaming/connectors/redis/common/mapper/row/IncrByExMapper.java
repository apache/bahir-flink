package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

public class IncrByTtlMapper extends RowRedisMapper {

    public IncrByTtlMapper() {
        super();
    }

    public IncrByTtlMapper(int ttl) {
        super(ttl, RedisCommand.INCRBY_TTL);
    }
}
