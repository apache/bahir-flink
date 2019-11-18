package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

public class DecrByTtlMapper extends RowRedisMapper {

    public DecrByTtlMapper() {
        super();
    }

    public DecrByTtlMapper(int ttl) {
        super(ttl, RedisCommand.DESCRBY_TTL);
    }


}