package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * decrease operation redis mapper.
 */
public class DecrByMapper extends RowRedisMapper {

    public DecrByMapper() {
        super(RedisCommand.DECRBY);
    }

}
