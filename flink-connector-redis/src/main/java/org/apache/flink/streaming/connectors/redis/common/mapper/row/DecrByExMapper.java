package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * @author Ameng .
 * decrease with expire operation redis mapper.
 */
public class DecrByExMapper extends RowRedisMapper {

    public DecrByExMapper() {
        super(RedisCommand.DESCRBY_EX);
    }

    public DecrByExMapper(Integer ttl) {
        super(ttl, RedisCommand.DESCRBY_EX);
    }

}
