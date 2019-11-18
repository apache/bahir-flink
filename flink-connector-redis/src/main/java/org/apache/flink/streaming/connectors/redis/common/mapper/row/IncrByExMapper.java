package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * @author Ameng .
 * Delta plus with expire key operation redis mapper.
 */
public class IncrByExMapper extends RowRedisMapper {

    public IncrByExMapper() {
        super(RedisCommand.INCRBY_EX);
    }

    public IncrByExMapper(Integer ttl) {
        super(ttl, RedisCommand.INCRBY_EX);
    }

}
