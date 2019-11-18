package org.apache.flink.streaming.connectors.redis.common.mapper.row;


import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * @author Ameng .
 * ZADD operation redis mapper.
 */
public class ZAddMapper extends RowRedisMapper {

    public ZAddMapper() {
        super(RedisCommand.ZADD);
    }
}
