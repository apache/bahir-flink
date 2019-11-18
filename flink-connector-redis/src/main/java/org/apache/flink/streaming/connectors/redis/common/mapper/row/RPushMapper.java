package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * @author Ameng .
 * RPUSH  operation redis mapper.
 */
public class RPushMapper extends RowRedisMapper {

    public RPushMapper() {
        super(RedisCommand.RPUSH);
    }

}
