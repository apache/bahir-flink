package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * @author Ameng.
 * SET with expire key operation redis mapper.
 */
public class SetExMapper extends RowRedisMapper {

    public SetExMapper() {
        super(RedisCommand.SETEX);
    }

    public SetExMapper(Integer ttl) {
        super(ttl, RedisCommand.SETEX);
    }

}
