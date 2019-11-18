package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * @author Ameng .
 * SADD  operation redis mapper.
 */
public class SAddMapper extends RowRedisMapper {

    public SAddMapper() {
        super(RedisCommand.SADD);
    }

}
