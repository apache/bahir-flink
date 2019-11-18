package org.apache.flink.streaming.connectors.redis.common.mapper.row;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;

/**
 * @author Ameng .
 * SET operation redis mapper.
 */
public class SetMapper extends RowRedisMapper {

    public SetMapper() {
        super(RedisCommand.SET);
    }

}
