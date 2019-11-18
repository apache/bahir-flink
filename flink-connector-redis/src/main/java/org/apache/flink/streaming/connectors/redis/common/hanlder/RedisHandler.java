package org.apache.flink.streaming.connectors.redis.common.hanlder;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/***
 * @author Ameng.
 * redis handler to create redis mapper and flink jedis config.
 */
public interface RedisHandler extends Serializable {

    /**
     * require context for spi to find this redis handler.
     * @return properties to find correct redis handler.
     */
    Map<String, String> requiredContext();

    /**
     * suppport properties used for this redis handler.
     * @return support properties list
     * @throws Exception
     */
    default List<String> supportProperties() throws Exception {
        return Collections.emptyList();
    }

}
