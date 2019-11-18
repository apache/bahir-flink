package org.apache.flink.streaming.connectors.redis.common.hanlder;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface RedisHanlder extends Serializable {

    Map<String, String> requiredContext();
    default List<String> supportProperties() throws Exception {
        return Collections.emptyList();
    }

}
