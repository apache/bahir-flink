/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.common.hanlder;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/***
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
