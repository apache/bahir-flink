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

package org.apache.flink.streaming.connectors.redis.descriptor;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.*;

/**
 * redis descriptor for create redis connector.
 */
public class Redis extends ConnectorDescriptorValidator {

    private final DescriptorProperties properties;

    private String mode = null;
    private String redisCommand = null;
    private Integer ttl;

    public Redis(String type, int version) {
        super();
        properties = new DescriptorProperties();
        properties.putString("connector.type", type);
        properties.putInt("connector.property-version", version);
    }

    public Redis() {
        this(REDIS, 1);
    }

    /**
     * redis operation type.
     * @param redisCommand redis operation type
     * @return this descriptor.
     */
    public Redis command(String redisCommand) {
        this.redisCommand = redisCommand;
        properties.putString(REDIS_COMMAND, redisCommand);
        return this;
    }

    /**
     * ttl for specified key.
     * @param ttl time for key.
     * @return this descriptor
     */
    public Redis ttl(Integer ttl) {
        this.ttl = ttl;
        properties.putInt(REDIS_KEY_TTL, ttl);
        return this;
    }

    /**
     * redis mode to connect a specified redis cluster
     * @param mode redis mode
     * @return this descriptor
     */
    public Redis mode(String mode) {
        this.mode = mode;
        properties.putString(REDIS_MODE, mode);
        return this;
    }

    /**
     * add properties used to connect to redis.
     * @param k specified key
     * @param v value for specified key
     * @return this descriptor
     */
    public Redis property(String k, String v) {
        properties.putString(k, v);
        return this;
    }

    /**
     * validate the necessary properties for redis descriptor.
     */
    public void validate() {
        Preconditions.checkArgument(properties.containsKey(REDIS_COMMAND), "need specified redis command");
        if (mode.equalsIgnoreCase(REDIS_CLUSTER)) {
            Preconditions.checkArgument(properties.containsKey(REDIS_NODES), "cluster mode need cluster-nodes info");
        } else if (mode.equalsIgnoreCase(REDIS_SENTINEL)) {
            Preconditions.checkArgument(properties.containsKey(REDIS_MASTER_NAME), "sentinel mode need master name");
            Preconditions.checkArgument(properties.containsKey(REDIS_SENTINEL), "sentinel mode need sentinel infos");
        }
    }
}
