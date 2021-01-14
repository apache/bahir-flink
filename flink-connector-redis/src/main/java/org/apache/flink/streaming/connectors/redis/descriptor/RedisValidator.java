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

/**
 * redis validator for validate redis descriptor.
 */
public class RedisValidator {
    public static final String REDIS = "redis";
    public static final String REDIS_MODE = "redis-mode";
    public static final String REDIS_NODES = "cluster-nodes";
    public static final String REDIS_CLUSTER = "cluster";
    public static final String REDIS_SENTINEL = "sentinel";
    public static final String REDIS_COMMAND = "command";
    public static final String REDIS_MASTER_NAME = "master.name";
    public static final String SENTINELS_INFO = "sentinels.info";
    public static final String SENTINELS_PASSWORD = "sentinels.password";
    public static final String REDIS_CLUSTER_PASSWORD = "cluster.password";
    public static final String REDIS_KEY_TTL = "key.ttl";

}
