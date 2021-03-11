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

package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.*;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Schema.*;

/**
 * redis table sink factory for creare redis table sink.
 */
public class RedisTableSinkFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {

    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
        return new RedisTableSink(properties);
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(CONNECTOR_TYPE, REDIS);
        return require;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        properties.add(REDIS_MODE);
        properties.add(REDIS_COMMAND);
        properties.add(REDIS_NODES);
        properties.add(REDIS_MASTER_NAME);
        properties.add(REDIS_CLUSTER_PASSWORD);
        properties.add(REDIS_SENTINEL);
        properties.add(REDIS_KEY_TTL);
        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);
        // format wildcard
        properties.add(FORMAT + ".*");
        properties.add(CONNECTOR + ".*");
        return properties;
    }
}
