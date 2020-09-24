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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * redis table sink to use redis in sql env.
 */
public class RedisTableSink implements UpsertStreamTableSink<Row> {

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisMapper redisMapper;
    private TableSchema tableSchema;
    private String[] keyFields;
    private boolean isAppendOnly;
    private Map<String, String> properties = null;


    public RedisTableSink(Map<String, String> properties) {
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties should not be null");
        redisMapper = RedisHandlerServices
                .findRedisHandler(RedisMapperHandler.class, properties)
                .createRedisMapper(properties);
        flinkJedisConfigBase = RedisHandlerServices
                .findRedisHandler(FlinkJedisConfigHandler.class, properties)
                .createFlinkJedisConfig(properties);
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        tableSchema = descriptorProperties.getTableSchema(SCHEMA);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream.addSink(new RedisSink(flinkJedisConfigBase, redisMapper))
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames()));
    }


    @Override
    public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
        return new RedisTableSink(getProperties());
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public void setKeyFields(String[] keys) {
        this.keyFields = keys;
    }

    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) {
        this.isAppendOnly = isAppendOnly;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return tableSchema.toRowType();
    }
}
