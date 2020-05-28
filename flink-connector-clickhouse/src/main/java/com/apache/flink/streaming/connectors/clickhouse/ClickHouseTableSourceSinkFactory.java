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
package com.apache.flink.streaming.connectors.clickhouse;

import com.apache.flink.table.descriptors.ClickHouseValidator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.apache.flink.table.descriptors.ClickHouseValidator.*;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.DescriptorProperties.*;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.*;


/**
 * Factory for creating configured instances of {@link ClickHouseTableSink} .
 */
public class ClickHouseTableSourceSinkFactory implements StreamTableSinkFactory<Row> {

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_CLICKHOUSE);
        context.put(CONNECTOR_VERSION, "1");
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        properties.add(CONNECTOR_ADRESS);
        properties.add(CONNECTOR_DATABASE);
        properties.add(CONNECTOR_TABLE);
        properties.add(CONNECTOR_USERNAME);
        properties.add(CONNECTOR_PASSWORD);

        properties.add(CONNECTOR_COMMIT_BATCH_SIZE);
        properties.add(CONNECTOR_COMMIT_PADDING);

        properties.add(CONNECTOR_COMMIT_RETRY_ATTEMPTS);
        properties.add(CONNECTOR_COMMIT_RETRY_INTERVAL);

        properties.add(CONNECTOR_COMMIT_IGNORE_ERROR);

        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        // computed column
        properties.add(SCHEMA + ".#." + TABLE_SCHEMA_EXPR);

        // watermark
        properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_ROWTIME);
        properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_EXPR);
        properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_DATA_TYPE);

        return properties;

    }

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> map) {
        DescriptorProperties descriptorProperties = getValidatedPropertities(map);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
        final ClickHouseTableSink.Builder builder = ClickHouseTableSink.builder()
            .setSchema(schema);

        descriptorProperties.getOptionalString(CONNECTOR_ADRESS).ifPresent(builder::setAddress);
        descriptorProperties.getOptionalString(CONNECTOR_DATABASE).ifPresent(builder::setDatabase);
        descriptorProperties.getOptionalString(CONNECTOR_TABLE).ifPresent(builder::setTable);
        descriptorProperties.getOptionalString(CONNECTOR_USERNAME).ifPresent(builder::setUsername);
        descriptorProperties.getOptionalString(CONNECTOR_PASSWORD).ifPresent(builder::setPassword);
        descriptorProperties.getOptionalInt(CONNECTOR_COMMIT_BATCH_SIZE).ifPresent(builder::setBatchSize);
        descriptorProperties.getOptionalLong(CONNECTOR_COMMIT_PADDING).ifPresent(builder::setCommitPadding);
        descriptorProperties.getOptionalInt(CONNECTOR_COMMIT_RETRY_ATTEMPTS).ifPresent(builder::setRetries);
        descriptorProperties.getOptionalLong(CONNECTOR_COMMIT_RETRY_INTERVAL).ifPresent(builder::setRetryInterval);
        descriptorProperties.getOptionalBoolean(CONNECTOR_COMMIT_IGNORE_ERROR).ifPresent(builder::setIgnoreInsertError);

        return builder.builder();
    }

    private DescriptorProperties getValidatedPropertities(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        new SchemaValidator(true, false, false).validate(descriptorProperties);
        new ClickHouseValidator().validate(descriptorProperties);

        return descriptorProperties;

    }
}
