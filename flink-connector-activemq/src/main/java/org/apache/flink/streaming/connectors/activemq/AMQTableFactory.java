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

package org.apache.flink.streaming.connectors.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.connectors.activemq.table.descriptors.AMQValidator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.activemq.table.descriptors.AMQValidator.*;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.*;
import static org.apache.flink.table.descriptors.Schema.*;

/**
 * Factory for creating configured instances of {@link AMQTableSource} or {@link AMQTableSink}.
 */
public class AMQTableFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Row> {

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
        String destinationType = descriptorProperties.getString(CONNECTOR_DESTINATION_TYPE);
        String destinationName = descriptorProperties.getString(CONNECTOR_DESTINATION_NAME);
        Boolean persistent = descriptorProperties.getOptionalBoolean(CONNECTOR_PERSISTENT).orElse(false);
        ActiveMQConnectionFactory factory = getActiveMQConnectionFactory(descriptorProperties);

        final AMQSinkConfig<Row> config = new AMQSinkConfig.AMQSinkConfigBuilder<Row>()
            .setConnectionFactory(factory)
            .setSerializationSchema(new JsonRowSerializationSchema.Builder(tableSchema.toRowType()).build())
            .setDestinationName(destinationName)
            .setDestinationType(DestinationType.valueOf(destinationType.toUpperCase()))
            .setPersistentDelivery(persistent)
            .build();

        return new AMQTableSink(config, tableSchema);
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
        String destinationType = descriptorProperties.getString(CONNECTOR_DESTINATION_TYPE);
        String destinationName = descriptorProperties.getString(CONNECTOR_DESTINATION_NAME);
        ActiveMQConnectionFactory factory = getActiveMQConnectionFactory(descriptorProperties);

        final AMQSourceConfig<Row> config = new AMQSourceConfig.AMQSourceConfigBuilder<Row>()
            .setConnectionFactory(factory)
            .setDeserializationSchema(new JsonRowDeserializationSchema.Builder(tableSchema.toRowType()).build())
            .setDestinationName(destinationName)
            .setDestinationType(DestinationType.valueOf(destinationType.toUpperCase()))
            .build();

        return new AMQTableSource(config, tableSchema);
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_AMQ);
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        properties.add(CONNECTOR_BROKER_URL);
        properties.add(CONNECTOR_USERNAME);
        properties.add(CONNECTOR_PASSWORD);
        properties.add(CONNECTOR_DESTINATION_TYPE);
        properties.add(CONNECTOR_DESTINATION_NAME);
        properties.add(CONNECTOR_PERSISTENT);

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

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        new AMQValidator().validate(descriptorProperties);
        return descriptorProperties;
    }

    private ActiveMQConnectionFactory getActiveMQConnectionFactory(DescriptorProperties descriptorProperties){
        String brokerURL = descriptorProperties.getString(CONNECTOR_BROKER_URL);
        String username = descriptorProperties.getOptionalString(CONNECTOR_USERNAME).orElse(null);
        String password = descriptorProperties.getOptionalString(CONNECTOR_PASSWORD).orElse(null);
        return new ActiveMQConnectionFactory(username, password, brokerURL);
    }
}
