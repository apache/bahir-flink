package org.apache.flink.streaming.connectors.activemq.table;/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.activemq.AMQSinkConfig;
import org.apache.flink.streaming.connectors.activemq.DestinationType;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;



public class ActiveMQDynamicTableFactory
        implements  DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "activemq";


    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        String brokerUrl = tableOptions.get(ActiveMQOptions.BROKER_URL);
        String destinationName = tableOptions.get(ActiveMQOptions.DESTINATION_NAME);
        String destinationType = tableOptions.get(ActiveMQOptions.DESTINATION_TYPE);
        Boolean persistentDelivery = tableOptions.get(ActiveMQOptions.PERSISTENT_DELIVERY);
        EncodingFormat<SerializationSchema<RowData>> encodingFormat = getEncodingFormat(helper).get();
        return new ActiveMQDynamicSink(brokerUrl,destinationName,destinationType,persistentDelivery,encodingFormat,context.getCatalogTable().getSchema().toPhysicalRowDataType());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        String brokerUrl = tableOptions.get(ActiveMQOptions.BROKER_URL);
        String destinationName = tableOptions.get(ActiveMQOptions.DESTINATION_NAME);
        String destinationType = tableOptions.get(ActiveMQOptions.DESTINATION_TYPE);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat = getDecodingFormat(helper).get();
        return new ActiveMQDynamicSource(brokerUrl,destinationName,destinationType,decodingFormat,context.getCatalogTable().getSchema().toPhysicalRowDataType());
    }
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ActiveMQOptions.DESTINATION_NAME);
        options.add(ActiveMQOptions.BROKER_URL);
        options.add(ActiveMQOptions.DESTINATION_TYPE);
        options.add(ActiveMQOptions.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getDecodingFormat(FactoryUtil.TableFactoryHelper helper) {
        Optional<DecodingFormat<DeserializationSchema<RowData>>> decodingFormat = helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, ActiveMQOptions.FORMAT);
        return decodingFormat;
    }

    private static Optional<EncodingFormat<SerializationSchema<RowData>>> getEncodingFormat(FactoryUtil.TableFactoryHelper helper) {
        Optional<EncodingFormat<SerializationSchema<RowData>>> encodingFormat = helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, ActiveMQOptions.FORMAT);
        return encodingFormat;
    }

}
