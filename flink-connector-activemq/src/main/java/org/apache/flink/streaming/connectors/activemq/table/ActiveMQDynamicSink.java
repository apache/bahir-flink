/*
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

package org.apache.flink.streaming.connectors.activemq.table;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.activemq.AMQSink;
import org.apache.flink.streaming.connectors.activemq.AMQSinkConfig;
import org.apache.flink.streaming.connectors.activemq.DestinationType;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class ActiveMQDynamicSink implements DynamicTableSink{
    private final String brokerUrl;
    private final String destinationName;
    private final String destinationType;
    private final Boolean persistentDelivery;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final DataType physicalDataType;

    public ActiveMQDynamicSink(String brokerUrl, String destinationName, String destinationType, Boolean persistentDelivery, EncodingFormat<SerializationSchema<RowData>> encodingFormat, DataType physicalDataType) {
        this.brokerUrl = brokerUrl;
        this.destinationName = destinationName;
        this.destinationType = destinationType;
        this.persistentDelivery = persistentDelivery;
        this.encodingFormat = encodingFormat;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        AMQSinkConfig<RowData> config = new AMQSinkConfig.AMQSinkConfigBuilder<RowData>()
                .setConnectionFactory(new ActiveMQConnectionFactory(brokerUrl))
                .setDestinationName(destinationName)
                .setDestinationType("topic".equals(destinationType)?DestinationType.TOPIC:DestinationType.QUEUE)
                .setPersistentDelivery(persistentDelivery)
                .setSerializationSchema(encodingFormat.createRuntimeEncoder(context,physicalDataType))
                .build();
        return SinkFunctionProvider.of(new AMQSink(config));
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
