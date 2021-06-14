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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.activemq.*;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Optional;

public class ActiveMQDynamicSource implements ScanTableSource {
    private final String brokerUrl;
    private final String destinationName;
    private final String destinationType;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType physicalDataType;

    public ActiveMQDynamicSource(String brokerUrl, String destinationName, String destinationType, DecodingFormat<DeserializationSchema<RowData>> decodingFormat,DataType physicalDataType ) {
        this.brokerUrl = brokerUrl;
        this.destinationName = destinationName;
        this.destinationType = destinationType;
        this.decodingFormat = decodingFormat;
        this.physicalDataType = physicalDataType;

    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        AMQSourceConfig<RowData> config = new AMQSourceConfig.AMQSourceConfigBuilder<RowData>()
                .setConnectionFactory(new ActiveMQConnectionFactory(brokerUrl))
                .setDestinationName(destinationName)
                .setDestinationType("topic".equals(destinationType)? DestinationType.TOPIC:DestinationType.QUEUE)
                .setDeserializationSchema(decodingFormat.createRuntimeDecoder(context,physicalDataType))
                .build();
        return SourceFunctionProvider.of(new AMQSource(config),false);
    }


    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
