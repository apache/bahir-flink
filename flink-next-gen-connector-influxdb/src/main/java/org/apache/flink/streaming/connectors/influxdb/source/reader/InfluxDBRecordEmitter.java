/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.influxdb.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.streaming.connectors.influxdb.source.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.InfluxDBDataPointDeserializer;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;

public class InfluxDBRecordEmitter<T> implements RecordEmitter<DataPoint, T, InfluxDBSplit> {
    private final InfluxDBDataPointDeserializer<T> dataPointDeserializer;

    public InfluxDBRecordEmitter(final InfluxDBDataPointDeserializer<T> dataPointDeserializer) {
        this.dataPointDeserializer = dataPointDeserializer;
    }

    @Override
    public void emitRecord(
            final DataPoint element, final SourceOutput<T> output, final InfluxDBSplit splitState)
            throws Exception {
        output.collect(this.dataPointDeserializer.deserialize(element), (Long) element.getTime());
        // TODO: think about splitState timestamp
        // IDEA: -> splitState.lastTimestamp = Math.max(this.lastTimestamp, element.getTime());
    }
}
