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
package org.apache.flink.streaming.connectors.influxdb.sink.writer;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.write.Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.connector.sink.Sink.ProcessingTimeService;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;

public class InfluxDBWriter<IN> implements SinkWriter<IN, Void, IN> {

    private static final int BUFFER_SIZE = 1000;

    private final List<IN> elements;
    private ProcessingTimeService processingTimerService;
    private final InfluxDBSchemaSerializer<IN> schemaSerializer;
    private final InfluxDBClient influxDBClient;

    public InfluxDBWriter(
            final InfluxDBSchemaSerializer<IN> schemaSerializer, final InfluxDBConfig config) {
        this.schemaSerializer = schemaSerializer;
        this.elements = new ArrayList<>(BUFFER_SIZE);
        this.influxDBClient = config.getClient();
    }

    @Override
    public void write(final IN in, final Context context) throws IOException {
        try {
            if (this.elements.size() == BUFFER_SIZE) {
                this.writeCurrentElements();
                this.elements.clear();
            } else {
                this.elements.add(in);
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Void> prepareCommit(final boolean flush) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<IN> snapshotState() throws IOException {
        return this.elements;
    }

    @Override
    public void close() throws Exception {
        this.writeCurrentElements();
        this.elements.clear();
    }

    public void setProcessingTimerService(final ProcessingTimeService processingTimerService) {
        this.processingTimerService = processingTimerService;
    }

    private void writeCurrentElements() throws Exception {
        try (final WriteApi writeApi = this.influxDBClient.getWriteApi()) {
            final List<Point> points = new ArrayList<>(this.elements.size());
            for (final IN element : this.elements) {
                points.add(this.schemaSerializer.serialize(element));
            }
            writeApi.writePoints(points);
        }
    }
}
