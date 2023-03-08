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
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Sink.ProcessingTimeService;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSinkOptions.*;

/**
 * This Class implements the {@link SinkWriter} and it is responsible to write incoming inputs to
 * InfluxDB. It uses the {@link InfluxDBSchemaSerializer} to serialize the input into a {@link
 * Point} object. Each serialized object is stored in the {@link #elements} list. Whenever the size
 * of the list reaches the {@link #bufferSize}, the influxDB write API is called and all the items
 * all written. The {@link #lastTimestamp} keeps track of the biggest timestamp of the incoming
 * elements.
 *
 * @param <IN> Type of the input
 * @see WriteApi
 */
@Internal
public final class InfluxDBWriter<IN> implements SinkWriter<IN, Long, Point> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBWriter.class);

    private final int bufferSize;
    private final boolean writeCheckpoint;
    private long lastTimestamp = 0;
    private final List<Point> elements;
    private ProcessingTimeService processingTimerService;
    private final InfluxDBSchemaSerializer<IN> schemaSerializer;
    private final InfluxDBClient influxDBClient;

    public InfluxDBWriter(
            final InfluxDBSchemaSerializer<IN> schemaSerializer,
            final Configuration configuration) {
        this.schemaSerializer = schemaSerializer;
        this.bufferSize = configuration.getInteger(WRITE_BUFFER_SIZE);
        this.elements = new ArrayList<>(this.bufferSize);
        this.writeCheckpoint = configuration.getBoolean(WRITE_DATA_POINT_CHECKPOINT);
        this.influxDBClient = getInfluxDBClient(configuration);
    }

    /**
     * This method calls the InfluxDB write API whenever the element list reaches the {@link
     * #bufferSize}. It keeps track of the latest timestamp of each element. It compares the latest
     * timestamp with the context.timestamp() and takes the bigger (latest) timestamp.
     *
     * @param in incoming data
     * @param context current Flink context
     * @see org.apache.flink.api.connector.sink.SinkWriter.Context
     */
    public void write(final IN in, final Context context) throws IOException {
        LOG.trace("Adding elements to buffer. Buffer size: {}", this.elements.size());
        this.elements.add(this.schemaSerializer.serialize(in, context));
        if (this.elements.size() == this.bufferSize) {
            LOG.debug("Buffer size reached preparing to write the elements.");
            this.writeCurrentElements();
            this.elements.clear();
        }

        if (context.timestamp() != null) {
            this.lastTimestamp = Math.max(this.lastTimestamp, context.timestamp());
        }
    }

    /**
     * This method is called whenever a checkpoint is set by Flink. It creates a list and fills it
     * up with the latest timestamp.
     *
     * @param flush
     * @return A list containing 0 or 1 element
     */
    @Override
    public List<Long> prepareCommit(final boolean flush) {
        if (this.lastTimestamp == 0) {
            return Collections.emptyList();
        }
        final List<Long> lastTimestamp = new ArrayList<>(1);
        lastTimestamp.add(this.lastTimestamp);
        return lastTimestamp;
    }

    @Override
    public List<Point> snapshotState() {
        return this.elements;
    }

    @Override
    public void close() throws Exception {
        LOG.debug("Preparing to write the elements in InfluxDB.");
        this.writeCurrentElements();
        LOG.debug("Closing the writer.");
        this.elements.clear();
    }

    public void setProcessingTimerService(final ProcessingTimeService processingTimerService) {
        this.processingTimerService = processingTimerService;
    }

    private void writeCurrentElements() {
        try (final WriteApi writeApi = this.influxDBClient.getWriteApi()) {
            writeApi.writePoints(this.elements);
            LOG.debug("Wrote {} data points", this.elements.size());
        }
    }
}
