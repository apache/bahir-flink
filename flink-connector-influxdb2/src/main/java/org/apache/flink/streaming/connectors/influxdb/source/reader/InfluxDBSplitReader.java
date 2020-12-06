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

import static org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSourceOptions.ENQUEUE_WAIT_TIME;
import static org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSourceOptions.INGEST_QUEUE_CAPACITY;
import static org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSourceOptions.MAXIMUM_LINES_PER_REQUEST;
import static org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSourceOptions.PORT;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.source.http.HealthCheckHandler;
import org.apache.flink.streaming.connectors.influxdb.source.http.WriteAPIHandler;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;

/**
 * A {@link SplitReader} implementation that reads records from InfluxDB splits.
 *
 * <p>The returned type are in the format of {@link DataPoint}.
 */
@Internal
public final class InfluxDBSplitReader implements SplitReader<DataPoint, InfluxDBSplit> {

    private final long enqueueWaitTime;
    private final int maximumLinesPerRequest;
    private final int defaultPort;

    private HttpServer server = null;

    private final FutureCompletingBlockingQueue<List<DataPoint>> ingestionQueue;

    private InfluxDBSplit split;

    public InfluxDBSplitReader(final Configuration configuration) {
        this.enqueueWaitTime = configuration.getLong(ENQUEUE_WAIT_TIME);
        this.maximumLinesPerRequest = configuration.getInteger(MAXIMUM_LINES_PER_REQUEST);
        this.defaultPort = configuration.getInteger(PORT);
        final int capacity = configuration.getInteger(INGEST_QUEUE_CAPACITY);
        this.ingestionQueue = new FutureCompletingBlockingQueue<>(capacity);
    }

    @Override
    public RecordsWithSplitIds<DataPoint> fetch() throws IOException {
        if (this.split == null) {
            return null;
        }
        final InfluxDBSplitRecords recordsBySplits = new InfluxDBSplitRecords(this.split.splitId());

        try {
            this.ingestionQueue.getAvailabilityFuture().get();
        } catch (final InterruptedException | ExecutionException exception) {
            throw new IOException("An exception occurred during fetch", exception);
        }
        final List<DataPoint> requests = this.ingestionQueue.poll();
        if (requests == null) {
            recordsBySplits.prepareForRead();
            return recordsBySplits;
        }
        recordsBySplits.addAll(requests);
        recordsBySplits.prepareForRead();
        return recordsBySplits;
    }

    @Override
    public void handleSplitsChanges(final SplitsChange<InfluxDBSplit> splitsChange) {
        if (splitsChange.splits().isEmpty()) {
            return;
        }
        this.split = splitsChange.splits().get(0);

        if (this.server != null) {
            return;
        }
        try {
            this.server = HttpServer.create(new InetSocketAddress(this.defaultPort), 0);
        } catch (final IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Unable to start HTTP Server on Port %d: %s",
                            this.defaultPort, e.getMessage()));
        }

        this.server.createContext(
                "/api/v2/write",
                new WriteAPIHandler(
                        this.maximumLinesPerRequest,
                        this.ingestionQueue,
                        this.split.splitId().hashCode(),
                        this.enqueueWaitTime));
        this.server.createContext("/health", new HealthCheckHandler());
        this.server.setExecutor(null); // creates a default executor
        this.server.start();
    }

    @Override
    public void wakeUp() {
        this.ingestionQueue.notifyAvailable();
    }

    @Override
    public void close() {
        if (this.server != null) {
            this.server.stop(1); // waits max 1 second for pending requests to finish
        }
    }

    // ---------------- private helper class --------------------

    private static class InfluxDBSplitRecords implements RecordsWithSplitIds<DataPoint> {
        private final List<DataPoint> records;
        private Iterator<DataPoint> recordIterator;
        private final String splitId;

        private InfluxDBSplitRecords(final String splitId) {
            this.splitId = splitId;
            this.records = new ArrayList<>();
        }

        private boolean addAll(final List<DataPoint> records) {
            return this.records.addAll(records);
        }

        private void prepareForRead() {
            this.recordIterator = this.records.iterator();
        }

        @Override
        @Nullable
        public String nextSplit() {
            if (this.recordIterator.hasNext()) {
                return this.splitId;
            }
            return null;
        }

        @Override
        @Nullable
        public DataPoint nextRecordFromSplit() {
            if (this.recordIterator.hasNext()) {
                return this.recordIterator.next();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }
    }
}
