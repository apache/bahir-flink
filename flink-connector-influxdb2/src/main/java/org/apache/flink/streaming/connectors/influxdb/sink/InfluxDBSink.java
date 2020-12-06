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
package org.apache.flink.streaming.connectors.influxdb.sink;

import com.influxdb.client.write.Point;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.influxdb.sink.commiter.InfluxDBCommittableSerializer;
import org.apache.flink.streaming.connectors.influxdb.sink.commiter.InfluxDBCommitter;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBPointSerializer;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBSchemaSerializer;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBWriter;

/**
 * This Sink implementation of InfluxDB/Line Protocol. Please use a {@link InfluxDBSinkBuilder} to
 * construct a {@link InfluxDBSink}. The following example shows how to create an InfluxDBSink
 * having records of <code>Long</code> as input type.
 *
 * <pre>{@code
 * InfluxDBSink<Long> influxDBSink = InfluxDBSink.builder()
 * .setInfluxDBSchemaSerializer(new InfluxDBSerializer())
 * .setInfluxDBUrl(getUrl())
 * .setInfluxDBUsername(getUsername())
 * .setInfluxDBPassword(getPassword())
 * .setInfluxDBBucket(getBucket())
 * .setInfluxDBOrganization(getOrg())
 * .build();
 * }</pre>
 *
 * <p>See {@link InfluxDBSinkBuilder} for more details.
 *
 * @param <IN> type of the input of the sink.
 */
public final class InfluxDBSink<IN> implements Sink<IN, Long, Point, Void> {

    private final InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer;
    private final Configuration configuration;

    InfluxDBSink(
            final InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer,
            final Configuration configuration) {
        this.influxDBSchemaSerializer = influxDBSchemaSerializer;
        this.configuration = configuration;
    }

    /**
     * Get a influxDBSinkBuilder to build a {@link InfluxDBSink}.
     *
     * @return a InfluxDB sink builder.
     */
    public static <IN> InfluxDBSinkBuilder<IN> builder() {
        return new InfluxDBSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN, Long, Point> createWriter(
            final InitContext initContext, final List<Point> list) {
        final InfluxDBWriter<IN> writer =
                new InfluxDBWriter<>(this.influxDBSchemaSerializer, this.configuration);
        writer.setProcessingTimerService(initContext.getProcessingTimeService());
        return writer;
    }

    @Override
    public Optional<Committer<Long>> createCommitter() {
        return Optional.of(new InfluxDBCommitter(this.configuration));
    }

    @Override
    public Optional<SimpleVersionedSerializer<Long>> getCommittableSerializer() {
        return Optional.of(new InfluxDBCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<Point>> getWriterStateSerializer() {
        return Optional.of(new InfluxDBPointSerializer());
    }

    @Override
    public Optional<GlobalCommitter<Long, Void>> createGlobalCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }
}
