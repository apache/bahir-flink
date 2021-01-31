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

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Builder;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.sink.commiter.InfluxDBCommitter;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBSchemaSerializer;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBWriter;

@Builder
public class InfluxDBSink<IN> implements Sink<IN, Void, IN, Void> {

    private final InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer;

    private final InfluxDBConfig influxDBConfig;

    @Nullable private final SimpleVersionedSerializer<IN> writerStateSerializer;

    @Builder.Default
    private SimpleVersionedSerializer<Void> committableSerializer =
            InfluxDBCommittableSerializer.INSTANCE;

    private InfluxDBSink(
            final InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer,
            final InfluxDBConfig influxDBConfig,
            @Nullable final SimpleVersionedSerializer<IN> writerStateSerializer,
            final SimpleVersionedSerializer<Void> committableSerializer) {
        this.influxDBSchemaSerializer = influxDBSchemaSerializer;
        this.influxDBConfig = influxDBConfig;
        this.writerStateSerializer = writerStateSerializer;
        this.committableSerializer = committableSerializer;
    }

    @Override
    public SinkWriter<IN, Void, IN> createWriter(
            final InitContext initContext, final List<IN> list) {
        final InfluxDBWriter<IN> writer =
                new InfluxDBWriter<>(this.influxDBSchemaSerializer, this.influxDBConfig);
        writer.setProcessingTimerService(initContext.getProcessingTimeService());
        return writer;
    }

    @Override
    public Optional<Committer<Void>> createCommitter() {
        return Optional.of(new InfluxDBCommitter(this.influxDBConfig));
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getCommittableSerializer() {
        return Optional.ofNullable(this.committableSerializer);
    }

    @Override
    public Optional<SimpleVersionedSerializer<IN>> getWriterStateSerializer() {
        return Optional.ofNullable(this.writerStateSerializer);
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }
}
