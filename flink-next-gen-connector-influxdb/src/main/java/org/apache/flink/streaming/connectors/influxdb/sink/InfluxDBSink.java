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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBWriter;

@Getter
@Builder
public class InfluxDBSink<IN> implements Sink<IN, String, String, Void> {

    private final InfluxDBWriter writer;

    @Nullable private final SimpleVersionedSerializer<String> writerStateSerializer;

    @Nullable private final Committer<String> committer;

    @Nullable private final SimpleVersionedSerializer<String> committableSerializer;

    @Nullable private final GlobalCommitter<String, String> globalCommitter;

    @Nullable private final SimpleVersionedSerializer<String> globalCommittableSerializer;

    @Override
    public SinkWriter<IN, String, String> createWriter(
            final InitContext initContext, final List<String> list) throws IOException {
        this.writer.setProcessingTimerService(initContext.getProcessingTimeService());
        return this.writer;
    }

    @Override
    public Optional<Committer<String>> createCommitter() throws IOException {
        return Optional.ofNullable(this.committer);
    }

    @Override
    public Optional<GlobalCommitter<String, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<String>> getCommittableSerializer() {
        return Optional.ofNullable(this.committableSerializer);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<String>> getWriterStateSerializer() {
        return Optional.ofNullable(this.committableSerializer);
    }
}
