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
package org.apache.flink.streaming.connectors.influxdb.source;

import java.util.Properties;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.influxdb.source.enumerator.InfluxDBSourceEnumState;
import org.apache.flink.streaming.connectors.influxdb.source.enumerator.InfluxDBSourceEnumStateSerializer;
import org.apache.flink.streaming.connectors.influxdb.source.enumerator.InfluxDBSplitEnumerator;
import org.apache.flink.streaming.connectors.influxdb.source.reader.InfluxDBRecordEmitter;
import org.apache.flink.streaming.connectors.influxdb.source.reader.InfluxDBSourceReader;
import org.apache.flink.streaming.connectors.influxdb.source.reader.InfluxDBSplitReader;
import org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer.InfluxDBDataPointDeserializer;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplitSerializer;

/**
 * The Source implementation of InfluxDB. Please use a {@link InfluxDBSourceBuilder} to construct a
 * {@link InfluxDBSource}. The following example shows how to create an InfluxDBSource emitting
 * records of <code>
 * String</code> type.
 *
 * <pre>{@code
 * add example
 * }</pre>
 *
 * <p>See {@link InfluxDBSourceBuilder} for more details.
 *
 * @param <OUT> the output type of the source.
 */
public class InfluxDBSource<OUT>
        implements Source<OUT, InfluxDBSplit, InfluxDBSourceEnumState>, ResultTypeQueryable<OUT> {

    private final Properties properties;
    private final InfluxDBDataPointDeserializer<OUT> deserializationSchema;

    InfluxDBSource(
            final Properties properties,
            final InfluxDBDataPointDeserializer<OUT> deserializationSchema) {
        this.properties = properties;
        this.deserializationSchema = deserializationSchema;
    }

    /**
     * Get a influxDBSourceBuilder to build a {@link InfluxDBSource}.
     *
     * @return a InfluxDB source builder.
     */
    public static <OUT> InfluxDBSourceBuilder<OUT> builder() {
        return new InfluxDBSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<OUT, InfluxDBSplit> createReader(
            final SourceReaderContext sourceReaderContext) {
        final Supplier<InfluxDBSplitReader> splitReaderSupplier =
                () -> new InfluxDBSplitReader(this.properties);
        final InfluxDBRecordEmitter<OUT> recordEmitter =
                new InfluxDBRecordEmitter<>(this.deserializationSchema);

        return new InfluxDBSourceReader<>(
                splitReaderSupplier,
                recordEmitter,
                this.toConfiguration(this.properties),
                sourceReaderContext);
    }

    @Override
    public SplitEnumerator<InfluxDBSplit, InfluxDBSourceEnumState> createEnumerator(
            final SplitEnumeratorContext<InfluxDBSplit> splitEnumeratorContext) {
        return new InfluxDBSplitEnumerator(splitEnumeratorContext);
    }

    @Override
    public SplitEnumerator<InfluxDBSplit, InfluxDBSourceEnumState> restoreEnumerator(
            final SplitEnumeratorContext<InfluxDBSplit> splitEnumeratorContext,
            final InfluxDBSourceEnumState influxDBSourceEnumState) {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<InfluxDBSplit> getSplitSerializer() {
        return new InfluxDBSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<InfluxDBSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new InfluxDBSourceEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return this.deserializationSchema.getProducedType();
    }

    // ----------- private helper methods ---------------

    private Configuration toConfiguration(final Properties props) {
        final Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }
}
