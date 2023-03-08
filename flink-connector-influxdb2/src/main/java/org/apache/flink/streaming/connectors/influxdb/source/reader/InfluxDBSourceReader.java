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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.streaming.connectors.influxdb.common.DataPoint;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;

import java.util.Map;
import java.util.function.Supplier;

/** The source reader for the InfluxDB line protocol. */
@Internal
public final class InfluxDBSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                DataPoint, OUT, InfluxDBSplit, InfluxDBSplit> {

    public InfluxDBSourceReader(
            final Supplier<InfluxDBSplitReader> splitReaderSupplier,
            final RecordEmitter<DataPoint, OUT, InfluxDBSplit> recordEmitter,
            final Configuration config,
            final SourceReaderContext context) {
        super(splitReaderSupplier::get, recordEmitter, config, context);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (this.getNumberOfCurrentlyAssignedSplits() == 0) {
            this.context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(final Map<String, InfluxDBSplit> map) {
        this.context.sendSplitRequest();
    }

    @Override
    protected InfluxDBSplit initializedState(final InfluxDBSplit influxDBSplit) {
        return influxDBSplit;
    }

    @Override
    protected InfluxDBSplit toSplitType(final String s, final InfluxDBSplit influxDBSplitState) {
        return influxDBSplitState;
    }
}
