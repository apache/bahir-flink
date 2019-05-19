/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.kudu;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kudu.connector.KuduConnector;
import org.apache.flink.streaming.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.streaming.connectors.kudu.serde.KuduSerialization;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KuduSink<OUT> extends RichSinkFunction<OUT> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);

    private String kuduMasters;
    private KuduTableInfo tableInfo;
    private KuduConnector.Consistency consistency;
    private KuduConnector.WriteMode writeMode;
    private FlushMode flushMode;

    private KuduSerialization<OUT> serializer;

    private transient KuduConnector connector;

    public KuduSink(String kuduMasters, KuduTableInfo tableInfo, KuduSerialization<OUT> serializer) {
        Preconditions.checkNotNull(kuduMasters,"kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo,"tableInfo could not be null");
        this.tableInfo = tableInfo;
        this.consistency = KuduConnector.Consistency.STRONG;
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        this.serializer = serializer.withSchema(tableInfo.getSchema());
    }

    public KuduSink<OUT> withEventualConsistency() {
        this.consistency = KuduConnector.Consistency.EVENTUAL;
        return this;
    }

    public KuduSink<OUT> withStrongConsistency() {
        this.consistency = KuduConnector.Consistency.STRONG;
        return this;
    }

    public KuduSink<OUT> withUpsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        return this;
    }

    public KuduSink<OUT> withInsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.INSERT;
        return this;
    }

    public KuduSink<OUT> withUpdateWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPDATE;
        return this;
    }

    public KuduSink<OUT> withSyncFlushMode() {
        this.flushMode = FlushMode.AUTO_FLUSH_SYNC;
        return this;
    }

    public KuduSink<OUT> withAsyncFlushMode() {
        this.flushMode = FlushMode.AUTO_FLUSH_BACKGROUND;
        return this;
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        if (this.connector != null) return;
        this.connector = new KuduConnector(kuduMasters, tableInfo, consistency, writeMode, getflushMode());
        this.serializer.withSchema(tableInfo.getSchema());
    }

    /**
     * If flink checkpoint is disable,synchronously write data to kudu.
     * <p>If flink checkpoint is enable, asynchronously write data to kudu by default.
     *
     * <p>(Note: async may result in out-of-order writes to Kudu.
     *  you also can change to sync by explicitly calling {@link KuduSink#withSyncFlushMode()} when initializing KuduSink. )
     *
     * @return flushMode
     */
    private FlushMode getflushMode() {
        FlushMode flushMode = FlushMode.AUTO_FLUSH_SYNC;
        boolean enableCheckpoint = ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();
        if(enableCheckpoint && this.flushMode == null) {
            flushMode = FlushMode.AUTO_FLUSH_BACKGROUND;
        }
        if(enableCheckpoint && this.flushMode != null) {
            flushMode = this.flushMode;
        }
        return flushMode;
    }

    @Override
    public void invoke(OUT row) throws Exception {
        KuduRow kuduRow = serializer.serialize(row);
        boolean response = connector.writeRow(kuduRow);

        if(!response) {
            throw new IOException("error with some transaction");
        }
    }

    @Override
    public void close() throws Exception {
        if (this.connector == null) return;
        try {
            this.connector.close();
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Snapshotting state {} ...", context.getCheckpointId());
        }
        this.connector.flush();
    }
}
