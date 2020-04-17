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
package org.apache.flink.connectors.kudu.streaming;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.failure.DefaultKuduFailureHandler;
import org.apache.flink.connectors.kudu.connector.failure.KuduFailureHandler;
import org.apache.flink.connectors.kudu.connector.writer.KuduOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriter;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Streaming Sink that executes Kudu operations based on the incoming elements.
 * The target Kudu table is defined in the {@link KuduTableInfo} object together with parameters for table
 * creation in case the table does not exist.
 * <p>
 * Incoming records are mapped to Kudu table operations using the provided {@link KuduOperationMapper} logic. While
 * failures resulting from the operations are handled by the {@link KuduFailureHandler} instance.
 *
 * @param <IN> Type of the input records
 */
@PublicEvolving
public class KuduSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;
    private final KuduOperationMapper<IN> opsMapper;
    private transient KuduWriter kuduWriter;

    /**
     * Creates a new {@link KuduSink} that will execute operations against the specified Kudu table (defined in {@link KuduTableInfo})
     * for the incoming stream elements.
     *
     * @param writerConfig Writer configuration
     * @param tableInfo    Table information for the target table
     * @param opsMapper    Mapping logic from inputs to Kudu operations
     */
    public KuduSink(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper) {
        this(writerConfig, tableInfo, opsMapper, new DefaultKuduFailureHandler());
    }

    /**
     * Creates a new {@link KuduSink} that will execute operations against the specified Kudu table (defined in {@link KuduTableInfo})
     * for the incoming stream elements.
     *
     * @param writerConfig   Writer configuration
     * @param tableInfo      Table information for the target table
     * @param opsMapper      Mapping logic from inputs to Kudu operations
     * @param failureHandler Custom failure handler instance
     */
    public KuduSink(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper, KuduFailureHandler failureHandler) {
        this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
        this.writerConfig = checkNotNull(writerConfig, "config could not be null");
        this.opsMapper = checkNotNull(opsMapper, "opsMapper could not be null");
        this.failureHandler = checkNotNull(failureHandler, "failureHandler could not be null");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kuduWriter = new KuduWriter(tableInfo, writerConfig, opsMapper, failureHandler);
    }

    @Override
    public void invoke(IN value) throws Exception {
        try {
            kuduWriter.write(value);
        } catch (ClassCastException e) {
            failureHandler.onTypeMismatch(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (kuduWriter != null) {
            kuduWriter.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        kuduWriter.flushAndCheckErrors();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    }

}
