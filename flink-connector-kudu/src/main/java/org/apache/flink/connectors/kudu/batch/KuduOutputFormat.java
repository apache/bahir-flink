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
package org.apache.flink.connectors.kudu.batch;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.RichOutputFormat;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Output format for writing data into a Kudu table (defined by the provided {@link KuduTableInfo}) in both batch
 * and stream programs.
 */
@PublicEvolving
public class KuduOutputFormat<IN> extends RichOutputFormat<IN> implements CheckpointedFunction {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;
    private final KuduOperationMapper<IN> opsMapper;

    private transient KuduWriter kuduWriter;

    public KuduOutputFormat(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper) {
        this(writerConfig, tableInfo, opsMapper, new DefaultKuduFailureHandler());
    }

    public KuduOutputFormat(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper, KuduFailureHandler failureHandler) {
        this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
        this.writerConfig = checkNotNull(writerConfig, "config could not be null");
        this.opsMapper = checkNotNull(opsMapper, "opsMapper could not be null");
        this.failureHandler = checkNotNull(failureHandler, "failureHandler could not be null");
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        kuduWriter = new KuduWriter(tableInfo, writerConfig, opsMapper, failureHandler);
    }

    @Override
    public void writeRecord(IN row) throws IOException {
        kuduWriter.write(row);
    }

    @Override
    public void close() throws IOException {
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
