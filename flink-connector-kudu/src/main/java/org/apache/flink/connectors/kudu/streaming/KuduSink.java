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
import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.failure.DefaultKuduFailureHandler;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.serde.KuduSerialization;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.connectors.kudu.connector.failure.KuduFailureHandler;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

@PublicEvolving
public class KuduSink<OUT> extends RichSinkFunction<OUT> implements CheckpointedFunction {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;
    private final KuduSerialization<OUT> serializer;

    private transient KuduWriter kuduWriter;

    public KuduSink(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduSerialization<OUT> serializer) {
        this(writerConfig, tableInfo, serializer, new DefaultKuduFailureHandler());
    }

    public KuduSink(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduSerialization<OUT> serializer, KuduFailureHandler failureHandler) {
        this.tableInfo = checkNotNull(tableInfo,"tableInfo could not be null");
        this.writerConfig = checkNotNull(writerConfig,"config could not be null");
        this.serializer = checkNotNull(serializer,"serializer could not be null");
        this.failureHandler = checkNotNull(failureHandler,"failureHandler could not be null");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kuduWriter = new KuduWriter(tableInfo, writerConfig, failureHandler);

        serializer.withSchema(kuduWriter.getSchema());
    }

    @Override
    public void invoke(OUT value) throws Exception {
        final KuduRow kuduRow = serializer.serialize(value);
        kuduWriter.write(kuduRow);
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
