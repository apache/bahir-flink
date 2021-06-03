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
package org.apache.flink.connectors.kudu.table;

import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.KuduOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author weijunhao
 * @date 2021/3/23 15:58
 */
public class KuduDynamicTableSink implements DynamicTableSink {

    private Logger log = LoggerFactory.getLogger(KuduDynamicTableSink.class);
    private TableSchema schema;
    private KuduTableInfo tableInfo;
    private KuduWriterConfig writerConfig;
    private KuduOperationMapper operationMapper;

    public KuduDynamicTableSink(TableSchema schema, KuduTableInfo tableInfo, KuduWriterConfig writerConfig, KuduOperationMapper operationMapper) {
        this.schema = schema;
        this.tableInfo = tableInfo;
        this.writerConfig = writerConfig;
        this.operationMapper = operationMapper;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode changelogMode = ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataType[] dataTypes = schema.getFieldDataTypes();
        KuduDynamicTableSinkProvider sinkFunction = new KuduDynamicTableSinkProvider(tableInfo, writerConfig, operationMapper, dataTypes);
        SinkFunctionProvider provider = SinkFunctionProvider.of(sinkFunction);
        return provider;
    }

    @Override
    public DynamicTableSink copy() {
        KuduDynamicTableSink sink = new KuduDynamicTableSink(schema, tableInfo, writerConfig, operationMapper);
        return sink;
    }

    @Override
    public String asSummaryString() {
        return String.format("Kudu[%s]", tableInfo.getName());
    }


}
