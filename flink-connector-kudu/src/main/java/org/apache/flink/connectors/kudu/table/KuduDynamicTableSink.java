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
import org.apache.flink.connectors.kudu.connector.writer.KuduWriter;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.*;

/**
 * @author weijunhao
 * @date 2021/3/23 15:58
 */
public class KuduDynamicTableSink<T> implements DynamicTableSink {

    private Logger log = LoggerFactory.getLogger(KuduDynamicTableSink.class);
    private TableSchema schema;
    private KuduTableInfo tableInfo;
    private KuduWriterConfig writerConfig;
    private RowOperationMapper rowOperationMapper;

    public KuduDynamicTableSink(TableSchema schema, KuduTableInfo tableInfo, KuduWriterConfig writerConfig, RowOperationMapper rowOperationMapper) {
        this.schema = schema;
        this.tableInfo = tableInfo;
        this.writerConfig = writerConfig;
        this.rowOperationMapper = rowOperationMapper;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode changelogMode = ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        KuduWriter<Row> kuduWriter;
        try {
            kuduWriter = new KuduWriter(tableInfo, writerConfig, rowOperationMapper);
            SinkFunctionProvider provider = SinkFunctionProvider.of(new SinkFunction<RowData>() {
                @Override
                public void invoke(RowData value, Context context) throws Exception {
                    RowKind rowKind = value.getRowKind();
                    int arity = value.getArity();
                    int fieldCount = schema.getFieldCount();
                    if (arity != fieldCount)
                        throw new IllegalArgumentException(String.format("query schema length[%d] is different to sink schema length[%d]", arity, fieldCount));
                    Row row = new Row(arity);
                    for (int i = 0; i < arity; i++) {
                        Optional<DataType> dataType = schema.getFieldDataType(i);
                        Object data = castValue(value, dataType.orElse(DataTypes.NULL()), i);
                        row.setField(i, data);
                    }
                    kuduWriter.write(row);
                }
            });
            return provider;
        } catch (IOException e) {
            log.error("");
            return null;
        }
    }

    @Override
    public DynamicTableSink copy() {
        KuduDynamicTableSink sink = new KuduDynamicTableSink(schema, tableInfo, writerConfig, rowOperationMapper);
        return sink;
    }

    @Override
    public String asSummaryString() {
        return null;
    }


    public Object castValue(RowData row, DataType dataType, int pos) {
        LogicalType logicalType = dataType.getLogicalType();
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        Object data;
        if (row.isNullAt(pos)) {
            return null;
        }
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                data = row.getString(pos).toString();
                break;
            case BOOLEAN:
                data = row.getBoolean(pos);
                break;
            case BINARY:
            case VARBINARY:
                data = row.getBinary(pos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(logicalType);
                final int decimalScale = getScale(logicalType);
                data = row.getDecimal(pos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                data = row.getByte(pos);
                break;
            case SMALLINT:
                data = row.getShort(pos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                data = row.getInt(pos);
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                data = row.getLong(pos);
                break;
            case FLOAT:
                data = row.getFloat(pos);
                break;
            case DOUBLE:
                data = row.getDouble(pos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(logicalType);
                data = row.getTimestamp(pos, timestampPrecision);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
            case ARRAY:
            case NULL:
            case MULTISET:
            case MAP:
            case ROW:
            case STRUCTURED_TYPE:
            case DISTINCT_TYPE:
            case RAW:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new UnsupportedOperationException();
        }
        return data;
    }
}
