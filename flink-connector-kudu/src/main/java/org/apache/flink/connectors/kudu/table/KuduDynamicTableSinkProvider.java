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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.KuduOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriter;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/**
 * @author weijunhao
 * @date 2021/3/24 10:39
 */
public class KuduDynamicTableSinkProvider extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;
    private KuduWriter<Tuple2<Boolean, Row>> kuduWriter;
    private KuduTableInfo tableInfo;
    private KuduWriterConfig writerConfig;
    private KuduOperationMapper operationMapper;
    private DataType[] dataTypes;

    public KuduDynamicTableSinkProvider(KuduTableInfo tableInfo, KuduWriterConfig writerConfig, KuduOperationMapper operationMapper, DataType[] dataTypes) {
        this.tableInfo = tableInfo;
        this.writerConfig = writerConfig;
        this.operationMapper = operationMapper;
        this.dataTypes = dataTypes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        kuduWriter = new KuduWriter(tableInfo, writerConfig, operationMapper);
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        int arity = value.getArity();
        if (arity != dataTypes.length)
            throw new IllegalArgumentException(String.format("query schema length[%d] is different to sink schema length[%d]", arity, dataTypes.length));
        Row row = new Row(arity);
        for (int i = 0; i < arity; i++) {
            DataType dataType = dataTypes[i];
            Object data = castValue(value, dataType, i);
            row.setField(i, data);
        }
        RowKind rowKind = value.getRowKind();
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                kuduWriter.write(new Tuple2<>(true, row));
                break;
            case DELETE:
                kuduWriter.write(new Tuple2<>(false, row));
                break;
            default:
                throw new IllegalArgumentException(String.format("unsupport row kind %s.", rowKind));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        kuduWriter.close();
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
                data = row.getDecimal(pos, decimalPrecision, decimalScale).toBigDecimal();
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
                data = row.getTimestamp(pos, timestampPrecision).toTimestamp();
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
