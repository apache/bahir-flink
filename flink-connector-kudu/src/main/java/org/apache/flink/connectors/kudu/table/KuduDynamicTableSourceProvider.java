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

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connectors.kudu.table.KuduDynamicTableSource.ScanOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.*;
import org.apache.flink.types.RowKind;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * @author weijunhao
 * @date 2021/5/28 17:18
 */
public class KuduDynamicTableSourceProvider extends RichSourceFunction<RowData> implements CheckpointedFunction {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final String master;
    private final String tableName;
    private final ScanOptions scanOptions;

    private transient BroadcastState<String, RowData> state;
    private final Map<String, RowData> bufferRows;

    public KuduDynamicTableSourceProvider(String master, String tableName, ScanOptions scanOptions) {
        this.master = master;
        this.tableName = tableName;
        this.scanOptions = scanOptions;
        this.bufferRows = new HashMap<>(1024);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        KuduClient client = new KuduClient.KuduClientBuilder(master).build();
        KuduTable table = client.openTable(tableName);
        Schema schema = table.getSchema();
        while (isRunning.get()) {
            //本次查询的结果集
            Map<String, RowData> rows = new HashMap<>();
            KuduScanner scanner = getTableScanner(client, table, Collections.emptyList());
            while (!scanner.isClosed() && scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    long timestamp = System.currentTimeMillis();
                    RowResult row = results.next();
                    List<ColumnSchema> columns = schema.getColumns();
                    Object[] values = columns.stream().map(columnSchema -> {
                        columnSchema.getTypeAttributes();
                        Object value = getOrNullToFlinkData(row, columnSchema.getName(), columnSchema.getType().getDataType(columnSchema.getTypeAttributes()));
                        return value;
                    }).collect(Collectors.toList()).toArray();
                    List<ColumnSchema> pkColumns = columns.stream().filter(columnSchema -> columnSchema.isKey()).collect(Collectors.toList());
                    String primaryKey = generatePrimaryKey(pkColumns);
                    if (bufferRows.containsKey(primaryKey)) {
                        RowData rowDataBefore = bufferRows.get(primaryKey);
                        rowDataBefore.setRowKind(RowKind.UPDATE_BEFORE);
                        ctx.collectWithTimestamp(rowDataBefore, timestamp);
                        GenericRowData rowData = GenericRowData.ofKind(RowKind.UPDATE_AFTER, values);
                        ctx.collectWithTimestamp(rowData, timestamp);
                        rows.put(primaryKey, rowData);
                    } else {
                        GenericRowData rowData = GenericRowData.ofKind(RowKind.INSERT, values);
                        ctx.collectWithTimestamp(rowData, timestamp);
                        rows.put(primaryKey, rowData);
                    }
                }
            }
            //遍历上一次的结果集，判断被删除的row
            for (Map.Entry<String, RowData> entry : bufferRows.entrySet()) {
                //本次结果集没有上次结果集的元素 则为删除记录
                if (!rows.containsKey(entry.getKey())) {
                    RowData rowData = entry.getValue();
                    rowData.setRowKind(RowKind.DELETE);
                    ctx.collectWithTimestamp(rowData, System.currentTimeMillis());
                }
            }
            bufferRows.clear();
            bufferRows.putAll(rows);
            LockSupport.parkNanos(scanOptions.getInerval() * 1000 * 1000);
        }
        client.shutdown();
    }

    private KuduScanner getTableScanner(KuduClient client, KuduTable table, List<Tuple3<String, Object, Comparison>> conditions) {
        Objects.requireNonNull(conditions, "conditions is not allow null");
        Schema schema = table.getSchema();
        KuduScanner.KuduScannerBuilder builder = client
                .newScannerBuilder(table)
                .setProjectedColumnNames(schema.getColumns().stream().map(ColumnSchema::getName).collect(Collectors.toList()));
        for (Tuple3<String, Object, Comparison> condition : conditions) {
            String k = condition.f0;
            Object v = condition.f1;
            Comparison comparison = condition.f2;
            if (v instanceof String) {
                builder.addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumn(k), comparison.getOp(), (String) v));
            } else if (v instanceof Number) {
                builder.addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumn(k), comparison.getOp(), (long) v));
            } else if (v instanceof Boolean) {
                builder.addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumn(k), comparison.getOp(), (boolean) v));
            } else if (v instanceof Float) {
                builder.addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumn(k), comparison.getOp(), (float) v));
            } else if (v instanceof BigDecimal) {
                builder.addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumn(k), comparison.getOp(), (BigDecimal) v));
            } else if (v instanceof Timestamp) {
                builder.addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumn(k), comparison.getOp(), (Timestamp) v));
            } else if (comparison == Comparison.IN && v instanceof List) {
                builder.addPredicate(KuduPredicate.newInListPredicate(schema.getColumn(k), (List) v));
            } else if (v == null) {
                throw new IllegalArgumentException("condition param is null，key=" + k);
            } else {
                throw new IllegalArgumentException(MessageFormat.format("condition param's type error，value={0}，type={1}", v, v.getClass().getTypeName()));
            }
        }
        KuduScanner scanner = builder.build();
        return scanner;
    }


    private Object getOrNullToFlinkData(RowResult result, String columnName, Common.DataType dataType) {
        Object value = null;
        if (!result.isNull(columnName)) {
            switch (dataType) {
                case BOOL:
                    value = result.getBoolean(columnName);
                    break;
                case INT8:
                    value = result.getByte(columnName);
                    break;
                case INT16:
                    value = result.getShort(columnName);
                    break;
                case INT32:
                    value = result.getInt(columnName);
                    break;
                case INT64:
                    value = result.getLong(columnName);
                    break;
                case BINARY:
                    value = result.getBinary(columnName).array();
                    break;
                case FLOAT:
                    value = result.getFloat(columnName);
                    break;
                case DOUBLE:
                    value = result.getDouble(columnName);
                    break;
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    BigDecimal decimal = result.getDecimal(columnName);
                    value = DecimalData.fromBigDecimal(decimal, decimal.precision(), decimal.scale());
                    break;
                case UNIXTIME_MICROS:
                    value = TimestampData.fromTimestamp(result.getTimestamp(columnName));
                    break;
                case STRING:
                    value = StringData.fromString(result.getString(columnName));
                    break;
                default:
                    value = null;
            }
        }
        return value;
    }

    private String generatePrimaryKey(List<ColumnSchema> pkColumns) {
        String pk = pkColumns.stream().map(columnSchema -> columnSchema.getName()).collect(Collectors.joining("&"));
        return pk;
    }

    @Override
    public void cancel() {
        isRunning.compareAndSet(true, false);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        state.clear();
        state.putAll(bufferRows);
//        for (Map.Entry<String, RowData> entry : state.entries()) {
//            bufferRows.put(entry.getKey(), entry.getValue());
//        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        MapStateDescriptor<String, RowData> descriptor = new MapStateDescriptor<>("buffered-elements", TypeInformation.of(String.class), TypeInformation.of(RowData.class));
        state = context.getOperatorStateStore().getBroadcastState(descriptor);
        if (context.isRestored()) {//作业恢复
            for (Map.Entry<String, RowData> entry : state.entries()) {
                bufferRows.put(entry.getKey(), entry.getValue());
            }
        }
    }
}