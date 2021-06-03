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

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.*;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author weijunhao
 * @date 2021/5/31 19:47
 */
public class KuduLookupFunction extends TableFunction<RowData> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final String master;
    private final String tableName;
    private final String[] keyNames;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private transient KuduClient client;
    private transient KuduTable table;
    private transient Cache<RowData, List<RowData>> cache;

    public KuduLookupFunction(String master,
                              String tableName,
                              String[] keyNames,
                              long cacheMaxSize,
                              long cacheExpireMs,
                              int maxRetryTimes) {
        this.master = master;
        this.tableName = tableName;
        this.keyNames = keyNames;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.client = new KuduClient.KuduClientBuilder(master).build();
        this.table = client.openTable(tableName);
        this.cache =
                cacheMaxSize == -1 || cacheExpireMs == -1
                        ? null
                        : CacheBuilder.newBuilder()
                        .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                        .maximumSize(cacheMaxSize)
                        .build();
    }

    public void eval(Object... values) throws KuduException {
        Preconditions.checkArgument(values.length == keyNames.length, "eval error.");
        RowData data = GenericRowData.of(values);
        if (cache != null) {
            List<RowData> cacheRow = cache.getIfPresent(data);
            if (cacheRow != null) {
                cacheRow.stream().forEach(row -> collect(row));
            }
            return;
        }
        for (int tryTime = 0; tryTime < maxRetryTimes; tryTime++) {
            try {
                KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);
                Schema schema = table.getSchema();
                for (int i = 0; i < keyNames.length; i++) {
                    ColumnSchema columnSchema = schema.getColumn(keyNames[i]);
                    addPredicate(builder, columnSchema, data, i);
                }
                KuduScanner scanner = builder.build();
                List<RowData> list = new ArrayList<>();
                while (scanner.hasMoreRows()) {
                    RowResultIterator rowResults = scanner.nextRows();
                    while (rowResults.hasNext()) {
                        RowResult result = rowResults.next();
                        Object[] objects = result.getSchema().getColumns().stream().map(columnSchema -> {
                            Object object = result.getObject(columnSchema.getName());
                            if (object instanceof String) {
                                return StringData.fromString((String) object);
                            } else if (object instanceof BigDecimal) {
                                BigDecimal decimal = (BigDecimal) object;
                                return DecimalData.fromBigDecimal(decimal, decimal.precision(), decimal.scale());
                            } else if (object instanceof Timestamp) {
                                return TimestampData.fromTimestamp((Timestamp) object);
                            } else {
                                return object;
                            }
                        }).collect(Collectors.toList()).toArray();
                        GenericRowData rowData = GenericRowData.of(objects);
                        collect(rowData);
                        list.add(rowData);
                    }
                }
                if (cache != null) {
                    cache.put(data, list);
                }
                break;
            } catch (Exception e) {
                log.error("query kudu table error.", e);
            }
        }
    }


    private void addPredicate(KuduScanner.KuduScannerBuilder builder, ColumnSchema columnSchema, RowData data, int index) {
        Type type = columnSchema.getType();
        switch (type) {
            case INT8:
                builder.addPredicate(KuduPredicate.newComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, data.getByte(index)));
                break;
            case INT16:
                builder.addPredicate(KuduPredicate.newComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, data.getShort(index)));
                break;
            case INT32:
                builder.addPredicate(KuduPredicate.newComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, data.getInt(index)));
                break;
            case INT64:
                builder.addPredicate(KuduPredicate.newComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, data.getLong(index)));
                break;
            case BINARY:
                builder.addPredicate(KuduPredicate.newComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, data.getBinary(index)));
                break;
            case VARCHAR:
            case STRING:
                builder.addPredicate(KuduPredicate.newComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, data.getString(index).toString()));
                break;
            case BOOL:
                builder.addPredicate(KuduPredicate.newComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, data.getBoolean(index)));
                break;
            case FLOAT:
                builder.addPredicate(KuduPredicate.newComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, data.getFloat(index)));
                break;
            case DOUBLE:
                builder.addPredicate(KuduPredicate.newComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, data.getDouble(index)));
                break;
            case UNIXTIME_MICROS:
            case DATE:
                builder.addPredicate(KuduPredicate.newComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, data.getTimestamp(index, 0).toTimestamp()));
                break;
            case DECIMAL:
                builder.addPredicate(KuduPredicate.newComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, data.getDecimal(index, 0, 0).toBigDecimal()));
                break;
            default:
                throw new IllegalArgumentException("not support the data type.");
        }
    }
}
