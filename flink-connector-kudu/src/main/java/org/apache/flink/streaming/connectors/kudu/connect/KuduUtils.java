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

package org.apache.flink.streaming.connectors.kudu.connect;


import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

public final class KuduUtils {

    private KuduUtils() { }

    public static void addKuduRowValue(RowResult row, ColumnSchema column, Map<String, Object> values) {
        String colName = column.getName();
        if(row.isNull(colName)) {
            values.put(colName, null);
            return;
        }
        switch (column.getType()) {
            case STRING:
                values.put(colName, row.getString(colName));
                break;
            case FLOAT:
                values.put(colName, row.getFloat(colName));
                break;
            case INT8:
                values.put(colName, row.getByte(colName));
                break;
            case INT16:
                values.put(colName, row.getShort(colName));
                break;
            case INT32:
                values.put(colName, row.getInt(colName));
                break;
            case INT64:
                values.put(colName, row.getLong(colName));
                break;
            case DOUBLE:
                values.put(colName, row.getDouble(colName));
                break;
            case BOOL:
                values.put(colName, row.getBoolean(colName));
                break;
            case UNIXTIME_MICROS:
                long time = row.getLong(colName)/1000;
                values.put(colName, new Date(time));
                break;
            case BINARY:
                values.put(colName, row.getBinary(colName).array());
                break;
        }
    }

    public static void addPartialRowValue(PartialRow partialRow, ColumnSchema column, Object value) {
        String columnName = column.getName();
        if (value == null) {
            partialRow.setNull(columnName);
            return;
        }

        switch (column.getType()){
            case STRING:
                partialRow.addString(columnName, (String)value);
                break;
            case FLOAT:
                partialRow.addFloat(columnName, (Float)value);
                break;
            case INT8:
                partialRow.addByte(columnName, (Byte)value);
                break;
            case INT16:
                partialRow.addShort(columnName, (Short)value);
                break;
            case INT32:
                partialRow.addInt(columnName, (Integer)value);
                break;
            case INT64:
                partialRow.addLong(columnName, (Long)value);
                break;
            case DOUBLE:
                partialRow.addDouble(columnName, (Double)value);
                break;
            case BOOL:
                partialRow.addBoolean(columnName, (Boolean)value);
                break;
            case UNIXTIME_MICROS:
                Long time = (Long)value;
                if (time != null) {
                    //*1000 to correctly create date on kudu
                    partialRow.addLong(columnName, time*1000);
                }
                break;
            case BINARY:
                partialRow.addBinary(columnName, (byte[]) value);
                break;
        }
    }

    public static KuduPredicate predicate(KuduFilter filter, Schema kSchema) {
        ColumnSchema column = kSchema.getColumn(filter.getColumn());
        return predicate(filter, column);
    }
    public static KuduPredicate predicate(KuduFilter filter, ColumnSchema column) {

        KuduPredicate predicate;

        switch (filter.getFilter()) {
            case IS_IN:
                predicate = KuduPredicate.newInListPredicate(column, (List) filter.getValue());
                break;
            case IS_NULL:
                predicate = KuduPredicate.newIsNullPredicate(column);
                break;
            case IS_NOT_NULL:
                predicate = KuduPredicate.newIsNotNullPredicate(column);
                break;
            default:
                predicate = predicateComparator(column,filter);
                break;
        }
        return predicate;
    }

    private static KuduPredicate predicateComparator(ColumnSchema column, KuduFilter filter) {

        KuduPredicate.ComparisonOp comparison = filter.getFilter().comparator;

        KuduPredicate predicate = null;

        switch (column.getType()) {
            case STRING:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, (String)filter.getValue());
                break;
            case FLOAT:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, (Float)filter.getValue());
                break;
            case INT8:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, (Byte)filter.getValue());
                break;
            case INT16:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, (Short)filter.getValue());
                break;
            case INT32:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, (Integer)filter.getValue());
                break;
            case INT64:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, (Long)filter.getValue());
                break;
            case DOUBLE:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, (Double)filter.getValue());
                break;
            case BOOL:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, (Boolean)filter.getValue());
                break;
            case UNIXTIME_MICROS:
                Long time = (Long)filter.getValue();
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, time*1000);
                break;
            case BINARY:
                predicate = KuduPredicate.newComparisonPredicate(
                        column, comparison, filter.getValue().toString().getBytes());
                break;
        }
        return predicate;
    }

}
