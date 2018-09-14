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
package org.apache.flink.streaming.connectors.kudu.connector;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

import java.util.List;

public final class KuduMapper {

    private KuduMapper() { }

    public static KuduRow toKuduRow(RowResult row) {
        Schema schema = row.getColumnProjection();
        List<ColumnSchema> columns = schema.getColumns();

        KuduRow values = new KuduRow(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            String name = schema.getColumnByIndex(i).getName();
            if(row.isNull(i)) {
                values.setField(i, name, null);
            } else {
                Type type = schema.getColumnByIndex(i).getType();
                switch (type) {
                    case BINARY:
                        values.setField(i, name, row.getBinary(i));
                        break;
                    case STRING:
                        values.setField(i, name, row.getString(i));
                        break;
                    case BOOL:
                        values.setField(i, name, row.getBoolean(i));
                        break;
                    case DOUBLE:
                        values.setField(i, name, row.getDouble(i));
                        break;
                    case FLOAT:
                        values.setField(i, name, row.getFloat(i));
                        break;
                    case INT8:
                        values.setField(i, name, row.getByte(i));
                        break;
                    case INT16:
                        values.setField(i, name, row.getShort(i));
                        break;
                    case INT32:
                        values.setField(i, name, row.getInt(i));
                        break;
                    case INT64:
                    case UNIXTIME_MICROS:
                        values.setField(i, name, row.getLong(i));
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal var type: " + type);
                }
            }
        }
        return values;
    }


    public static Operation toOperation(KuduTable table, KuduConnector.WriteMode writeMode, KuduRow row) {
        final Operation operation = toOperation(table, writeMode);
        final PartialRow partialRow = operation.getRow();

        Schema schema = table.getSchema();
        List<ColumnSchema> columns = schema.getColumns();

        for (int i = 0; i < columns.size(); i++) {
            String columnName = schema.getColumnByIndex(i).getName();
            Object value = row.getField(i);
            if (value == null) {
                partialRow.setNull(columnName);
            } else {
                Type type = schema.getColumnByIndex(i).getType();
                switch (type) {
                    case STRING:
                        partialRow.addString(columnName, (String) value);
                        break;
                    case FLOAT:
                        partialRow.addFloat(columnName, (Float) value);
                        break;
                    case INT8:
                        partialRow.addByte(columnName, (Byte) value);
                        break;
                    case INT16:
                        partialRow.addShort(columnName, (Short) value);
                        break;
                    case INT32:
                        partialRow.addInt(columnName, (Integer) value);
                        break;
                    case INT64:
                        partialRow.addLong(columnName, (Long) value);
                        break;
                    case DOUBLE:
                        partialRow.addDouble(columnName, (Double) value);
                        break;
                    case BOOL:
                        partialRow.addBoolean(columnName, (Boolean) value);
                        break;
                    case UNIXTIME_MICROS:
                        //*1000 to correctly create date on kudu
                        partialRow.addLong(columnName, ((Long) value) * 1000);
                        break;
                    case BINARY:
                        partialRow.addBinary(columnName, (byte[]) value);
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal var type: " + type);
                }
            }
        }
        return operation;
    }

    public static Operation toOperation(KuduTable table, KuduConnector.WriteMode writeMode) {
        switch (writeMode) {
            case INSERT: return table.newInsert();
            case UPDATE: return table.newUpdate();
            case UPSERT: return table.newUpsert();
        }
        return table.newUpsert();
    }

}
