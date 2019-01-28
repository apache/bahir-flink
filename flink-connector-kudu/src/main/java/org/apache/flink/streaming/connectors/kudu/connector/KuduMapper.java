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


import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

final class KuduMapper {

    private KuduMapper() { }

    static KuduRow toKuduRow(RowResult row) {
        Schema schema = row.getColumnProjection();

        KuduRow values = new KuduRow(schema.getColumnCount());
        schema.getColumns().forEach(column -> {
            String name = column.getName();
            int pos = schema.getColumnIndex(name);
            if(row.isNull(name)) {
                values.setField(pos, name, null);
            } else {
                Type type = column.getType();
                switch (type) {
                    case BINARY:
                        values.setField(pos, name, row.getBinary(name));
                        break;
                    case STRING:
                        values.setField(pos, name, row.getString(name));
                        break;
                    case BOOL:
                        values.setField(pos, name, row.getBoolean(name));
                        break;
                    case DOUBLE:
                        values.setField(pos, name, row.getDouble(name));
                        break;
                    case FLOAT:
                        values.setField(pos, name, row.getFloat(name));
                        break;
                    case INT8:
                        values.setField(pos, name, row.getByte(name));
                        break;
                    case INT16:
                        values.setField(pos, name, row.getShort(name));
                        break;
                    case INT32:
                        values.setField(pos, name, row.getInt(name));
                        break;
                    case INT64:
                        values.setField(pos, name, row.getLong(name));
                        break;
                    case UNIXTIME_MICROS:
                        values.setField(pos, name, row.getLong(name) / 1000);
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal var type: " + type);
                }
            }
        });
        return values;
    }


    static Operation toOperation(KuduTable table, KuduConnector.WriteMode writeMode, KuduRow row) {
        final Operation operation = toOperation(table, writeMode);
        final PartialRow partialRow = operation.getRow();

        table.getSchema().getColumns().forEach(column -> {
            String columnName = column.getName();
            Object value = row.getField(column.getName());

            if (value == null) {
                partialRow.setNull(columnName);
            } else {
                Type type = column.getType();
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
        });
        return operation;
    }

    static Operation toOperation(KuduTable table, KuduConnector.WriteMode writeMode) {
        switch (writeMode) {
            case INSERT: return table.newInsert();
            case UPDATE: return table.newUpdate();
            case UPSERT: return table.newUpsert();
        }
        return table.newUpsert();
    }

}
