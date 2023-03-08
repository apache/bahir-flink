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

package org.apache.flink.connectors.kudu.connector.convertor;

import org.apache.flink.table.data.*;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.RowResult;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Transform the Kudu RowResult object into a Flink RowData object
 */
public class RowResultRowDataConvertor implements RowResultConvertor<RowData> {
    @Override
    public RowData convertor(RowResult row) {
        Schema schema = row.getColumnProjection();
        GenericRowData values = new GenericRowData(schema.getColumnCount());
        schema.getColumns().forEach(column -> {
            String name = column.getName();
            Type type = column.getType();
            int pos = schema.getColumnIndex(name);
            if (Objects.isNull(type)) {
                throw new IllegalArgumentException("columnName:" + name);
            }
            if (row.isNull(name)){
                return;
            }
            switch (type) {
                case DECIMAL:
                    BigDecimal decimal = row.getDecimal(name);
                    values.setField(pos, DecimalData.fromBigDecimal(decimal, decimal.precision(), decimal.scale()));
                    break;
                case UNIXTIME_MICROS:
                    values.setField(pos, TimestampData.fromTimestamp(row.getTimestamp(name)));
                    break;
                case DOUBLE:
                    values.setField(pos, row.getDouble(name));
                    break;
                case STRING:
                    Object value = row.getObject(name);
                    values.setField(pos, StringData.fromString(Objects.nonNull(value) ? value.toString() : ""));
                    break;
                case BINARY:
                    values.setField(pos, row.getBinary(name));
                    break;
                case FLOAT:
                    values.setField(pos, row.getFloat(name));
                    break;
                case INT64:
                    values.setField(pos, row.getLong(name));
                    break;
                case INT32:
                case INT16:
                case INT8:
                    values.setField(pos, row.getInt(name));
                    break;
                case BOOL:
                    values.setField(pos, row.getBoolean(name));
                    break;
                default:
                    throw new IllegalArgumentException("columnName:" + name + ",type:" + type.getName() + "not support!");
            }
        });
        return values;
    }
}
