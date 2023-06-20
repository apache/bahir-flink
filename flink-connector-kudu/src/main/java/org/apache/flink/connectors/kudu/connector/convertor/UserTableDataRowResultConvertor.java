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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connectors.kudu.connector.configuration.UserTableDataTypeDetail;
import org.apache.flink.connectors.kudu.connector.convertor.builder.UserTableDataTypeBuilder;
import org.apache.flink.connectors.kudu.connector.convertor.parser.UserTableDataTypeParser;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.RowResult;

import java.sql.Timestamp;

@PublicEvolving
/**
 * Generic table data convertor which will convert the Kudu Row to the user defined Java type
 *
 * @param <T> The mapped Java type against the Kudu table.
 */
public class UserTableDataRowResultConvertor<T> implements RowResultConvertor<T> {
    private UserTableDataTypeDetail userTableDataTypeDetail;

    public UserTableDataRowResultConvertor(Class<T> userTableDataType) throws Exception {
        this.userTableDataTypeDetail = UserTableDataTypeParser.getInstance().parse(userTableDataType);
    }

    public UserTableDataTypeDetail getUserTableDataTypeDetail() {
        return userTableDataTypeDetail;
    }

    public void setUserTableDataTypeDetail(UserTableDataTypeDetail userTableDataTypeDetail) {
        this.userTableDataTypeDetail = userTableDataTypeDetail;
    }

    @Override
    public T convertor(RowResult row) {
        T newUserTypeInst;
        try {
            newUserTypeInst = (T)userTableDataTypeDetail.getUserTableDataTypeConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Fail to initialize the UserTableData instance: ", e);
        }

        UserTableDataTypeBuilder userTableDataTypeBuilder = new UserTableDataTypeBuilder(userTableDataTypeDetail);
        Schema schema = row.getColumnProjection();
        for (ColumnSchema columnSchema : schema.getColumns()) {
            String colName = columnSchema.getName();
            Type colType = columnSchema.getType();

            if (row.isNull(colName)) {
                continue;
            }

            switch (colType) {
                case INT64:
                    userTableDataTypeBuilder.build(newUserTypeInst, colName, new Long[]{row.getLong(colName)});
                    break;
                case INT8:
                    userTableDataTypeBuilder.build(newUserTypeInst, colName, new Byte[]{row.getByte(colName)});
                    break;
                case INT16:
                    userTableDataTypeBuilder.build(newUserTypeInst, colName, new Short[]{row.getShort(colName)});
                    break;
                case INT32:
                    userTableDataTypeBuilder.build(newUserTypeInst, colName, new Integer[]{row.getInt(colName)});
                    break;
                case STRING:
                    userTableDataTypeBuilder.build(newUserTypeInst, colName, new String[]{row.getString(colName)});
                    break;
                case UNIXTIME_MICROS:
                    userTableDataTypeBuilder.build(newUserTypeInst, colName, new Timestamp[]{row.getTimestamp(colName)});
                    break;
                case DOUBLE:
                    userTableDataTypeBuilder.build(newUserTypeInst, colName, new Double[]{row.getDouble(colName)});
                    break;
                case FLOAT:
                    userTableDataTypeBuilder.build(newUserTypeInst, colName, new Float[]{row.getFloat(colName)});
                    break;
                case BOOL:
                    userTableDataTypeBuilder.build(newUserTypeInst, colName, new Boolean[]{row.getBoolean(colName)});
                    break;
                default:
                    throw new IllegalArgumentException("Illegal column name: " + colName);
            }
        }

        return newUserTypeInst;
    }
}
