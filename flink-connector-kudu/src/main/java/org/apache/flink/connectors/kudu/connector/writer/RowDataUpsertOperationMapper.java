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
package org.apache.flink.connectors.kudu.connector.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

@Internal
public class RowDataUpsertOperationMapper extends AbstractSingleOperationMapper<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(RowDataUpsertOperationMapper.class);


    private static final int MIN_TIME_PRECISION = 0;
    private static final int MAX_TIME_PRECISION = 3;
    private static final int MIN_TIMESTAMP_PRECISION = 0;
    private static final int MAX_TIMESTAMP_PRECISION = 6;

    private LogicalType[] logicalTypes;

    public RowDataUpsertOperationMapper(TableSchema schema) {
        super(schema.getFieldNames());
        logicalTypes = Arrays.stream(schema.getFieldDataTypes())
                .map(DataType::getLogicalType)
                .toArray(LogicalType[]::new);
    }

    @Override
    public Object getField(RowData input, int i) {
        return getFieldValue(input, i);
    }

    public Object getFieldValue(RowData input, int i) {
        if (input == null || input.isNullAt(i)) {
            return null;
        }
        LogicalType fieldType = logicalTypes[i];
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR: {
                StringData data = input.getString(i);
                if (data != null) {
                    return data.toString();
                }
                return null;
            }
            case BOOLEAN:
                return input.getBoolean(i);
            case BINARY:
            case VARBINARY:
                return input.getBinary(i);
            case DECIMAL: {
                DecimalType decimalType = (DecimalType) fieldType;
                final int precision = decimalType.getPrecision();
                final int scale = decimalType.getScale();
                DecimalData data = input.getDecimal(i, precision, scale);
                if (data != null) {
                    return data.toBigDecimal();
                } else {
                    return null;
                }
            }
            case TINYINT:
                return input.getByte(i);
            case SMALLINT:
                return input.getShort(i);
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
                return input.getInt(i);
            case TIME_WITHOUT_TIME_ZONE:
                final int timePrecision = getPrecision(fieldType);
                if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
                    throw new UnsupportedOperationException(
                            String.format("The precision %s of TIME type is out of the range [%s, %s] supported by " +
                                    "kudu connector", timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
                }
                return input.getInt(i);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return input.getLong(i);
            case FLOAT:
                return input.getFloat(i);
            case DOUBLE:
                return input.getDouble(i);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                if (timestampPrecision < MIN_TIMESTAMP_PRECISION || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
                    throw new UnsupportedOperationException(
                            String.format("The precision %s of TIMESTAMP type is out of the range [%s, %s] supported " +
                                            "by " +
                                            "kudu connector", timestampPrecision, MIN_TIMESTAMP_PRECISION,
                                    MAX_TIMESTAMP_PRECISION));
                }
                return input.getTimestamp(i, timestampPrecision).toTimestamp();
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }

    @Override
    public Optional<Operation> createBaseOperation(RowData input, KuduTable table) {
        Optional<Operation> operation = Optional.empty();
        switch (input.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                operation = Optional.of(table.newUpsert());
                break;
            case DELETE:
                operation = Optional.of(table.newDelete());
                break;
        }
        return operation;
    }
}
