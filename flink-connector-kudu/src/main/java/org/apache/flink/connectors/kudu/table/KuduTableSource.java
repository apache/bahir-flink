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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.kudu.batch.KuduRowInputFormat;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.LimitableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

public class KuduTableSource implements StreamTableSource<Row>, LimitableTableSource<Row>, ProjectableTableSource<Row> {

    private final KuduReaderConfig.Builder configBuilder;
    private final KuduTableInfo tableInfo;
    private final TableSchema flinkSchema;
    private final String[] projectedFields;

    public KuduTableSource(KuduReaderConfig.Builder configBuilder, KuduTableInfo tableInfo, TableSchema flinkSchema, String[] projectedFields) {
        this.configBuilder = configBuilder;
        this.tableInfo = tableInfo;
        this.flinkSchema = flinkSchema;
        this.projectedFields = projectedFields;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {

        KuduRowInputFormat inputFormat = new KuduRowInputFormat(configBuilder.build(), tableInfo, Lists.newArrayList(projectedFields));

        return env.createInput(inputFormat, (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(getProducedDataType())).name(explainSource());
    }

    @Override
    public TableSchema getTableSchema() {
        return flinkSchema;
    }

    @Override
    public DataType getProducedDataType() {
        if (projectedFields.length == 0) {
            return flinkSchema.toRowDataType();
        } else {
            DataTypes.Field[] fields = new DataTypes.Field[projectedFields.length];
            for (int i = 0; i < fields.length; i++) {
                String fieldName = projectedFields[i];
                fields[i] = DataTypes.FIELD(
                        fieldName,
                        flinkSchema
                                .getTableColumn(fieldName)
                                .get()
                                .getType()
                );
            }
            return DataTypes.ROW(fields);
        }
    }

    @Override
    public boolean isBounded() {
        return true;
    }

    @Override
    public boolean isLimitPushedDown() {
        return true;
    }

    @Override
    public TableSource<Row> applyLimit(long l) {
        return new KuduTableSource(configBuilder.setRowLimit((int) l), tableInfo, flinkSchema, projectedFields);
    }

    @Override
    public TableSource<Row> projectFields(int[] ints) {
        String[] fieldNames = new String[ints.length];
        RowType producedDataType = (RowType) getProducedDataType().getLogicalType();
        List<String> prevFieldNames = producedDataType.getFieldNames();
        for (int i = 0; i < ints.length; i++) {
            fieldNames[i] = prevFieldNames.get(ints[i]);
        }
        return new KuduTableSource(configBuilder, tableInfo, flinkSchema, fieldNames);
    }

    @Override
    public String explainSource() {
        return "KuduStreamTableSource[schema=" + Arrays.toString(getTableSchema().getFieldNames())
                + ", projectFields=" + Arrays.toString(projectedFields) + "]";
    }
}
