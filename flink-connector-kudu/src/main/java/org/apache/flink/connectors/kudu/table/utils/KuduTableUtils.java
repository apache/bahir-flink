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

package org.apache.flink.connectors.kudu.table.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.kudu.connector.ColumnSchemasFactory;
import org.apache.flink.connectors.kudu.connector.CreateTableOptionsFactory;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.table.KuduTableFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.connectors.kudu.table.KuduTableFactory.KUDU_HASH_COLS;
import static org.apache.flink.connectors.kudu.table.KuduTableFactory.KUDU_PRIMARY_KEY_COLS;

public class KuduTableUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KuduTableUtils.class);

    public static KuduTableInfo createTableInfo(String tableName, TableSchema schema, Map<String, String> props) {
        // Since KUDU_HASH_COLS is a required property for table creation, we use it to infer whether to create table
        boolean createIfMissing = props.containsKey(KUDU_HASH_COLS);

        KuduTableInfo tableInfo = KuduTableInfo.forTable(tableName);

        if (createIfMissing) {

            List<Tuple2<String, DataType>> columns = getSchemaWithSqlTimestamp(schema)
                    .getTableColumns()
                    .stream()
                    .map(tc -> Tuple2.of(tc.getName(), tc.getType()))
                    .collect(Collectors.toList());

            List<String> keyColumns = getPrimaryKeyColumns(props, schema);
            ColumnSchemasFactory schemasFactory = () -> toKuduConnectorColumns(columns, keyColumns);
            List<String> hashColumns = getHashColumns(props);
            int replicas = Optional.ofNullable(props.get(KuduTableFactory.KUDU_REPLICAS)).map(Integer::parseInt).orElse(1);

            CreateTableOptionsFactory optionsFactory = () -> new CreateTableOptions()
                    .setNumReplicas(replicas)
                    .addHashPartitions(hashColumns, replicas * 2);

            tableInfo.createTableIfNotExists(schemasFactory, optionsFactory);
        } else {
            LOG.debug("Property {} is missing, assuming the table is already created.", KUDU_HASH_COLS);
        }

        return tableInfo;
    }

    public static List<ColumnSchema> toKuduConnectorColumns(List<Tuple2<String, DataType>> columns, Collection<String> keyColumns) {
        return columns.stream()
                .map(t -> {
                            ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema
                                    .ColumnSchemaBuilder(t.f0, KuduTypeUtils.toKuduType(t.f1))
                                    .key(keyColumns.contains(t.f0))
                                    .nullable(!keyColumns.contains(t.f0) && t.f1.getLogicalType().isNullable());
                            if(t.f1.getLogicalType() instanceof DecimalType) {
                                DecimalType decimalType = ((DecimalType) t.f1.getLogicalType());
                                builder.typeAttributes(new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
                                    .precision(decimalType.getPrecision())
                                    .scale(decimalType.getScale())
                                    .build());
                            }
                            return builder.build();
                        }
                ).collect(Collectors.toList());
    }

    public static TableSchema kuduToFlinkSchema(Schema schema) {
        TableSchema.Builder builder = TableSchema.builder();

        for (ColumnSchema column : schema.getColumns()) {
            DataType flinkType = KuduTypeUtils.toFlinkType(column.getType(), column.getTypeAttributes()).nullable();
            builder.field(column.getName(), flinkType);
        }

        return builder.build();
    }

    public static List<String> getPrimaryKeyColumns(Map<String, String> tableProperties, TableSchema tableSchema) {
        return tableProperties.containsKey(KUDU_PRIMARY_KEY_COLS) ? Arrays.asList(tableProperties.get(KUDU_PRIMARY_KEY_COLS).split(",")) : tableSchema.getPrimaryKey().get().getColumns();
    }

    public static List<String> getHashColumns(Map<String, String> tableProperties) {
        return Lists.newArrayList(tableProperties.get(KUDU_HASH_COLS).split(","));
    }

    public static TableSchema getSchemaWithSqlTimestamp(TableSchema schema) {
        TableSchema.Builder builder = new TableSchema.Builder();
        TableSchemaUtils.getPhysicalSchema(schema).getTableColumns().forEach(
                tableColumn -> {
                    if (tableColumn.getType().getLogicalType() instanceof TimestampType) {
                        builder.field(tableColumn.getName(), tableColumn.getType().bridgedTo(Timestamp.class));
                    } else {
                        builder.field(tableColumn.getName(), tableColumn.getType());
                    }
                });
        return builder.build();
    }
}
