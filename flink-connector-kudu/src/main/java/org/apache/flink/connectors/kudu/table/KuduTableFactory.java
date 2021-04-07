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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.table.utils.KuduTableUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class KuduTableFactory implements TableSourceFactory<Row>, TableSinkFactory<Tuple2<Boolean, Row>> {

    public static final String KUDU_TABLE = "kudu.table";
    public static final String KUDU_MASTERS = "kudu.masters";
    public static final String KUDU_HASH_COLS = "kudu.hash-columns";
    public static final String KUDU_PRIMARY_KEY_COLS = "kudu.primary-key-columns";
    public static final String KUDU_REPLICAS = "kudu.replicas";
    public static final String KUDU_MAX_BUFFER_SIZE = "kudu.max-buffer-size";
    public static final String KUDU_FLUSH_INTERVAL = "kudu.flush-interval";
    public static final String KUDU_OPERATION_TIMEOUT = "kudu.operation-timeout";
    public static final String KUDU_IGNORE_NOT_FOUND = "kudu.ignore-not-found";
    public static final String KUDU_IGNORE_DUPLICATE = "kudu.ignore-duplicate";
    public static final String KUDU = "kudu";

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, KUDU);
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        properties.add(KUDU_TABLE);
        properties.add(KUDU_MASTERS);
        properties.add(KUDU_HASH_COLS);
        properties.add(KUDU_PRIMARY_KEY_COLS);
        properties.add(KUDU_MAX_BUFFER_SIZE);
        properties.add(KUDU_FLUSH_INTERVAL);
        properties.add(KUDU_OPERATION_TIMEOUT);
        properties.add(KUDU_IGNORE_NOT_FOUND);
        properties.add(KUDU_IGNORE_DUPLICATE);
        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);
        // computed column
        properties.add(SCHEMA + ".#." + EXPR);

        // time attributes
        properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

        // watermark
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE);
        return properties;
    }

    private DescriptorProperties validateTable(CatalogTable table) {
        Map<String, String> properties = table.toProperties();
        checkNotNull(properties.get(KUDU_MASTERS), "Missing required property " + KUDU_MASTERS);

        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        new SchemaValidator(true, false, false).validate(descriptorProperties);
        return descriptorProperties;
    }

    @Override
    public KuduTableSource createTableSource(ObjectPath tablePath, CatalogTable table) {
        validateTable(table);
        String tableName = table.toProperties().getOrDefault(KUDU_TABLE, tablePath.getObjectName());
        return createTableSource(tableName, table.getSchema(), table.getProperties());
    }

    private KuduTableSource createTableSource(String tableName, TableSchema schema, Map<String, String> props) {
        String masterAddresses = props.get(KUDU_MASTERS);
        TableSchema physicalSchema = KuduTableUtils.getSchemaWithSqlTimestamp(schema);
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, schema, props);

        KuduReaderConfig.Builder configBuilder = KuduReaderConfig.Builder
                .setMasters(masterAddresses);

        return new KuduTableSource(configBuilder, tableInfo, physicalSchema, null, null);
    }

    @Override
    public KuduTableSink createTableSink(ObjectPath tablePath, CatalogTable table) {
        validateTable(table);
        String tableName = table.toProperties().getOrDefault(KUDU_TABLE, tablePath.getObjectName());
        return createTableSink(tableName, table.getSchema(), table.toProperties());
    }

    private KuduTableSink createTableSink(String tableName, TableSchema schema, Map<String, String> props) {
        DescriptorProperties properties = new DescriptorProperties();
        properties.putProperties(props);
        String masterAddresses = props.get(KUDU_MASTERS);
        TableSchema physicalSchema = KuduTableUtils.getSchemaWithSqlTimestamp(schema);
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, schema, props);

        KuduWriterConfig.Builder configBuilder = KuduWriterConfig.Builder
                .setMasters(masterAddresses);

        Optional<Long> operationTimeout = properties.getOptionalLong(KUDU_OPERATION_TIMEOUT);
        Optional<Integer> flushInterval = properties.getOptionalInt(KUDU_FLUSH_INTERVAL);
        Optional<Integer> bufferSize = properties.getOptionalInt(KUDU_MAX_BUFFER_SIZE);
        Optional<Boolean> ignoreNotFound = properties.getOptionalBoolean(KUDU_IGNORE_NOT_FOUND);
        Optional<Boolean> ignoreDuplicate = properties.getOptionalBoolean(KUDU_IGNORE_DUPLICATE);

        operationTimeout.ifPresent(time -> configBuilder.setOperationTimeout(time));
        flushInterval.ifPresent(interval -> configBuilder.setFlushInterval(interval));
        bufferSize.ifPresent(size -> configBuilder.setMaxBufferSize(size));
        ignoreNotFound.ifPresent(i -> configBuilder.setIgnoreNotFound(i));
        ignoreDuplicate.ifPresent(i -> configBuilder.setIgnoreDuplicate(i));

        return new KuduTableSink(configBuilder, tableInfo, physicalSchema);
    }
}
