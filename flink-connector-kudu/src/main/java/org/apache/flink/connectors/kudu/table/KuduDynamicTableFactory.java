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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper.KuduOperation;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.table.KuduDynamicTableSource.LookupOptions;
import org.apache.flink.connectors.kudu.table.KuduDynamicTableSource.ScanOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * @author weijunhao
 * @date 2021/3/23 16:29
 */
public class KuduDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final Logger log = LoggerFactory.getLogger(KuduDynamicTableFactory.class);

    public static final String IDENTIFIER = "kudu";
    public static final ConfigOption<String> MASTER = ConfigOptions.key("master")
            .stringType().noDefaultValue().withDescription("the kudu master address.");
    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
            .stringType().noDefaultValue().withDescription("the kudu table name.");
    public static final ConfigOption<String> OPERATION = ConfigOptions.key("operation")
            .stringType().noDefaultValue().withDescription("the kudu operation type.");
    public static final ConfigOption<Long> INERVAL = ConfigOptions.key("inerval")
            .longType().defaultValue(5 * 60 * 1000L).withDescription("the kudu query inerval.");
    public static final ConfigOption<Long> MAX_SIZE = ConfigOptions.key("lookup.cache.max-size")
            .longType().defaultValue(-1L).withDescription("the kudu query cache max size.");
    public static final ConfigOption<Long> EXPIRE_MS = ConfigOptions.key("lookup.cache.expire-ms")
            .longType().defaultValue(-1L).withDescription("the kudu query cache expire millsecond.");
    public static final ConfigOption<Integer> MAX_RETRIES = ConfigOptions.key("lookup.cache.max-retries")
            .intType().defaultValue(3).withDescription("the kudu query cache max retry times.");


    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        String tableName = config.get(TABLE_NAME);
        String master = config.get(MASTER);
        TableSchema schema = context.getCatalogTable().getSchema();
        KuduTableInfo kuduTableInfo = KuduTableInfo.forTable(tableName);
        KuduWriterConfig kuduWriterConfig = KuduWriterConfig.Builder.newInstance(master).build();
        UpsertOperationMapper upsertOperationMapper = new UpsertOperationMapper(schema.getFieldNames());
        KuduDynamicTableSink sink = new KuduDynamicTableSink(schema, kuduTableInfo, kuduWriterConfig, upsertOperationMapper);
        return sink;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        String master = config.get(MASTER);
        String tableName = config.get(TABLE_NAME);
        Long inerval = config.get(INERVAL);
        Long cacheMaxSize = config.get(MAX_SIZE);
        Long cacheExpireMs = config.get(EXPIRE_MS);
        Integer maxRetryTimes = config.get(MAX_RETRIES);
        LookupOptions lookupOptions = new LookupOptions(cacheMaxSize, cacheExpireMs, maxRetryTimes);
        ScanOptions scanOptions = new ScanOptions(inerval);
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new KuduDynamicTableSource(master, tableName, physicalSchema, lookupOptions, scanOptions);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TABLE_NAME);
        options.add(MASTER);
        options.add(INERVAL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OPERATION);
        return options;
    }


    public KuduOperation getOperation(String text) {
        switch (text) {
            case "insert":
                return KuduOperation.INSERT;
            case "update":
                return KuduOperation.UPDATE;
            case "upsert":
                return KuduOperation.UPSERT;
            case "delete":
                return KuduOperation.DELETE;
            default:
                throw new IllegalArgumentException(String.format("not support the operation type:%s.\nkudu operation support insert,update,upsert,delete.", text));
        }
    }


}
