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
package org.apache.flink.connectors.kudu.table.dynamic;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.table.dynamic.KuduDynamicTableSink;
import org.apache.flink.connectors.kudu.table.dynamic.KuduDynamicTableSource;
import org.apache.flink.connectors.kudu.table.function.lookup.KuduLookupOptions;
import org.apache.flink.connectors.kudu.table.utils.KuduTableUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.kudu.shaded.com.google.common.collect.Sets;

import java.util.Optional;
import java.util.Set;

/**
 * Factory for creating configured instances of {@link KuduDynamicTableSource}/{@link KuduDynamicTableSink} in
 * a stream environment.
 */
public class KuduDynamicTableSourceSinkFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final String IDENTIFIER = "kudu";
    public static final ConfigOption<String> KUDU_TABLE = ConfigOptions
            .key("kudu.table")
            .stringType()
            .noDefaultValue()
            .withDescription("kudu's table name");

    public static final ConfigOption<String> KUDU_MASTERS =
            ConfigOptions
                    .key("kudu.masters")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's master server address");


    public static final ConfigOption<String> KUDU_HASH_COLS =
            ConfigOptions
                    .key("kudu.hash-columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's hash columns");

    public static final ConfigOption<Integer> KUDU_REPLICAS =
            ConfigOptions
                    .key("kudu.replicas")
                    .intType()
                    .defaultValue(3)
                    .withDescription("kudu's replica nums");

    public static final ConfigOption<Integer> KUDU_MAX_BUFFER_SIZE =
            ConfigOptions
                    .key("kudu.max-buffer-size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("kudu's max buffer size");

    public static final ConfigOption<Integer> KUDU_FLUSH_INTERVAL =
            ConfigOptions
                    .key("kudu.flush-interval")
                    .intType()
                    .noDefaultValue()
                    .withDescription("kudu's data flush interval");

    public static final ConfigOption<Long> KUDU_OPERATION_TIMEOUT =
            ConfigOptions
                    .key("kudu.operation-timeout")
                    .longType()
                    .noDefaultValue()
                    .withDescription("kudu's operation timeout");

    public static final ConfigOption<Boolean> KUDU_IGNORE_NOT_FOUND =
            ConfigOptions
                    .key("kudu.ignore-not-found")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("if true, ignore all not found rows");

    public static final ConfigOption<Boolean> KUDU_IGNORE_DUPLICATE =
            ConfigOptions
                    .key("kudu.ignore-not-found")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("if true, ignore all dulicate rows");

    /**
     * hash partition bucket nums
     */
    public static final ConfigOption<Integer> KUDU_HASH_PARTITION_NUMS =
            ConfigOptions
                    .key("kudu.hash-partition-nums")
                    .intType()
                    .defaultValue(KUDU_REPLICAS.defaultValue() * 2)
                    .withDescription("kudu's hash partition bucket nums,defaultValue is 2 * replica nums");

    public static final ConfigOption<String> KUDU_PRIMARY_KEY_COLS =
            ConfigOptions
                    .key("kudu.primary-key-columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's primary key,primary key must be ordered");


    public static final ConfigOption<Integer> KUDU_SCAN_ROW_SIZE =
            ConfigOptions
                    .key("kudu.scan.row-size")
                    .intType()
                    .defaultValue(0)
                    .withDescription("kudu's scan row size");

    /**
     * lookup cache config
     */
    public static final ConfigOption<Long> KUDU_LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions
                    .key("kudu.lookup.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("the max number of rows of lookup cache, over this value, the oldest rows will " +
                            "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any" +
                            " of them is " +
                            "specified. Cache is not enabled as default.");

    public static final ConfigOption<Long> KUDU_LOOKUP_CACHE_TTL =
            ConfigOptions
                    .key("kudu.lookup.cache.ttl")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("the cache time to live.");

    public static final ConfigOption<Integer> KUDU_LOOKUP_MAX_RETRIES =
            ConfigOptions
                    .key("kudu.lookup.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the max retry times if lookup database failed.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        ReadableConfig config = getReadableConfig(context);
        String masterAddresses = config.get(KUDU_MASTERS);
        String tableName = config.get(KUDU_TABLE);
        Optional<Long> operationTimeout = config.getOptional(KUDU_OPERATION_TIMEOUT);
        Optional<Integer> flushInterval = config.getOptional(KUDU_FLUSH_INTERVAL);
        Optional<Integer> bufferSize = config.getOptional(KUDU_MAX_BUFFER_SIZE);
        Optional<Boolean> ignoreNotFound = config.getOptional(KUDU_IGNORE_NOT_FOUND);
        Optional<Boolean> ignoreDuplicate = config.getOptional(KUDU_IGNORE_DUPLICATE);
        TableSchema schema = context.getCatalogTable().getSchema();
        TableSchema physicalSchema = KuduTableUtils.getSchemaWithSqlTimestamp(schema);
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, schema,
                context.getCatalogTable().toProperties());

        KuduWriterConfig.Builder configBuilder = KuduWriterConfig.Builder
                .setMasters(masterAddresses);
        operationTimeout.ifPresent(configBuilder::setOperationTimeout);
        flushInterval.ifPresent(configBuilder::setFlushInterval);
        bufferSize.ifPresent(configBuilder::setMaxBufferSize);
        ignoreNotFound.ifPresent(configBuilder::setIgnoreNotFound);
        ignoreDuplicate.ifPresent(configBuilder::setIgnoreDuplicate);
        return new KuduDynamicTableSink(configBuilder, physicalSchema, tableInfo);
    }

    /**
     * get readableConfig
     *
     * @param context
     * @return {@link ReadableConfig}
     */
    private ReadableConfig getReadableConfig(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        return helper.getOptions();
    }


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        ReadableConfig config = getReadableConfig(context);
        String masterAddresses = config.get(KUDU_MASTERS);

        int scanRowSize = config.get(KUDU_SCAN_ROW_SIZE);
        long kuduCacheMaxRows = config.get(KUDU_LOOKUP_CACHE_MAX_ROWS);
        long kuduCacheTtl = config.get(KUDU_LOOKUP_CACHE_TTL);
        int kuduMaxReties = config.get(KUDU_LOOKUP_MAX_RETRIES);

        // 构造kudu lookup options
        KuduLookupOptions kuduLookupOptions = KuduLookupOptions.Builder.options().withCacheMaxSize(kuduCacheMaxRows)
                .withCacheExpireMs(kuduCacheTtl)
                .withMaxRetryTimes(kuduMaxReties)
                .build();

        TableSchema schema = context.getCatalogTable().getSchema();
        TableSchema physicalSchema = KuduTableUtils.getSchemaWithSqlTimestamp(schema);
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(config.get(KUDU_TABLE), schema,
                context.getCatalogTable().toProperties());

        KuduReaderConfig.Builder configBuilder = KuduReaderConfig.Builder
                .setMasters(masterAddresses)
                .setRowLimit(scanRowSize);
        return new KuduDynamicTableSource(configBuilder, tableInfo, physicalSchema, physicalSchema.getFieldNames(),
                kuduLookupOptions);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Sets.newHashSet(KUDU_TABLE, KUDU_MASTERS);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Sets.newHashSet(KUDU_HASH_COLS, KUDU_HASH_PARTITION_NUMS,
                KUDU_PRIMARY_KEY_COLS, KUDU_SCAN_ROW_SIZE, KUDU_REPLICAS,
                KUDU_MAX_BUFFER_SIZE, KUDU_MAX_BUFFER_SIZE, KUDU_OPERATION_TIMEOUT,
                KUDU_IGNORE_NOT_FOUND, KUDU_IGNORE_DUPLICATE,
                //lookup
                KUDU_LOOKUP_CACHE_MAX_ROWS, KUDU_LOOKUP_CACHE_TTL, KUDU_LOOKUP_MAX_RETRIES);
    }
}