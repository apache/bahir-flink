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
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

/**
 * @author weijunhao
 * @date 2021/3/23 16:29
 */
public class KuduDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "kudu";
    public static final ConfigOption<String> MASTER = ConfigOptions.key("master")
            .stringType().noDefaultValue().withDescription("the kudu master address.");
    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
            .stringType().noDefaultValue().withDescription("the kudu table name.");
    public static final ConfigOption<String> OPERATION = ConfigOptions.key("operation")
            .stringType().noDefaultValue().withDescription("the kudu operation type.");


    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        String tableName = config.get(TABLE_NAME);
        String master = config.get(MASTER);
        KuduOperation operation = getOperation(config.get(OPERATION));
        helper.validate();
        TableSchema schema = context.getCatalogTable().getSchema();
        KuduTableInfo kuduTableInfo = KuduTableInfo.forTable(tableName);
        KuduWriterConfig kuduWriterConfig = KuduWriterConfig.Builder.newInstance(master).build();
        RowOperationMapper rowOperationMapper = new RowOperationMapper(schema.getFieldNames(), operation);
        KuduDynamicTableSink sink = new KuduDynamicTableSink(schema, kuduTableInfo, kuduWriterConfig, rowOperationMapper);
        return sink;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        throw new IllegalArgumentException("not support now.");
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
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
