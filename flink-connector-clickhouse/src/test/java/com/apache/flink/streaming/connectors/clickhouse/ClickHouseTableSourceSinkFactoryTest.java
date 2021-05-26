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

package com.apache.flink.streaming.connectors.clickhouse;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.junit.Test;

import java.util.Map;

import static com.apache.flink.table.descriptors.ClickHouseValidator.*;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.junit.Assert.assertEquals;

public class ClickHouseTableSourceSinkFactoryTest {


    @Test
    public void testCreateStreamTableSink() throws Exception {
        String connectorVersion = "1";
        String connectorAddress = "jdbc:clickhouse://localhost:8123/default";
        String connectorDatabase = "qtt";
        String connectorTable = "insert_test";
        String connectorUserName = "admin";
        String connectorPassWord = "admin";
        String connectorCommitBatchSize = "1";
        String connectorCommitPadding = "1";
        String connectorCommitRetryAttempts = "3";
        String connectorCommitRetryInterval = "3000";
        String connectorCommitIgnoreError = "false";

        DescriptorProperties properties = new DescriptorProperties();
        properties.putString(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_CLICKHOUSE);
        properties.putString(CONNECTOR_VERSION, connectorVersion);
        properties.putString(CONNECTOR_ADRESS, connectorAddress);
        properties.putString(CONNECTOR_DATABASE, connectorDatabase);
        properties.putString(CONNECTOR_TABLE, connectorTable);
        properties.putString(CONNECTOR_USERNAME, connectorUserName);
        properties.putString(CONNECTOR_PASSWORD, connectorPassWord);
        properties.putString(CONNECTOR_COMMIT_BATCH_SIZE, connectorCommitBatchSize);
        properties.putString(CONNECTOR_COMMIT_PADDING, connectorCommitPadding);
        properties.putString(CONNECTOR_COMMIT_RETRY_ATTEMPTS, connectorCommitRetryAttempts);
        properties.putString(CONNECTOR_COMMIT_RETRY_INTERVAL, connectorCommitRetryInterval);
        properties.putString(CONNECTOR_COMMIT_IGNORE_ERROR, connectorCommitIgnoreError);

        Schema schema = new Schema().field("s", DataTypes.STRING()).field("d", DataTypes.BIGINT());
        Map<String, String> stringStringMap = schema.toProperties();

        properties.putProperties(stringStringMap);

        ClickHouseTableSourceSinkFactory factory = new ClickHouseTableSourceSinkFactory();
        ClickHouseTableSink actualClickHouseTableSink = (ClickHouseTableSink) factory.createStreamTableSink(properties.asMap());

        TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(properties.getTableSchema(SCHEMA));
        ClickHouseTableSink expectedClickHouseTableSink = new ClickHouseTableSink(connectorAddress,
                connectorUserName,
                connectorPassWord,
                connectorDatabase,
                connectorTable,
                tableSchema,
                Integer.valueOf(connectorCommitBatchSize),
                Long.valueOf(connectorCommitPadding),
                Integer.valueOf(connectorCommitRetryAttempts),
                Long.valueOf(connectorCommitRetryInterval),
                Boolean.valueOf(connectorCommitIgnoreError)
        );

        assertEquals(expectedClickHouseTableSink, actualClickHouseTableSink);
    }


}
