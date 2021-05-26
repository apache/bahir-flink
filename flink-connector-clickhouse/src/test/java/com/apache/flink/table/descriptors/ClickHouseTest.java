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

package com.apache.flink.table.descriptors;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.apache.flink.table.descriptors.ClickHouseValidator.*;
import static org.junit.Assert.assertEquals;

public class ClickHouseTest {

    @Test
    public void testToConnectorProperties() throws Exception {
        String connectorVersion = "1";
        String connectorAddress = "jdbc:clickhouse://localhost:8123/default";
        String connectorDatabase = "qtt";
        String connectorTable = "insert_test";
        String connectorUserName = "admin";
        String connectorPassWord = "admin";
        String connectorCommitBatchSize = "1";
        String connectorCommitPadding = "2000";
        String connectorCommitRetryAttempts = "3";
        String connectorCommitRetryInterval = "3000";
        String connectorCommitIgnoreError = "false";
        HashMap<String, String> expectedMap = new HashMap<>();
        expectedMap.put(CONNECTOR_VERSION, connectorVersion);
        expectedMap.put(CONNECTOR_ADRESS, connectorAddress);
        expectedMap.put(CONNECTOR_DATABASE, connectorDatabase);
        expectedMap.put(CONNECTOR_TABLE, connectorTable);
        expectedMap.put(CONNECTOR_USERNAME, connectorUserName);
        expectedMap.put(CONNECTOR_PASSWORD, connectorPassWord);
        expectedMap.put(CONNECTOR_COMMIT_BATCH_SIZE, connectorCommitBatchSize);
        expectedMap.put(CONNECTOR_COMMIT_PADDING, connectorCommitPadding);
        expectedMap.put(CONNECTOR_COMMIT_RETRY_ATTEMPTS, connectorCommitRetryAttempts);
        expectedMap.put(CONNECTOR_COMMIT_RETRY_INTERVAL, connectorCommitRetryInterval);
        expectedMap.put(CONNECTOR_COMMIT_IGNORE_ERROR, connectorCommitIgnoreError);

        ClickHouse clickhouse = new ClickHouse()
                .version(connectorVersion)
                .address(connectorAddress)
                .database(connectorDatabase)
                .table(connectorTable)
                .username(connectorUserName)
                .password(connectorPassWord)
                .batchSize(Integer.valueOf(connectorCommitBatchSize))
                .padding(Long.valueOf(connectorCommitPadding))
                .retryAttempts(Integer.valueOf(connectorCommitRetryAttempts))
                .retryInterval(Long.valueOf(connectorCommitRetryInterval))
                .ignoreError(Boolean.valueOf(connectorCommitIgnoreError));
        Map<String, String> actualMap = clickhouse.toConnectorProperties();

        assertEquals(expectedMap, actualMap);


    }

}
