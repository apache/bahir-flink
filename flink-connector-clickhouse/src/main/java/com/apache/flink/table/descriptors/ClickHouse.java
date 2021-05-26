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

import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Map;

import static com.apache.flink.table.descriptors.ClickHouseValidator.*;

/**
 * Connector descriptor for ClickHouse
 */
public class ClickHouse extends ConnectorDescriptor {

    private DescriptorProperties properties = new DescriptorProperties();

    public ClickHouse() {
        super(CONNECTOR_TYPE_VALUE_CLICKHOUSE, 1, false);
    }

    /**
     * Set the ClickHouse version to be used .Required.
     *
     * @param version ClickHouse version . E.g. , "1.0.0"
     * @return
     */
    public ClickHouse version(String version) {
        properties.putString(CONNECTOR_VERSION, version);
        return this;
    }
    /**
     * Set the ClickHouse address to connect the  ClickHouse cluster .Required.
     *
     * @param address ClickHouse address. E.g. , "jdbc:clickhouse://localhost:8123"
     * @return
     */
    public ClickHouse address(String address) {
        properties.putString(CONNECTOR_ADRESS, address);
        return this;
    }

    public ClickHouse database(String database) {
        properties.putString(CONNECTOR_DATABASE, database);
        return this;
    }

    public ClickHouse table(String table) {
        properties.putString(CONNECTOR_TABLE, table);
        return this;
    }

    public ClickHouse username(String username) {
        properties.putString(CONNECTOR_USERNAME, username);
        return this;
    }

    public ClickHouse password(String password) {
        properties.putString(CONNECTOR_PASSWORD, password);
        return this;
    }

    public ClickHouse batchSize(Integer batchSize) {
        properties.putInt(CONNECTOR_COMMIT_BATCH_SIZE, batchSize);
        return this;
    }

    public ClickHouse padding(Long padding) {
        properties.putLong(CONNECTOR_COMMIT_PADDING, padding);
        return this;
    }

    public ClickHouse retryAttempts(Integer attempts) {
        properties.putInt(CONNECTOR_COMMIT_RETRY_ATTEMPTS, attempts);
        return this;
    }

    public ClickHouse retryInterval(Long interval) {
        properties.putLong(CONNECTOR_COMMIT_RETRY_INTERVAL, interval);
        return this;
    }

    public ClickHouse ignoreError(Boolean ignore) {
        properties.putBoolean(CONNECTOR_COMMIT_IGNORE_ERROR, ignore);
        return this;
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        return properties.asMap();
    }
}
