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

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

/**
 * The validator for ClickHouse
 */

public class ClickHouseValidator extends ConnectorDescriptorValidator {

    public static final String CONNECTOR_TYPE_VALUE_CLICKHOUSE = "clickhouse";
    public static final String CONNECTOR_ADRESS = "connector.address";
    public static final String CONNECTOR_DATABASE = "connector.database";
    public static final String CONNECTOR_TABLE = "connector.table";
    public static final String CONNECTOR_USERNAME = "connector.username";
    public static final String CONNECTOR_PASSWORD = "connector.password";
    public static final String CONNECTOR_COMMIT_BATCH_SIZE = "connector.commit.batch.size";
    public static final String CONNECTOR_COMMIT_PADDING = "connector.commit.padding";
    public static final String CONNECTOR_COMMIT_RETRY_ATTEMPTS = "connector.commit.retry.attempts";
    public static final String CONNECTOR_COMMIT_RETRY_INTERVAL = "connector.commit.retry.interval";
    public static final String CONNECTOR_COMMIT_IGNORE_ERROR = "connector.commit.ignore.error";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        validateComonProperties(properties);
    }

    private void validateComonProperties(DescriptorProperties properties) {

        properties.validateString(CONNECTOR_ADRESS, false, 1);
        properties.validateString(CONNECTOR_DATABASE, false, 1);
        properties.validateString(CONNECTOR_TABLE, false, 1);
        properties.validateString(CONNECTOR_USERNAME, false,1);
        properties.validateString(CONNECTOR_PASSWORD, false,1);
        properties.validateInt(CONNECTOR_COMMIT_BATCH_SIZE, true);
        properties.validateString(CONNECTOR_USERNAME, true);
        properties.validateInt(CONNECTOR_COMMIT_PADDING, true);
    }
}
