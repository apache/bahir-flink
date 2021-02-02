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

package org.apache.flink.streaming.connectors.activemq.table.descriptors;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Arrays;


/**
 * The validator for ActiveMQ
 */
public class AMQValidator extends ConnectorDescriptorValidator {
    public static final String CONNECTOR_TYPE_VALUE_AMQ = "activemq";
    public static final String CONNECTOR_BROKER_URL = "connector.broker-url";
    public static final String CONNECTOR_USERNAME = "connector.username";
    public static final String CONNECTOR_PASSWORD = "connector.password";
    public static final String CONNECTOR_DESTINATION_TYPE = "connector.destination-type";
    public static final String CONNECTOR_DESTINATION_NAME = "connector.destination-name";
    public static final String CONNECTOR_PERSISTENT = "connector.persistent";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_AMQ, false);
        properties.validateString(CONNECTOR_BROKER_URL, false, 1);
        properties.validateString(CONNECTOR_USERNAME, true, 0);
        properties.validateString(CONNECTOR_PASSWORD, true, 0);
        properties.validateEnumValues(CONNECTOR_DESTINATION_TYPE, false,
            Arrays.asList("QUEUE", "TOPIC"));
        properties.validateString(CONNECTOR_DESTINATION_NAME, false, 1);
        validateSinkProperties(properties);
    }

    private void validateSinkProperties(DescriptorProperties properties) {
        properties.validateBoolean(CONNECTOR_PERSISTENT, true);
    }

}
