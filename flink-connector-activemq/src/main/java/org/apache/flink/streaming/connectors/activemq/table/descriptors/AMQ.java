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

import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Map;

import static org.apache.flink.streaming.connectors.activemq.table.descriptors.AMQValidator.*;

/**
 * Connector descriptor for Apache ActiveMQ
 */
public class AMQ extends ConnectorDescriptor {
    private DescriptorProperties properties = new DescriptorProperties();

    public AMQ() {
        super(CONNECTOR_TYPE_VALUE_AMQ, 1, true);
    }

    /**
     * Sets the <a href="http://activemq.apache.org/configuring-transports.html"> connection
     * URL</a> used to connect to the ActiveMQ broker.
     *
     * @param brokerUrl URL used to connect to the ActiveMQ broker.
     */
    public AMQ brokerURL(String brokerUrl) {
        properties.putString(CONNECTOR_BROKER_URL, brokerUrl);
        return this;
    }

    /**
     * Sets the username used to connect to the ActiveMQ broker.
     *
     * @param username username used to connect to the ActiveMQ broker.
     */
    public AMQ username(String username) {
        properties.putString(CONNECTOR_USERNAME, username);
        return this;
    }

    /**
     * Sets the password used to connect to the ActiveMQ broker.
     *
     * @param password password used to connect to the ActiveMQ broker.
     */
    public AMQ password(String password) {
        properties.putString(CONNECTOR_PASSWORD, password);
        return this;
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        return properties.asMap();
    }
}
