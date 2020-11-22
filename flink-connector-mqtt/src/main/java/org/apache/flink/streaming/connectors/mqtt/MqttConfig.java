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

package org.apache.flink.streaming.connectors.mqtt;

import java.io.Serializable;
import java.util.Properties;

/**
 * mqtt config, include required / optional property keys.
 */
public class MqttConfig implements Serializable {

    // ----- Required property keys
    protected static final String SERVER_URL = "server.url";
    protected static final String USERNAME = "username";
    protected static final String PASSWORD = "password";

    // ------ Optional property keys
    protected static final String CLIENT_ID = "client.id";
    protected static final String CLEAN_SESSION = "clean.session";
    protected static final String RETAINED = "retained";
    protected static final String QOS = "qos";

    protected static void checkProperty(Properties properties, String key) {
        if (!properties.containsKey(key)) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }

}
