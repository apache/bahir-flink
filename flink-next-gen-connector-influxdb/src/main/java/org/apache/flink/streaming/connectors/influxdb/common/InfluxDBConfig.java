/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.influxdb.common;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import java.io.Serializable;
import lombok.Builder;
import lombok.NonNull;

/** A Configuration wrapper for InfluxDB Java Client. */
@Builder
public class InfluxDBConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @NonNull private final String url;
    @NonNull private final String username;
    @NonNull private final String password;
    @NonNull private final String bucket;
    @NonNull private final String organization;

    public InfluxDBClient getClient() {
        final InfluxDBClientOptions influxDBClientOptions =
                InfluxDBClientOptions.builder()
                        .url(this.url)
                        .authenticate(this.username, this.password.toCharArray())
                        .bucket(this.bucket)
                        .org(this.organization)
                        .build();
        return InfluxDBClientFactory.create(influxDBClientOptions);
    }
}
