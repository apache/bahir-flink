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

package org.apache.flink.streaming.connectors.influxdb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

/**
 * Sink to save data into a InfluxDB cluster.
 */
public class InfluxDBSink extends RichSinkFunction<InfluxDBPoint> {

    private transient InfluxDB influxDB = null;
    private final String dbName;
    private final String username;
    private final String password;
    private final String host;
    private boolean batchEnabled = true;

    /**
     * Creates a new {@link InfluxDBSink} that connects to the InfluxDB server.
     *
     * @param host     the url to connect to.
     * @param username the username which is used to authorize against the influxDB instance.
     * @param password the password for the username which is used to authorize against the influxDB instance.
     * @param dbName   the database to write to.
     */
    public InfluxDBSink(String host, String username, String password, String dbName) {
        this.host = Preconditions.checkNotNull(host, "host can not be null");
        this.username = Preconditions.checkNotNull(username, "username can not be null");
        this.password = Preconditions.checkNotNull(password, "password can not be null");
        this.dbName = Preconditions.checkNotNull(dbName, "dbName can not be null");
    }

    public InfluxDBSink(String host, String username, String password, String dbName, boolean batchEnabled) {
        this(host, username, password, dbName);
        this.batchEnabled = Preconditions.checkNotNull(batchEnabled, "batchEnabled can not be null");
    }

    /**
     * Initializes the connection to InfluxDB by either cluster or sentinels or single server.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        influxDB = InfluxDBFactory.connect(host, username, password);
        if (!influxDB.databaseExists(dbName)) {
            influxDB.createDatabase(dbName);
        }

        if (batchEnabled) {
            // Flush every 2000 Points, at least every 100ms
            influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Called when new data arrives to the sink, and forwards it to InfluxDB.
     *
     * @param dataPoint {@link InfluxDBPoint}
     */
    @Override
    public void invoke(InfluxDBPoint dataPoint) throws Exception {
        if (StringUtils.isNullOrWhitespaceOnly(dataPoint.getMeasurement())) {
            throw new Exception("No measurement defined");
        }

        Point.Builder builder = Point.measurement(dataPoint.getMeasurement())
                .time(dataPoint.getTimestamp(), TimeUnit.MILLISECONDS);

        if (!CollectionUtil.isNullOrEmpty(dataPoint.getFields())) {
            builder.fields(dataPoint.getFields());
        }

        if (!CollectionUtil.isNullOrEmpty(dataPoint.getTags())) {
            builder.tag(dataPoint.getTags());
        }

        Point point = builder.build();
        influxDB.write(this.dbName, "autogen", point);
    }

    @Override
    public void close() {
        if (influxDB.isBatchEnabled()) {
            influxDB.disableBatch();
        }
        influxDB.close();
    }
}
