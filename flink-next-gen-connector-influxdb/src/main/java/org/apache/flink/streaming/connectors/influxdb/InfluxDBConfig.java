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
package org.apache.flink.streaming.connectors.influxdb;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.apache.flink.util.Preconditions;
import org.influxdb.InfluxDB.LogLevel;

/** Configuration for InfluxDB. */
public class InfluxDBConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_BATCH_ACTIONS = 2000;
    private static final int DEFAULT_FLUSH_DURATION = 100;
    private static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.NONE;

    private final String url;
    private final String username;
    private final String password;
    private final String database;
    private final int batchActions;
    private final int flushDuration;
    private final TimeUnit flushDurationTimeUnit;
    private final LogLevel logLevel;
    private final boolean enableGzip;
    private final boolean createDatabase;

    public InfluxDBConfig(InfluxDBConfig.Builder builder) {
        Preconditions.checkArgument(builder != null, "InfluxDBConfig builder can not be null");

        this.url = Preconditions.checkNotNull(builder.getUrl(), "host can not be null");
        this.username =
                Preconditions.checkNotNull(builder.getUsername(), "username can not be null");
        this.password =
                Preconditions.checkNotNull(builder.getPassword(), "password can not be null");
        this.database =
                Preconditions.checkNotNull(builder.getDatabase(), "database name can not be null");

        this.batchActions = builder.getBatchActions();
        this.flushDuration = builder.getFlushDuration();
        this.flushDurationTimeUnit = builder.getFlushDurationTimeUnit();
        this.logLevel = builder.getLogLevel();
        this.enableGzip = builder.isEnableGzip();
        this.createDatabase = builder.isCreateDatabase();
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public int getBatchActions() {
        return batchActions;
    }

    public int getFlushDuration() {
        return flushDuration;
    }

    public TimeUnit getFlushDurationTimeUnit() {
        return flushDurationTimeUnit;
    }

    public LogLevel getLogLevel() {
        return logLevel;
    }

    public boolean isEnableGzip() {
        return enableGzip;
    }

    public boolean isCreateDatabase() {
        return createDatabase;
    }

    /**
     * Creates a new {@link InfluxDBConfig.Builder} instance.
     *
     * <p>This is a convenience method for {@code new InfluxDBConfig.Builder()}.
     *
     * @param url the url to connect to
     * @param username the username which is used to authorize against the influxDB instance
     * @param password the password for the username which is used to authorize against the influxDB
     *     instance
     * @param database the name of the database to write
     * @return the new InfluxDBConfig builder.
     */
    public static Builder builder(String url, String username, String password, String database) {
        return new Builder(url, username, password, database);
    }

    /** A builder used to create a build an instance of a InfluxDBConfig. */
    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String database;
        private int batchActions = DEFAULT_BATCH_ACTIONS;
        private int flushDuration = DEFAULT_FLUSH_DURATION;
        private TimeUnit flushDurationTimeUnit = TimeUnit.MILLISECONDS;
        private LogLevel logLevel = DEFAULT_LOG_LEVEL;
        private boolean enableGzip = false;
        private boolean createDatabase = false;

        /**
         * Creates a builder.
         *
         * @param url the url to connect to
         * @param username the username which is used to authorize against the influxDB instance
         * @param password the password for the username which is used to authorize against the
         *     influxDB instance
         * @param database the name of the database to write
         */
        public Builder(String url, String username, String password, String database) {
            this.url = url;
            this.username = username;
            this.password = password;
            this.database = database;
        }

        /**
         * Sets url.
         *
         * @param url the url to connect to
         * @return this Builder to use it fluent
         */
        public InfluxDBConfig.Builder url(String url) {
            this.url = url;
            return this;
        }

        /**
         * Sets username.
         *
         * @param username the username which is used to authorize against the influxDB instance
         * @return this Builder to use it fluent
         */
        public InfluxDBConfig.Builder username(String username) {
            this.username = username;
            return this;
        }

        /**
         * Sets password.
         *
         * @param password the password for the username which is used to authorize against the
         *     influxDB instance
         * @return this Builder to use it fluent
         */
        public InfluxDBConfig.Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets database name.
         *
         * @param database the name of the database to write
         * @return this Builder to use it fluent
         */
        public InfluxDBConfig.Builder database(String database) {
            this.database = database;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the number of batch actions currently
         * added. Defaults to <tt>DEFAULT_BATCH_ACTIONS</tt>. Can be set to <tt>-1</tt> to disable
         * it.
         *
         * @param batchActions number of Points written after which a write must happen.
         * @return this Builder to use it fluent
         */
        public InfluxDBConfig.Builder batchActions(int batchActions) {
            this.batchActions = batchActions;
            return this;
        }

        /**
         * Sets a flush interval flushing *any* bulk actions pending if the interval passes.
         *
         * @param flushDuration the flush duration
         * @param flushDurationTimeUnit the TimeUnit of the flush duration
         * @return this Builder to use it fluent
         */
        public Builder flushDuration(int flushDuration, TimeUnit flushDurationTimeUnit) {
            this.flushDuration = flushDuration;
            this.flushDurationTimeUnit = flushDurationTimeUnit;
            return this;
        }

        /**
         * Sets the log level to control level of logging of the Rest layer.
         *
         * @param value log level input as string (none, basic, headers, full)
         * @return this Builder to use it fluent
         */
        public Builder logLevel(final String value) {
            if (value != null) {
                try {
                    logLevel = LogLevel.valueOf(value.toUpperCase());
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                            String.format("Could not set %s as log level!", value.toUpperCase()));
                }
            }
            return this;
        }

        /**
         * Enable Gzip compress for http request body.
         *
         * @param enableGzip the enableGzip value
         * @return this Builder to use it fluent
         */
        public InfluxDBConfig.Builder enableGzip(boolean enableGzip) {
            this.enableGzip = enableGzip;
            return this;
        }

        /**
         * Make InfluxDb sink create new database.
         *
         * @param createDatabase createDatabase switch value
         * @return this Builder to use it fluent
         */
        public InfluxDBConfig.Builder createDatabase(boolean createDatabase) {
            this.createDatabase = createDatabase;
            return this;
        }

        /**
         * Builds InfluxDBConfig.
         *
         * @return the InfluxDBConfig instance.
         */
        public InfluxDBConfig build() {
            return new InfluxDBConfig(this);
        }

        public String getUrl() {
            return url;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getDatabase() {
            return database;
        }

        public int getBatchActions() {
            return batchActions;
        }

        public int getFlushDuration() {
            return flushDuration;
        }

        public TimeUnit getFlushDurationTimeUnit() {
            return flushDurationTimeUnit;
        }

        public LogLevel getLogLevel() {
            return logLevel;
        }

        public boolean isEnableGzip() {
            return enableGzip;
        }

        public boolean isCreateDatabase() {
            return createDatabase;
        }
    }
}
