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
package org.apache.flink.streaming.connectors.influxdb.source;

import java.util.Properties;
import java.util.function.Function;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/* Configurations for a InfluxDBSource. */
public class InfluxDBSourceOptions {

    InfluxDBSourceOptions() {}

    public static final ConfigOption<Long> ENQUEUE_WAIT_TIME =
            ConfigOptions.key("timeout.enqueue")
                    .longType()
                    .defaultValue(5L)
                    .withDescription("The time out for enqueuing an HTTP request to the queue.");

    public static final ConfigOption<Integer> INGEST_QUEUE_CAPACITY =
            ConfigOptions.key("queue_capacity.ingest")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Size of queue that buffers HTTP requests data points before fetching.");

    public static final ConfigOption<Integer> MAXIMUM_LINES_PER_REQUEST =
            ConfigOptions.key("limit.lines_per_request")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The maximum number of lines that should be parsed per HTTP request.");
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(8000)
                    .withDescription(
                            "TCP port on which the split reader's HTTP server is running on.");

    public static long getEnqueueWaitTime(final Properties properties) {
        return getOption(properties, InfluxDBSourceOptions.ENQUEUE_WAIT_TIME, Long::parseLong);
    }

    public static int getIngestQueueCapacity(final Properties properties) {
        return getOption(
                properties, InfluxDBSourceOptions.INGEST_QUEUE_CAPACITY, Integer::parseInt);
    }

    public static int getMaximumLinesPerRequest(final Properties properties) {
        return getOption(
                properties, InfluxDBSourceOptions.MAXIMUM_LINES_PER_REQUEST, Integer::parseInt);
    }

    public static int getPort(final Properties properties) {
        return getOption(properties, InfluxDBSourceOptions.PORT, Integer::parseInt);
    }

    @SuppressWarnings("unchecked")
    public static <T> T getOption(
            final Properties props,
            final ConfigOption configOption,
            final Function<String, T> parser) {
        final String value = props.getProperty(configOption.key());
        return (T) (value == null ? configOption.defaultValue() : parser.apply(value));
    }
}
