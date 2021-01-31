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
package org.apache.flink.streaming.connectors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSink;
import org.apache.flink.streaming.connectors.util.InfluxDBContainer;
import org.apache.flink.streaming.connectors.util.InfluxDBTestSerializer;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.util.TestLogger;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

public class InfluxDBSinkITCase extends TestLogger {

    @ClassRule
    public static final InfluxDBContainer<?> influxDBContainer =
            InfluxDBContainer.createWithDefaultTag();

    private static final List<Long> SOURCE_DATA = Arrays.asList(1L, 2L, 3L);

    private static final List<String> EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE =
            SOURCE_DATA.stream()
                    .flatMap(
                            x ->
                                    Collections.nCopies(
                                            2,
                                            new InfluxDBTestSerializer()
                                                    .serialize(x)
                                                    .toLineProtocol())
                                            .stream())
                    .collect(Collectors.toList());

    /**
     * Test the following topology.
     *
     * <pre>
     *     1L,2L,3L           "Test,LongValue=1 fieldKey="fieldValue"",
     *                        "Test,LongValue=2 fieldKey="fieldValue"",
     *                        "Test,LongValue=3 fieldKey="fieldValue"",
     *                        "Test,LongValue=1 fieldKey="fieldValue"",
     *                        "Test,LongValue=2 fieldKey="fieldValue"",
     *                        "Test,LongValue=3 fieldKey="fieldValue"",
     *     (source2/2) -----> (sink1/1)
     * </pre>
     *
     * Source collects twice and calls each time 2 checkpoint. In total 4 checkpoints are set. At
     * the end of the source another checkpoint is called. This makes it a total of 5 checkpoints.
     */
    @Test
    void shouldWriteDataToInfluxDB() throws Exception {
        final StreamExecutionEnvironment env = buildStreamEnv();
        final InfluxDBConfig influxDBConfig =
                InfluxDBConfig.builder()
                        .url(influxDBContainer.getUrl())
                        .username(InfluxDBContainer.getUsername())
                        .password(InfluxDBContainer.getPassword())
                        .bucket(InfluxDBContainer.getBucket())
                        .organization(InfluxDBContainer.getOrganization())
                        .build();

        final InfluxDBSink<Long> influxDBSink =
                InfluxDBSink.<Long>builder()
                        .influxDBSchemaSerializer(new InfluxDBTestSerializer())
                        .influxDBConfig(influxDBConfig)
                        .build();

        env.addSource(new FiniteTestSource(SOURCE_DATA), BasicTypeInfo.LONG_TYPE_INFO)
                .sinkTo(influxDBSink);

        env.execute();

        final List<String> actualWrittenPoints = queryWrittenData(influxDBConfig);

        assertThat(
                actualWrittenPoints.size(), is(EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE.size()));

        final List<String> actualCheckpoints = queryCheckpoints(influxDBConfig);

        assertThat(actualCheckpoints.size(), is(5));
    }

    private StreamExecutionEnvironment buildStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(100);
        return env;
    }

    // TODO: Find a clean way to query and test expected data points

    private static List<String> queryWrittenData(final InfluxDBConfig influxDBConfig) {
        final List<String> dataPoints = new ArrayList<>();

        final String query =
                String.format(
                        "from(bucket: \"%s\") |> "
                                + "range(start: -1h) |> "
                                + "filter(fn:(r) => r._measurement == \"test\")",
                        InfluxDBContainer.getBucket());
        final InfluxDBClient influxDBClient = influxDBConfig.getClient();
        final List<FluxTable> tables = influxDBClient.getQueryApi().query(query);
        for (final FluxTable table : tables) {
            for (final FluxRecord record : table.getRecords()) {
                dataPoints.add(recordToDataPoint(record).toLineProtocol());
            }
        }
        return dataPoints;
    }

    private static List<String> queryCheckpoints(final InfluxDBConfig influxDBConfig) {
        final List<String> commitDataPoints = new ArrayList<>();

        final String query =
                String.format(
                        "from(bucket: \"%s\") |> "
                                + "range(start: -1h) |> "
                                + "filter(fn:(r) => r._measurement == \"checkpoint\")",
                        InfluxDBContainer.getBucket());

        final InfluxDBClient influxDBClient = influxDBConfig.getClient();
        final List<FluxTable> tables = influxDBClient.getQueryApi().query(query);
        for (final FluxTable table : tables) {
            for (final FluxRecord record : table.getRecords()) {
                commitDataPoints.add(recordToDataPoint2(record).toLineProtocol());
            }
        }
        return commitDataPoints;
    }

    private static Point recordToDataPoint(final FluxRecord record) {
        final String tagKey = "longValue";
        final Point point = new Point(record.getMeasurement());
        point.addTag(tagKey, String.valueOf(record.getValueByKey(tagKey)));
        point.addField(
                Objects.requireNonNull(record.getField()), String.valueOf(record.getValue()));
        point.time(record.getTime(), WritePrecision.NS);
        return point;
    }

    private static Point recordToDataPoint2(final FluxRecord record) {
        final Point point = new Point(record.getMeasurement());
        point.addField(
                Objects.requireNonNull(record.getField()), String.valueOf(record.getValue()));
        point.time(record.getTime(), WritePrecision.NS);
        return point;
    }
}
