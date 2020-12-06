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

import static org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSinkOptions.getInfluxDBClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.sink.InfluxDBSink;
import org.apache.flink.streaming.connectors.influxdb.util.InfluxDBContainer;
import org.apache.flink.streaming.connectors.influxdb.util.InfluxDBTestSerializer;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class InfluxDBSinkIntegrationTestCase extends TestLogger {

    @Container
    public static final InfluxDBContainer<?> influxDBContainer =
            InfluxDBContainer.createWithDefaultTag();

    private static final List<Long> SOURCE_DATA = Arrays.asList(1L, 2L, 3L);

    private static final List<String> EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE =
            SOURCE_DATA.stream()
                    .map(x -> new InfluxDBTestSerializer().serialize(x, null).toLineProtocol())
                    .collect(Collectors.toList());

    /**
     * Test the following topology.
     *
     * <pre>
     *     1L,2L,3L           "test,longValue=1 fieldKey="fieldValue"",
     *                        "test,longValue=2 fieldKey="fieldValue"",
     *                        "test,longValue=3 fieldKey="fieldValue""
     *     (source2/2) -----> (sink1/1)
     * </pre>
     *
     * Source collects twice and calls each time 2 checkpoint. In total 4 checkpoints are set. In
     * some cases there are more than 4 checkpoints set.
     */
    @Test
    void testSinkDataToInfluxDB() throws Exception {
        final StreamExecutionEnvironment env = buildStreamEnv();

        final InfluxDBSink<Long> influxDBSink =
                InfluxDBSink.builder()
                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                        .setInfluxDBUrl(influxDBContainer.getUrl())
                        .setInfluxDBUsername(InfluxDBContainer.username)
                        .setInfluxDBPassword(InfluxDBContainer.password)
                        .setInfluxDBBucket(InfluxDBContainer.bucket)
                        .setInfluxDBOrganization(InfluxDBContainer.organization)
                        .addCheckpointDataPoint(true)
                        .build();

        env.addSource(new FiniteTestSource(SOURCE_DATA), BasicTypeInfo.LONG_TYPE_INFO)
                .sinkTo(influxDBSink);

        env.execute();

        final InfluxDBClient client = getInfluxDBClient(influxDBSink.getConfiguration());
        final List<String> actualWrittenPoints = queryWrittenData(client);

        assertEquals(actualWrittenPoints.size(), EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE.size());

        final List<String> actualCheckpoints = queryCheckpoints(client);
        assertTrue(actualCheckpoints.size() >= 4);
    }

    // ---------------- private helper methods --------------------

    private static StreamExecutionEnvironment buildStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(100);
        return env;
    }

    private static List<String> queryWrittenData(final InfluxDBClient influxDBClient) {
        final List<String> dataPoints = new ArrayList<>();

        final String query =
                String.format(
                        "from(bucket: \"%s\") |> "
                                + "range(start: -1h) |> "
                                + "filter(fn:(r) => r._measurement == \"test\")",
                        InfluxDBContainer.bucket);
        final List<FluxTable> tables = influxDBClient.getQueryApi().query(query);
        for (final FluxTable table : tables) {
            for (final FluxRecord record : table.getRecords()) {
                dataPoints.add(recordToDataPoint(record).toLineProtocol());
            }
        }
        return dataPoints;
    }

    private static List<String> queryCheckpoints(final InfluxDBClient influxDBClient) {
        final List<String> commitDataPoints = new ArrayList<>();

        final String query =
                String.format(
                        "from(bucket: \"%s\") |> "
                                + "range(start: -1h) |> "
                                + "filter(fn:(r) => r._measurement == \"checkpoint\")",
                        InfluxDBContainer.bucket);

        final List<FluxTable> tables = influxDBClient.getQueryApi().query(query);
        for (final FluxTable table : tables) {
            for (final FluxRecord record : table.getRecords()) {
                commitDataPoints.add(recordToCheckpointDataPoint(record).toLineProtocol());
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

    private static Point recordToCheckpointDataPoint(final FluxRecord record) {
        final Point point = new Point(record.getMeasurement());
        point.addField(
                Objects.requireNonNull(record.getField()), String.valueOf(record.getValue()));
        point.time(record.getTime(), WritePrecision.NS);
        return point;
    }
}
