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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSource;
import org.apache.flink.streaming.connectors.util.InfluxDBTestDeserializer;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Integration test for the InfluxDB source for Flink. */
public class InfluxDBSourceIntegrationTestCase extends TestLogger {
    @RegisterExtension
    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    /**
     * Test the following topology.
     *
     * <pre>
     *     1,2,3                +1              2,3,4
     *     (source1/1) -----> (map1/1) -----> (sink1/1)
     * </pre>
     */
    @Test
    void testIncrementPipeline() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        CollectSink.VALUES.clear();

        final Source influxDBSource =
                new InfluxDBSource<Long>(
                        Boundedness.CONTINUOUS_UNBOUNDED, new InfluxDBTestDeserializer());

        env.fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());
        final Thread runner = new Thread(new ExecutionRunner(env));
        runner.start();
        Thread.sleep(5000);
        final URL u = new URL("http://localhost:8000/api/v2/write");
        final HttpURLConnection conn = (HttpURLConnection) u.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        // TODO set Content-Type (look at Influx API docs)
        final OutputStream os = conn.getOutputStream();
        final String line = "LINE_PROTOCOL_HERE"; // TODO replace
        os.write(line.getBytes("utf-8"));

        final int code = conn.getResponseCode();
        assertTrue(code == 204);

        runner.join();

        final Collection<Long> results = new ArrayList<>();
        results.add(2L);
        // results.add(3L);
        // results.add(4L);
        assertTrue(CollectSink.VALUES.containsAll(results));
    }

    /** Simple incrementation with map. */
    public static class IncrementMapFunction implements MapFunction<Long, Long> {

        @Override
        public Long map(final Long record) throws Exception {
            return record + 1;
        }
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Long> {

        // must be static
        public static final List<Long> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(final Long value) throws Exception {
            VALUES.add(value);
        }
    }

    private static class ExecutionRunner implements Runnable {
        private final StreamExecutionEnvironment streamExecutionEnvironment;

        ExecutionRunner(final StreamExecutionEnvironment streamExecutionEnvironment) {
            this.streamExecutionEnvironment = streamExecutionEnvironment;
        }

        @SneakyThrows
        @Override
        public void run() {
            this.streamExecutionEnvironment.execute();
        }
    }
}
