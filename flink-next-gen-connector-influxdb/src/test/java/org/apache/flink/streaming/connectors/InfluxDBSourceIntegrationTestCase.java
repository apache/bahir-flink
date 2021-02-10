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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSource;
import org.apache.flink.streaming.connectors.util.InfluxDBTestDeserializer;
import org.apache.flink.util.TestLogger;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Integration test for the InfluxDB source for Flink. */
public class InfluxDBSourceIntegrationTestCase extends TestLogger {
    private static final int WAIT_MILLIS = 5000;

    private StreamExecutionEnvironment env = null;
    private InfluxDBSource<Long> influxDBSource = null;
    private CloseableHttpClient httpClient = null;
    private HttpPost httpPost = null;

    @Before
    public void setUp() {
        this.influxDBSource =
                InfluxDBSource.<Long>builder()
                        .setDeserializer(new InfluxDBTestDeserializer())
                        .build();
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setParallelism(1);
        this.httpClient = HttpClients.createDefault();
        this.httpPost = new HttpPost("http://localhost:8000/api/v2/write");
    }

    @SneakyThrows
    @After
    public void tearDown() {
        this.httpClient.close();
    }

    /**
     * Test the following topology.
     *
     * <pre>
     *     1,2,3                +1              2,3,4
     *     (source1/1) -----> (map1/1) -----> (sink1/1)
     * </pre>
     */
    @Test
    public void testIncrementPipeline() throws Exception {
        CollectSink.VALUES.clear();
        this.httpPost.setEntity(new StringEntity("test longValue=1i 1"));

        this.env
                .fromSource(this.influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        final JobClient jobClient = this.env.executeAsync();
        Thread.sleep(WAIT_MILLIS);

        final CloseableHttpResponse response = this.httpClient.execute(this.httpPost);
        assertThat(
                response.getStatusLine().getStatusCode(),
                equalTo(HttpURLConnection.HTTP_NO_CONTENT));

        jobClient.cancel();

        final Collection<Long> results = new ArrayList<>();
        results.add(2L);
        assertTrue(CollectSink.VALUES.containsAll(results));
    }

    @Test
    public void testBadRequestException() throws Exception {
        this.httpPost.setEntity(new StringEntity("malformedLineProtocol_test"));
        this.env
                .fromSource(this.influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        final JobClient jobClient = this.env.executeAsync();
        Thread.sleep(WAIT_MILLIS);

        final CloseableHttpResponse response = this.httpClient.execute(this.httpPost);

        assertThat(
                response.getStatusLine().getStatusCode(),
                equalTo(HttpURLConnection.HTTP_BAD_REQUEST));

        jobClient.cancel();
    }

    @Test
    public void testRequestTooLargeException() throws Exception {}

    // ---------------- private helper class --------------------

    /** Simple incrementation with map. */
    private static class IncrementMapFunction implements MapFunction<Long, Long> {

        @Override
        public Long map(final Long record) {
            return record + 1;
        }
    }

    /** create a simple testing sink */
    private static class CollectSink implements SinkFunction<Long> {

        // must be static
        public static final List<Long> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(final Long value) {
            VALUES.add(value);
        }
    }
}
