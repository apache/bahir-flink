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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.ExponentialBackOff;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.influxdb.source.InfluxDBSource;
import org.apache.flink.streaming.connectors.influxdb.util.InfluxDBTestDeserializer;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Integration test for the InfluxDB source for Flink. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InfluxDBSourceIntegrationTestCase extends TestLogger {

    private static final String HTTP_ADDRESS = "http://localhost";
    private int port = 0;

    private static final HttpRequestFactory HTTP_REQUEST_FACTORY =
            new NetHttpTransport().createRequestFactory();
    private static final ExponentialBackOff HTTP_BACKOFF =
            new ExponentialBackOff.Builder()
                    .setInitialIntervalMillis(250)
                    .setMaxElapsedTimeMillis(10000)
                    .setMaxIntervalMillis(1000)
                    .setMultiplier(1.3)
                    .setRandomizationFactor(0.5)
                    .build();

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

    @BeforeEach
    void init() {
        CollectSink.VALUES.clear();
        try (final ServerSocket serverSocket = new ServerSocket(0)) {
            this.port = serverSocket.getLocalPort();
            this.log.info("Using port {} for the HTTP server", this.port);
        } catch (final IOException ioException) {
            this.log.error("Could not open open port {}", ioException.getMessage());
        }
    }

    /**
     * Test the following topology.
     *
     * <pre>
     *     test longValue=1i 1     +1            2
     *     test longValue=2i 1     +1            3
     *     (source) ------------> (map) -----> (sink)
     * </pre>
     */
    // Test is disabled since it is not passing the Travis pipeline.
    @Test
    @Disabled
    void testIncrementPipeline() throws Exception {
        final InfluxDBSource<Long> influxDBSource =
                InfluxDBSource.builder()
                        .setPort(this.port)
                        .setDeserializer(new InfluxDBTestDeserializer())
                        .build();

        this.env
                .fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        final JobClient jobClient = this.env.executeAsync();
        assertTrue(this.checkHealthCheckAvailable());

        final int writeResponseCode =
                this.writeToInfluxDB("test longValue=1i 1\ntest longValue=2i 2");

        assertEquals(writeResponseCode, HttpURLConnection.HTTP_NO_CONTENT);

        jobClient.cancel();

        final Collection<Long> results = new ArrayList<>();
        results.add(2L);
        results.add(3L);
        assertTrue(CollectSink.VALUES.containsAll(results));
    }

    @Test
    void testBadRequestException() throws Exception {
        final InfluxDBSource<Long> influxDBSource =
                InfluxDBSource.builder()
                        .setPort(this.port)
                        .setDeserializer(new InfluxDBTestDeserializer())
                        .build();

        this.env
                .fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        final JobClient jobClient = this.env.executeAsync();
        assertTrue(this.checkHealthCheckAvailable());
        final HttpResponseException thrown =
                Assertions.assertThrows(
                        HttpResponseException.class,
                        () -> this.writeToInfluxDB("malformedLineProtocol_test"));
        assertTrue(thrown.getMessage().contains("Unable to parse line."));
        jobClient.cancel();
    }

    @Test
    void testRequestTooLargeException() throws Exception {
        final InfluxDBSource<Long> influxDBSource =
                InfluxDBSource.builder()
                        .setPort(this.port)
                        .setDeserializer(new InfluxDBTestDeserializer())
                        .setMaximumLinesPerRequest(2)
                        .build();
        this.env
                .fromSource(influxDBSource, WatermarkStrategy.noWatermarks(), "InfluxDBSource")
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        final JobClient jobClient = this.env.executeAsync();
        assertTrue(this.checkHealthCheckAvailable());

        final String lines = "test longValue=1i 1\ntest longValue=1i 1\ntest longValue=1i 1";
        final HttpResponseException thrown =
                Assertions.assertThrows(
                        HttpResponseException.class, () -> this.writeToInfluxDB(lines));
        assertTrue(
                thrown.getMessage()
                        .contains("Payload too large. Maximum number of lines per request is 2."));
        jobClient.cancel();
    }

    private int writeToInfluxDB(final String line) throws IOException {
        final HttpContent content = ByteArrayContent.fromString("text/plain; charset=utf-8", line);
        final HttpRequest request =
                HTTP_REQUEST_FACTORY.buildPostRequest(
                        new GenericUrl(
                                String.format("%s:%s/api/v2/write", HTTP_ADDRESS, this.port)),
                        content);
        return request.execute().getStatusCode();
    }

    private boolean checkHealthCheckAvailable() throws IOException {
        final HttpRequest request =
                HTTP_REQUEST_FACTORY.buildGetRequest(
                        new GenericUrl(String.format("%s:%s/health", HTTP_ADDRESS, this.port)));

        request.setUnsuccessfulResponseHandler(
                new HttpBackOffUnsuccessfulResponseHandler(HTTP_BACKOFF));
        request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(HTTP_BACKOFF));

        final int statusCode = request.execute().getStatusCode();
        return statusCode == HttpURLConnection.HTTP_OK;
    }

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
