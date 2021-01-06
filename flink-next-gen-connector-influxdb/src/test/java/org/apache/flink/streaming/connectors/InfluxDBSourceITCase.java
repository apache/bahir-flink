/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;
import org.influxdb.InfluxDB;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.rules.Timeout;
import util.InfluxDBContainer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Integration test for the InfluxDB source for Flink.
 */
public class InfluxDBSourceITCase extends TestLogger {
    private static InfluxDB influxDB;

    @RegisterExtension
    public static InfluxDBContainer influxDbContainer = new InfluxDBContainer();

    @RegisterExtension
    public static final MiniClusterWithClientResource CLUSTER = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @RegisterExtension
    public final Timeout timeout = Timeout.millis(300000L);

    @BeforeAll
    static void setUp() {
        influxDbContainer.startPreIngestedInfluxDB();
    }

    @AfterAll
    static void tearDown() {
        influxDbContainer.stop();
    }

    /**
     * Test the following topology.
     * <pre>
     *     (source1/1) -----> (map1/1) -----> (sink1/1)
     * </pre>
     */
    @Test
    void testIncrementPipeline() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        CollectSink.VALUES.clear();

        env.fromElements(1L, 21L, 22L)
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        env.execute();

        final Collection<Long> results = new ArrayList<>();
        results.add(2L);
        results.add(22L);
        results.add(23L);
        assertTrue(CollectSink.VALUES.containsAll(results));
    }

    /**
     * Simple incrementation with map.
     */
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
}
