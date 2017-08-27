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

package org.apache.flink.streaming.examples.influxdb;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * This is an example showing the to use the InfluxDB Sink in the Streaming API.
 */
public class InfluxDBSinkExample {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBSinkExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.fromElements("cpu#server01", "mem#server02", "disk#server03");

        DataStream<InfluxDBPoint> dataStream = source.map(
                new RichMapFunction<String, InfluxDBPoint>() {
                    @Override
                    public InfluxDBPoint map(String s) throws Exception {
                        String[] input = s.split("#");

                        String measurement = input[0];
                        long timestamp = System.currentTimeMillis();

                        HashMap<String, String> tags = new HashMap<>();
                        tags.put("host", input[1]);
                        tags.put("region", "region#" + String.valueOf(s.hashCode() % 20));

                        HashMap<String, Object> fields = new HashMap<>();
                        fields.put("value1", s.hashCode() % 100);
                        fields.put("value2", s.hashCode() % 50);

                        return new InfluxDBPoint(measurement, timestamp, tags, fields);
                    }
                }
        );

        //dataStream.print();

        dataStream.addSink(new InfluxDBSink("http://localhost:8086", "root", "root", "db_flink_test"));

        env.execute("InfluxDB Sink Example");
    }

}
