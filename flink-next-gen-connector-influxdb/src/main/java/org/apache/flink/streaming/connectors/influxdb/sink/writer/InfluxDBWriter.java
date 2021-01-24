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
package org.apache.flink.streaming.connectors.influxdb.sink.writer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.connector.sink.Sink.ProcessingTimeService;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple3;

public class InfluxDBWriter<IN> implements SinkWriter<IN, String, String>, Serializable {

    private List<String> elements;

    private ProcessingTimeService processingTimerService;

    public InfluxDBWriter() {
        this.elements = new ArrayList<>();
    }

    @Override
    public void write(final IN in, final Context context) throws IOException {
        // Here we should convert the incoming data to datapoint
        this.elements.add(
                Tuple3.of(in, context.timestamp(), context.currentWatermark()).toString());
    }

    @Override
    public List<String> prepareCommit(final boolean flush) throws IOException {
        List<String> result = elements;
        elements = new ArrayList<>();
        return result;
    }

    @Override
    public List<String> snapshotState() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {}

    public void setProcessingTimerService(final ProcessingTimeService processingTimerService) {
        this.processingTimerService = processingTimerService;
    }
}
