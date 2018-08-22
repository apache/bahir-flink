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
package org.apache.flink.streaming.connectors.flume;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class FlumeSink<IN> extends RichSinkFunction<IN> {

    private transient FlumeRpcClient client;

    private String host;
    private int port;
    private SerializationSchema<IN> schema;

    public FlumeSink(String host, int port, SerializationSchema<IN> schema) {
        this.host = host;
        this.port = port;
        this.schema = schema;
    }

    /**
     * Receives tuples from the Apache Flink {@link DataStream} and forwards
     * them to Apache Flume.
     *
     * @param value
     *            The tuple arriving from the datastream
     */
    @Override
    public void invoke(IN value, Context context) throws Exception {
        byte[] data = schema.serialize(value);
        client.sendData(data);
    }

    @Override
    public void open(Configuration config) {
        client = new FlumeRpcClient(host, port);
        client.init();
    }

    @Override
    public void close() {
        if (client == null) return;
        client.close();
    }


}
