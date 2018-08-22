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

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

class FlumeRpcClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FlumeRpcClient.class);

    protected RpcClient client;
    private String hostname;
    private int port;


    FlumeRpcClient(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    /**
     * Initializes the connection to Apache Flume.
     */
    public boolean init() {
        // Setup the RPC connection
        int initCounter = 0;
        while (true) {
            verifyCounter(initCounter, "Cannot establish connection");

            try {
                this.client = RpcClientFactory.getDefaultInstance(hostname, port);
            } catch (FlumeException e) {
                // Wait one second if the connection failed before the next
                // try
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Interrupted while trying to connect {} on {}", hostname, port);
                    }
                }
            }
            if (client != null) {
                break;
            }
            initCounter++;
        }
        return client.isActive();
    }


    public boolean sendData(String data) {
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
        return sendData(event);
    }
    public boolean sendData(byte[] data) {
        Event event = EventBuilder.withBody(data);
        return sendData(event);
    }

    private boolean sendData(Event event) {
        return sendData(event, 0);
    }
    private boolean sendData(Event event, int retryCount) {
        verifyCounter(retryCount, "Cannot send message");
        try {
            client.append(event);
            return true;
        } catch (EventDeliveryException e) {
            // clean up and recreate the client
            reconnect();
            return sendData(event, ++retryCount);
        }
    }


    private void verifyCounter(int counter, String messaje) {
        if (counter >= 10) {
            throw new RuntimeException(messaje + " on " + hostname + " on " + port);
        }
    }

    private void reconnect() {
        close();
        client = null;
        init();
    }

    @Override
    public void close() {
        if (this.client == null) return;

        this.client.close();
    }
}
