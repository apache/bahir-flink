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

package org.apache.flink.streaming.connectors.mqtt;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Collections.singletonList;
import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.flink.streaming.connectors.mqtt.MqttConfig.checkProperty;

/**
 * Source for reading messages from an Mqtt queue.
 *
 * @param <OUT> type of output messages
 */
public class MqttSource<OUT> extends RichSourceFunction<OUT>
    implements CheckpointedFunction, ResultTypeQueryable<OUT>, MqttCallbackExtended {
    private static final Logger LOG = LoggerFactory.getLogger(MqttSource.class);

    // Mqtt subcriber client id
    private static final String MQTT_CLIENT_ID = MqttClient.generateClientId();
    // Blocking queue max capacity
    private static final int MAX_QUEUE_CAPACITY = 10000;

    // Subscribe one or more topics
    private final String[] topics;
    // Convert bytes to output message
    private final DeserializationSchema<OUT> deserializationSchema;
    // User-supplied properties
    private final Properties properties;
    // Queue cache data form mqtt client
    private final LinkedBlockingQueue<MqttMessage> queue;

    private String serverUrl;
    private String username;
    private String password;
    private String clientId;
    private boolean cleanSession;
    private int qos;

    private transient ListState<String> mqttState;
    private transient MqttClient client;

    private volatile boolean isRunning;

    public MqttSource(String topic, DeserializationSchema<OUT> deserializationSchema, Properties properties) {
        this(Collections.singletonList(topic), deserializationSchema, properties, MAX_QUEUE_CAPACITY);
    }

    public MqttSource(List<String> topics, DeserializationSchema<OUT> deserializationSchema, Properties properties) {
        this(topics, deserializationSchema, properties, MAX_QUEUE_CAPACITY);
    }

    /**
     * Creates {@link MqttSource} for Streaming
     *
     * @param topics                Subscribe topic list
     * @param deserializationSchema Convert bytes to output message
     * @param properties            Mqtt properties
     * @param capacity              Blocking queue capacity
     */
    public MqttSource(List<String> topics,
                      DeserializationSchema<OUT> deserializationSchema,
                      Properties properties,
                      int capacity) {
        checkProperty(properties, MqttConfig.SERVER_URL);
        checkProperty(properties, MqttConfig.USERNAME);
        checkProperty(properties, MqttConfig.PASSWORD);

        this.topics = topics.toArray(new String[topics.size()]);
        this.deserializationSchema = deserializationSchema;
        this.properties = properties;
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.mqttState = context.getOperatorStateStore().getListState(
            new ListStateDescriptor<>("mqttClientId", String.class));
        this.clientId = context.isRestored() ? getOnlyElement(mqttState.get()) : MQTT_CLIENT_ID;
        LOG.info("Current mqtt subcriber clientId is: {}", clientId);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (connect()) {
            this.isRunning = true;
        }
    }

    private boolean connect() throws Exception {
        this.serverUrl = properties.getProperty(MqttConfig.SERVER_URL);
        this.username = properties.getProperty(MqttConfig.USERNAME);
        this.password = properties.getProperty(MqttConfig.PASSWORD);
        this.clientId = properties.getProperty(MqttConfig.CLIENT_ID, clientId);
        this.cleanSession = Boolean.parseBoolean(properties.getProperty(MqttConfig.CLEAN_SESSION, "true"));
        this.qos = Integer.parseInt(properties.getProperty(MqttConfig.QOS, "1"));

        int[] qoses = new int[topics.length];
        Arrays.fill(qoses, qos);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(username);
        options.setPassword(password.toCharArray());
        options.setCleanSession(cleanSession);
        options.setAutomaticReconnect(true);

        client = new MqttClient(serverUrl, clientId, new MemoryPersistence());
        client.setCallback(this);
        // Connect to mqtt broker
        client.connect(options);
        // Start subscribe topic
        client.subscribe(topics, qoses);

        LOG.info("Source subtask {} initialize by serverUrl:{}, start subscribing topics:{} with " +
                "clientId:{} and qoses:{}.", getRuntimeContext().getIndexOfThisSubtask(), serverUrl,
            Arrays.asList(topics), clientId, Arrays.asList(ArrayUtils.toObject(qoses)));

        return client.isConnected();
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        LOG.info("Consumer subtask {} start receiving data pushed by mqtt broker.",
            getRuntimeContext().getIndexOfThisSubtask());
        while (isRunning) {
            MqttMessage message = queue.take();
            try {
                OUT value = deserializationSchema.deserialize(message.getPayload());
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(value);
                }
            } catch (Exception e) {
                LOG.error("Failed to deserialize message, topic: {}, " + "message: {}", Arrays.asList(topics), message);
                throw new RuntimeException("Failed to deserialize message", e);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        mqttState.update(singletonList(MQTT_CLIENT_ID));
    }

    @Override
    public void cancel() {
        close();
    }

    @Override
    public void close() {
        try {
            queue.clear();
            client.disconnect();
            client.close();
        } catch (Exception e) {
            LOG.error("Close mqtt subscriber client failed", e);
        } finally {
            isRunning = false;
        }
    }

    @Override
    public void connectComplete(boolean reconnect, String serUrl) {
        LOG.info("Call connectCoplete method, reconnect:{}, serverUrl:{}.", reconnect, serUrl);
    }

    @Override
    public void connectionLost(Throwable cause) {
        LOG.error("Connection has losted betweenv mqtt subscriber client and broker.", cause);
        cause.printStackTrace();
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        LOG.debug("Message {} has arrived from topic {}.", mqttMessage.getPayload(), topic);
        queue.put(mqttMessage);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        throw new IllegalStateException("Method deliveryComplete is for publisher, so should not go to here.");
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
