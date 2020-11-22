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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.flink.streaming.connectors.mqtt.MqttConfig.checkProperty;

/**
 * Sink for writing messages to an Mqtt queue.
 *
 * @param <IN> type of input messages
 */
public class MqttSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction, MqttCallbackExtended {
    private static final Logger LOG = LoggerFactory.getLogger(MqttSink.class);

    // Mqtt publisher client id
    private static final String MQTT_CLIENT_ID = MqttClient.generateClientId();

    // Publish topic name
    private final String topic;
    // Convert input message to bytes
    private final SerializationSchema<IN> serializationSchema;
    // User-supplied mqtt properties
    private final Properties properties;

    private String serverUrl;
    private String username;
    private String password;
    private String clientId;
    private boolean cleanSession;
    private boolean retained;
    private int qos;

    private transient ListState<String> mqttState;
    private transient MqttClient client;

    /**
     * Creates {@link MqttSink} for Streaming
     *
     * @param topic               Publish topic name
     * @param serializationSchema Convert input message to bytes
     * @param properties          Mqtt properties
     */
    public MqttSink(String topic,
                    SerializationSchema<IN> serializationSchema,
                    Properties properties) {
        checkProperty(properties, MqttConfig.SERVER_URL);
        checkProperty(properties, MqttConfig.USERNAME);
        checkProperty(properties, MqttConfig.PASSWORD);

        this.topic = topic;
        this.serializationSchema = serializationSchema;
        this.properties = properties;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.mqttState = context.getOperatorStateStore().getListState(
            new ListStateDescriptor<>("mqttClientId", String.class));
        this.clientId = context.isRestored() ? getOnlyElement(mqttState.get()) : MQTT_CLIENT_ID;
        LOG.info("Current mqtt publisher clientId is : {}.", clientId);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        mqttState.update(singletonList(MQTT_CLIENT_ID));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.serverUrl = properties.getProperty(MqttConfig.SERVER_URL);
        this.username = properties.getProperty(MqttConfig.USERNAME);
        this.password = properties.getProperty(MqttConfig.PASSWORD);
        this.clientId = properties.getProperty(MqttConfig.CLIENT_ID, clientId);
        this.cleanSession = Boolean.parseBoolean(properties.getProperty(
            MqttConfig.CLEAN_SESSION, "true"));
        this.retained = Boolean.parseBoolean(properties.getProperty(
            MqttConfig.RETAINED, "false"));
        this.qos = Integer.parseInt(properties.getProperty(
            MqttConfig.QOS, "1"));

        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(username);
        options.setPassword(password.toCharArray());
        options.setCleanSession(cleanSession);
        options.setAutomaticReconnect(true);

        client = new MqttClient(serverUrl, clientId, new MemoryPersistence());
        client.setCallback(this);
        // Connect to mqtt broker
        client.connect(options);

        LOG.info("Sink subtask {} initialize by serverUrl:{}, start publishing topic:{} with " +
                "clientId:{} and qos:{}.", getRuntimeContext().getIndexOfThisSubtask(),
            serverUrl, topic, clientId, qos);
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        try {
            byte[] message = serializationSchema.serialize(value);
            client.publish(topic, message, qos, retained);
        } catch (Exception e) {
            LOG.error("Failed to publish message to mqtt broker, topic: {}, message: {}", topic, value);
            throw new RuntimeException("Failed to publish message to mqtt broker", e);
        }
    }

    @Override
    public void close() throws Exception {
        client.disconnect();
        client.close();
    }

    @Override
    public void connectComplete(boolean reconnect, String serUrl) {
        LOG.info("Call connectComplete method, reconnect:{}, serUrl:{}.", reconnect, serUrl);
    }

    @Override
    public void connectionLost(Throwable cause) {
        LOG.error("Connection has losted between publisher org.apache.flink.streaming.connectors.mqtt client and broker.", cause);
        cause.printStackTrace();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        throw new IllegalStateException("Method messageArrived is for subscriber, so should not go to here.");
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

}
