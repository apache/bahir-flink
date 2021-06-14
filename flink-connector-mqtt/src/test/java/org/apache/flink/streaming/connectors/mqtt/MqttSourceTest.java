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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.util.Properties;

public class MqttSourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Properties properties = new Properties();
        properties.load(new FileInputStream(MqttSourceTest.class.getClassLoader().getResource(
            "application.properties").getFile()));

        Properties mqttProperties = new Properties();

        // mqtt server url = tcp://<Org_ID>.messaging.internetofthings.ibmcloud.com:1883
        mqttProperties.setProperty(MqttConfig.SERVER_URL,
            String.format("tcp://%s.messaging.internetofthings.ibmcloud.com:1883", properties.getProperty("Org_ID")));

        // client id = a:<Org_ID>:<App_Id>
        mqttProperties.setProperty(MqttConfig.CLIENT_ID,
            String.format("a:%s:%s", properties.getProperty("Org_ID"), properties.getProperty("App_Id")));

        mqttProperties.setProperty(MqttConfig.USERNAME, properties.getProperty("API_Key"));
        mqttProperties.setProperty(MqttConfig.PASSWORD, properties.getProperty("APP_Authentication_Token"));

        String topic = String.format("iot/type/%s/id/%s/evt/%s/fmt/json",
            properties.getProperty("Device_Type"),
            properties.getProperty("Device_ID"),
            properties.getProperty("EVENT_ID"));

        // Create mqtt source subscribe topic
        MqttSource<String> mqttSource = new MqttSource(topic, new SimpleStringSchema(), mqttProperties);
        DataStreamSource<String> tempratureDataSource = env.addSource(mqttSource);
        DataStream<String> stream = tempratureDataSource.map((MapFunction<String, String>) s ->
            System.currentTimeMillis() + s);
        stream.print();

        env.execute("MqttSourceToPrintSink");
    }

}
