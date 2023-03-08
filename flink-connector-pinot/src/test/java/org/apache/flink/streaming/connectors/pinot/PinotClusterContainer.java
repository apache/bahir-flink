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

package org.apache.flink.streaming.connectors.pinot;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.net.HostAndPort;

import java.io.Closeable;
import java.io.IOException;

import static java.lang.String.format;
import static org.testcontainers.utility.DockerImageName.parse;

@Testcontainers
public class PinotClusterContainer implements Closeable {

    private static final String PINOT_IMAGE_NAME = "apachepinot/pinot:0.6.0";
    private static final String ZOOKEEPER_IMAGE_NAME = "zookeeper:latest";

    private static final String ZOOKEEPER_INTERNAL_HOST = "zookeeper";

    private static final int ZOOKEEPER_PORT = 2181;
    private static final int CONTROLLER_PORT = 9000;
    private static final int BROKER_PORT = 8099;
    private static final int SERVER_ADMIN_PORT = 8097;
    private static final int SERVER_PORT = 8098;
    private static final int GRPC_PORT = 8090;

    @Container
    private final GenericContainer<?> controller;
    @Container
    private final GenericContainer<?> broker;
    @Container
    private final GenericContainer<?> server;
    @Container
    private final GenericContainer<?> zookeeper;

    public PinotClusterContainer() {
        Network network = Network.newNetwork();

        zookeeper = new GenericContainer<>(parse(ZOOKEEPER_IMAGE_NAME))
                .withStartupAttempts(3)
                .withNetwork(network)
                .withNetworkAliases(ZOOKEEPER_INTERNAL_HOST)
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZOOKEEPER_PORT))
                .withExposedPorts(ZOOKEEPER_PORT);

        controller = new GenericContainer<>(parse(PINOT_IMAGE_NAME))
                .withStartupAttempts(3)
                .withNetwork(network)
                .withClasspathResourceMapping("/pinot-controller", "/var/pinot/controller/config", BindMode.READ_ONLY)
                .withEnv("JAVA_OPTS", "-Xmx512m -Dlog4j2.configurationFile=/opt/pinot/conf/pinot-controller-log4j2.xml -Dplugins.dir=/opt/pinot/plugins")
                .withCommand("StartController", "-configFileName", "/var/pinot/controller/config/pinot-controller.conf")
                .withNetworkAliases("pinot-controller", "localhost")
                .withExposedPorts(CONTROLLER_PORT);

        broker = new GenericContainer<>(parse(PINOT_IMAGE_NAME))
                .withStartupAttempts(3)
                .withNetwork(network)
                .withClasspathResourceMapping("/pinot-broker", "/var/pinot/broker/config", BindMode.READ_ONLY)
                .withEnv("JAVA_OPTS", "-Xmx512m -Dlog4j2.configurationFile=/opt/pinot/conf/pinot-broker-log4j2.xml -Dplugins.dir=/opt/pinot/plugins")
                .withCommand("StartBroker", "-clusterName", "pinot", "-zkAddress", getZookeeperInternalHostPort(), "-configFileName", "/var/pinot/broker/config/pinot-broker.conf")
                .withNetworkAliases("pinot-broker", "localhost")
                .withExposedPorts(BROKER_PORT);

        server = new GenericContainer<>(parse(PINOT_IMAGE_NAME))
                .withStartupAttempts(3)
                .withNetwork(network)
                .withClasspathResourceMapping("/pinot-server", "/var/pinot/server/config", BindMode.READ_ONLY)
                .withEnv("JAVA_OPTS", "-Xmx512m -Dlog4j2.configurationFile=/opt/pinot/conf/pinot-server-log4j2.xml -Dplugins.dir=/opt/pinot/plugins")
                .withCommand("StartServer", "-clusterName", "pinot", "-zkAddress", getZookeeperInternalHostPort(), "-configFileName", "/var/pinot/server/config/pinot-server.conf")
                .withNetworkAliases("pinot-server", "localhost")
                .withExposedPorts(SERVER_PORT, SERVER_ADMIN_PORT, GRPC_PORT);
    }

    public void start() {
        zookeeper.start();
        controller.start();
        broker.start();
        server.start();
    }

    public void stop() {
        server.stop();
        broker.stop();
        controller.stop();
        zookeeper.stop();
    }

    private String getZookeeperInternalHostPort() {
        return format("%s:%s", ZOOKEEPER_INTERNAL_HOST, ZOOKEEPER_PORT);
    }

    public HostAndPort getControllerHostAndPort() {
        return HostAndPort.fromParts(controller.getHost(), controller.getMappedPort(CONTROLLER_PORT));
    }

    public HostAndPort getBrokerHostAndPort() {
        return HostAndPort.fromParts(broker.getHost(), broker.getMappedPort(BROKER_PORT));
    }

    public HostAndPort getServerHostAndPort() {
        return HostAndPort.fromParts(server.getHost(), server.getMappedPort(SERVER_PORT));
    }


    @Override
    public void close() throws IOException {
        stop();
    }
}
