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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

public class FlumeServerTest {

    private static final Integer EXPOSED_PORT = 44444;

    public GenericContainer<?> sink = new GenericContainer<>("eskabetxe/flume")

            .withCopyFileToContainer(MountableFile.forClasspathResource("docker/conf/sink.conf"), "/opt/flume-config/flume.conf")
            .withEnv("FLUME_AGENT_NAME", "docker");

    public GenericContainer<?> source = new GenericContainer<>("eskabetxe/flume")
            .withCopyFileToContainer(MountableFile.forClasspathResource("docker/conf/source.conf"), "/opt/flume-config/flume.conf")
            .withExposedPorts(EXPOSED_PORT)
            .withEnv("FLUME_AGENT_NAME", "docker")
            .dependsOn(sink);

    @BeforeEach
    void start() {
        sink.start();
        source.start();
    }

    @AfterEach
    void stop() {
        source.stop();
        sink.stop();
    }

    protected String getHost() {
        return source.getHost();
    }

    protected Integer getPort() {
        return source.getMappedPort(EXPOSED_PORT);
    }

}
