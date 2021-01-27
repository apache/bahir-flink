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
package org.apache.flink.streaming.connectors.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import lombok.Getter;
import lombok.SneakyThrows;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public final class InfluxDBContainer<SELF extends InfluxDBContainer<SELF>>
        extends GenericContainer<SELF> {

    public static final Integer INFLUXDB_PORT = 8086;

    private static final String REGISTRY = "quay.io";
    private static final String REPOSITORY = "influxdb/influxdb";
    private static final String TAG = "v2.0.2";
    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse(String.format("%s/%s:%s", REGISTRY, REPOSITORY, TAG));
    private static final int NO_CONTENT_STATUS_CODE = 204;
    private static final String INFLUX_SETUP_SH = "influx-setup.sh";

    @Getter private static final String username = "test-user";
    @Getter private static final String password = "test-password";
    @Getter private static final String bucket = "test-bucket";
    @Getter private static final String organization = "test-org";
    private static final int retention = 0;
    private final String retentionUnit = RetentionUnit.NANOSECONDS.label;

    private InfluxDBContainer(final DockerImageName imageName) {
        super(imageName);
        imageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
        this.setEnv();
        this.waitStrategy =
                (new WaitAllStrategy())
                        .withStrategy(
                                Wait.forHttp("/ping")
                                        .withBasicCredentials(username, password)
                                        .forStatusCode(NO_CONTENT_STATUS_CODE))
                        .withStrategy(Wait.forListeningPort());

        this.addExposedPort(INFLUXDB_PORT);
        this.startContainer();
    }

    public static InfluxDBContainer<?> createWithDefaultTag() {
        return new InfluxDBContainer<>(DEFAULT_IMAGE_NAME);
    }

    private void setEnv() {
        this.addEnv("INFLUXDB_USER", username);
        this.addEnv("INFLUXDB_PASSWORD", password);
        this.addEnv("INFLUXDB_BUCKET", bucket);
        this.addEnv("INFLUXDB_ORG", organization);
        this.addEnv("INFLUXDB_RETENTION", String.valueOf(retention));
        this.addEnv("INFLUXDB_RETENTION_UNIT", this.retentionUnit);
    }

    private void startContainer() {
        this.withCopyFileToContainer(
                MountableFile.forClasspathResource(INFLUX_SETUP_SH),
                String.format("%s", INFLUX_SETUP_SH));
        this.start();
        this.setUpInfluxDB();
    }

    @SneakyThrows({InterruptedException.class, IOException.class})
    private void setUpInfluxDB() {
        final ExecResult execResult =
                this.execInContainer("chmod", "-x", String.format("/%s", INFLUX_SETUP_SH));
        assertEquals(execResult.getExitCode(), 0);
        final ExecResult writeResult =
                this.execInContainer("/bin/bash", String.format("/%s", INFLUX_SETUP_SH));
        assertEquals(writeResult.getExitCode(), 0);
    }

    public String getUrl() {
        return "http://" + this.getHost() + ":" + this.getMappedPort(INFLUXDB_PORT);
    }
}
