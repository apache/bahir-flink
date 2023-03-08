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
package org.apache.flink.streaming.connectors.influxdb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InfluxDBContainerCustom
        extends GenericContainer<InfluxDBContainerCustom> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBContainerCustom.class);

    public static final Integer INFLUXDB_PORT = 8086;

    private static final String TAG = "v2.0.2";
    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("quay.io/influxdb/influxdb:v2.0.2");
    private static final int NO_CONTENT_STATUS_CODE = 204;
    private static final String INFLUX_SETUP_SH = "influx-setup.sh";

    private static final String username = "test-user";
    private static final String password = "test-password";
    private static final String bucket = "test-bucket";
    private static final String organization = "test-org";

    public InfluxDBContainerCustom(final DockerImageName imageName) {
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

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getBucket() {
        return bucket;
    }

    public String getOrganization() {
        return organization;
    }

    private void setEnv() {
        this.addEnv("INFLUXDB_USER", username);
        this.addEnv("INFLUXDB_PASSWORD", password);
        this.addEnv("INFLUXDB_BUCKET", bucket);
        this.addEnv("INFLUXDB_ORG", organization);
    }

    private void startContainer() {
        this.withCopyFileToContainer(
                MountableFile.forClasspathResource(INFLUX_SETUP_SH),
                String.format("%s", INFLUX_SETUP_SH));
        this.start();
        this.setUpInfluxDB();
        LOG.info("Started InfluxDB container on: {}", this.getUrl());
    }

    private void setUpInfluxDB() {
        final ExecResult execResult;
        try {
            execResult = this.execInContainer("chmod", "-x", String.format("/%s", INFLUX_SETUP_SH));
            assertEquals(execResult.getExitCode(), 0);
            final ExecResult writeResult =
                    this.execInContainer("/bin/bash", String.format("/%s", INFLUX_SETUP_SH));
            assertEquals(writeResult.getExitCode(), 0);
        } catch (final InterruptedException | IOException e) {
            LOG.error("An error occurred while setting up InfluxDB {}", e.getMessage());
        }
    }

    public String getUrl() {
        return "http://" + this.getHost() + ":" + this.getMappedPort(INFLUXDB_PORT);
    }
}
