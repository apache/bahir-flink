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
package util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class InfluxDBContainer<SELF extends InfluxDBContainer<SELF>>
        extends GenericContainer<SELF> {
    private static final Integer INFLUXDB_PORT = 8086;
    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("quay.io/influxdb/influxdb:v2.0.2");
    private static final String INFLUX_SETUP = "influx-setup.sh";
    private static final String DATA = "adsb-test.txt";
    public static final int NO_CONTENT_STATUS_CODE = 204;

    private final String username;
    private final String password;
    private final String bucket;
    private final String organization;

    public InfluxDBContainer() {
        super(DEFAULT_IMAGE_NAME);
        this.username = "test-username";
        this.password = "test-password";
        this.bucket = "test-bucket";
        this.organization = "test-org";
        this.waitStrategy =
                (new WaitAllStrategy())
                        .withStrategy(
                                Wait.forHttp("/ping")
                                        .withBasicCredentials(this.username, this.password)
                                        .forStatusCode(NO_CONTENT_STATUS_CODE))
                        .withStrategy(Wait.forListeningPort());
        this.withExposedPorts(INFLUXDB_PORT);
    }

    public void startPreIngestedInfluxDB() {
        this.withCopyFileToContainer(
                        MountableFile.forClasspathResource(DATA), String.format("/%s", DATA))
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource(INFLUX_SETUP),
                        String.format("%s", INFLUX_SETUP));

        this.start();

        this.writeDataToInfluxDB();
    }

    public InfluxDB getNewInfluxDB() {
        final InfluxDB influxDB =
                InfluxDBFactory.connect(this.getUrl(), this.username, this.password);
        influxDB.setDatabase(this.bucket);
        return influxDB;
    }

    private void writeDataToInfluxDB() {
        try {
            final Container.ExecResult execResult =
                    this.execInContainer("chmod", "-x", "/influx-setup.sh");
            assertEquals(execResult.getExitCode(), 0);
            final Container.ExecResult writeResult =
                    this.execInContainer(
                            "/bin/bash",
                            "/influx-setup.sh",
                            this.username,
                            this.password,
                            this.bucket,
                            this.organization,
                            DATA);
            assertEquals(writeResult.getExitCode(), 0);
        } catch (final IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String getUrl() {
        return "http://" + this.getHost() + ":" + this.getMappedPort(INFLUXDB_PORT);
    }
}
