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
package org.apache.flink.streaming.connectors.influxdb.sink;

import org.apache.flink.streaming.connectors.influxdb.sink.writer.InfluxDBTestSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class InfluxDBSinkBuilderTest {

    @Test
    void shouldNotBuildSinkWhenURLIsNotProvided() {
        final NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                InfluxDBSink.builder()
                                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                                        .setInfluxDBUsername("username")
                                        .setInfluxDBPassword("password")
                                        .setInfluxDBBucket("bucket")
                                        .setInfluxDBOrganization("organization")
                                        .build());
        assertEquals(exception.getMessage(), "The InfluxDB URL is required but not provided.");
    }

    @Test
    void shouldNotBuildSinkWhenTokenNotProvidedAndUsernameIsNotProvided() {
        final IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                InfluxDBSink.builder()
                                        .setInfluxDBUrl("http://localhost:8086")
                                        .setInfluxDBPassword("password")
                                        .setInfluxDBBucket("bucket")
                                        .setInfluxDBOrganization("organization")
                                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                                        .build());
        assertEquals(exception.getMessage(),
                "Either the InfluxDB username and password or InfluxDB token are required but neither provided");
    }

    @Test
    void shouldNotBuildSinkWhenTokenNotProvidedAndPasswordIsNotProvided() {
        final IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                InfluxDBSink.builder()
                                        .setInfluxDBUrl("http://localhost:8086")
                                        .setInfluxDBUsername("username")
                                        .setInfluxDBBucket("bucket")
                                        .setInfluxDBOrganization("organization")
                                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                                        .build());
        assertEquals(exception.getMessage(),
                "Either the InfluxDB username and password or InfluxDB token are required but neither provided");
    }

    @Test
    void shouldNotBuildSinkWhenTokenProvidedAndUsernamePasswordIsProvided() {
        final IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                InfluxDBSink.builder()
                                        .setInfluxDBUrl("http://localhost:8086")
                                        .setInfluxDBToken("token")
                                        .setInfluxDBUsername("username")
                                        .setInfluxDBPassword("password")
                                        .setInfluxDBBucket("bucket")
                                        .setInfluxDBOrganization("organization")
                                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                                        .build());
        assertEquals(exception.getMessage(),
                "Either the InfluxDB username and password or InfluxDB token are required but both provided");
    }

    @Test
    void shouldNotBuildSinkWhenBucketIsNotProvided() {
        final NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                InfluxDBSink.builder()
                                        .setInfluxDBUrl("http://localhost:8086")
                                        .setInfluxDBUsername("username")
                                        .setInfluxDBPassword("password")
                                        .setInfluxDBOrganization("organization")
                                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                                        .build());
        assertEquals(exception.getMessage(), "The Bucket name is required but not provided.");
    }

    @Test
    void shouldNotBuildSinkWhenOrganizationIsNotProvided() {
        final NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                InfluxDBSink.builder()
                                        .setInfluxDBUrl("http://localhost:8086")
                                        .setInfluxDBUsername("username")
                                        .setInfluxDBPassword("password")
                                        .setInfluxDBBucket("bucket")
                                        .setInfluxDBSchemaSerializer(new InfluxDBTestSerializer())
                                        .build());
        assertEquals(exception.getMessage(), "The Organization name is required but not provided.");
    }

    @Test
    void shouldNotBuildSinkWhenSchemaSerializerIsNotProvided() {
        final NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                InfluxDBSink.builder()
                                        .setInfluxDBUrl("http://localhost:8086")
                                        .setInfluxDBUsername("username")
                                        .setInfluxDBPassword("password")
                                        .setInfluxDBBucket("bucket")
                                        .setInfluxDBOrganization("organization")
                                        .build());
        assertEquals(exception.getMessage(), "Serialization schema is required but not provided.");
    }

    @Test
    void shouldNotBuildSinkWhenBufferSizeIsZero() {
        final IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> InfluxDBSink.builder().setWriteBufferSize(0));
        assertEquals(exception.getMessage(), "The buffer size should be greater than 0.");
    }
}
