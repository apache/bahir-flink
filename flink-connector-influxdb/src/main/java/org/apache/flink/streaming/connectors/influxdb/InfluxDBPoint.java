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

package org.apache.flink.streaming.connectors.influxdb;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a InfluxDB database Point.
 */
public class InfluxDBPoint {

    private String measurement;
    private long timestamp;
    private Map<String, String> tags;
    private Map<String, Object> fields;

    public InfluxDBPoint(String measurement, long timestamp) {
        this.measurement = measurement;
        this.timestamp = timestamp;
        this.fields = new HashMap<>();
        this.tags = new HashMap<>();
    }

    public InfluxDBPoint(String measurement, long timestamp, Map<String, String> tags, Map<String, Object> fields) {
        this.measurement = measurement;
        this.timestamp = timestamp;
        this.tags = tags;
        this.fields = fields;
    }

    public String getMeasurement() {
        return measurement;
    }

    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }
}
