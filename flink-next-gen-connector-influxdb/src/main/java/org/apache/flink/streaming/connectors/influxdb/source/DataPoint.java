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
package org.apache.flink.streaming.connectors.influxdb.source;

import com.influxdb.Arguments;
import java.util.Map;
import java.util.TreeMap;

/** InfluxDB data points */
/* Split Reader (HTTP Server) Line Protocol -> DataPoint -> Deserializer */
public class DataPoint {
    private final String name;
    private final Map<String, String> tags = new TreeMap();
    private final Map<String, Object> fields = new TreeMap();
    private Number time;

    public DataPoint(final String measurementName) {
        Arguments.checkNotNull(measurementName, "measurement");
        this.name = measurementName;
    }

    public String getMeasurement() {
        return this.name;
    }

    public DataPoint putField(final String field, final Object value) {
        Arguments.checkNonEmpty(field, "fieldName");
        this.fields.put(field, value);
        return this;
    }

    public Object getField(final String field) {
        Arguments.checkNonEmpty(field, "fieldName");
        return this.fields.getOrDefault(field, null);
    }

    public DataPoint time(final Number time) {
        this.time = time;
        return this;
    }

    public Number getTime() {
        return this.time;
    }

    public DataPoint addTag(final String key, final String value) {
        Arguments.checkNotNull(key, "tagName");
        this.tags.put(key, value);
        return this;
    }
}
