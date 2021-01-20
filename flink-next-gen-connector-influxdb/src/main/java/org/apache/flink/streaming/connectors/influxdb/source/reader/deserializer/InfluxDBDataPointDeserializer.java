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
package org.apache.flink.streaming.connectors.influxdb.source.reader.deserializer;

import java.io.Serializable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.influxdb.source.DataPoint;
import org.apache.flink.util.Collector;

/** An interface for the deserialization of InfluxDB data points. */
public interface InfluxDBDataPointDeserializer<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Deserialize a data point into the given collector.
     *
     * @param dataPoint the {@code DataPoint} to deserialize.
     * @throws Exception if the deserialization failed.
     */
    void deserialize(DataPoint dataPoint, Collector<T> collector) throws Exception;

    // static function for single data point

    @Override
    default TypeInformation<T> getProducedType() {
        return TypeExtractor.createTypeInfo(
                InfluxDBDataPointDeserializer.class, this.getClass(), 0, null, null);
    }
}
