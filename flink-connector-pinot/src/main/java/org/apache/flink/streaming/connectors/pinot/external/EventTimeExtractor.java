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

package org.apache.flink.streaming.connectors.pinot.external;

import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Defines the interface for event time extractors
 *
 * @param <IN> Type of incoming elements
 */
public interface EventTimeExtractor<IN> extends Serializable {

    /**
     * Extracts event time from incoming elements.
     *
     * @param element Incoming element
     * @param context Context of SinkWriter
     * @return timestamp
     */
    long getEventTime(IN element, SinkWriter.Context context);

    /**
     * @return Name of column in Pinot target table that contains the timestamp.
     */
    String getTimeColumn();

    /**
     * @return Unit of the time column in the Pinot target table.
     */
    TimeUnit getSegmentTimeUnit();
}
