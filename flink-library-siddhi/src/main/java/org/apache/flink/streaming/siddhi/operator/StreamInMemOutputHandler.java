/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.siddhi.operator;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.siddhi.utils.SiddhiTupleFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;

/**
 * Siddhi Stream output callback handler and conver siddhi {@link Event} to required output type,
 * according to output {@link TypeInformation} and siddhi schema {@link AbstractDefinition}
 */
public class StreamInMemOutputHandler<R> extends StreamCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamInMemOutputHandler.class);

    private final AbstractDefinition definition;
    private final TypeInformation<R> typeInfo;
    private final ObjectMapper objectMapper;


    private final LinkedList<StreamRecord<R>> collectedRecords;

    public StreamInMemOutputHandler(TypeInformation<R> typeInfo, AbstractDefinition definition) {
        this.typeInfo = typeInfo;
        this.definition = definition;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.collectedRecords = new LinkedList<>();
    }

    @Override
    public void receive(Event[] events) {
        for (Event event : events) {
            if (typeInfo == null || Map.class.isAssignableFrom(typeInfo.getTypeClass())) {
                collectedRecords.add(new StreamRecord<R>((R) toMap(event), event.getTimestamp()));
            } else if (typeInfo.isTupleType()) {
                Tuple tuple = this.toTuple(event);
                collectedRecords.add(new StreamRecord<R>((R) tuple, event.getTimestamp()));
            } else if (typeInfo instanceof PojoTypeInfo) {
                R obj;
                try {
                    obj = objectMapper.convertValue(toMap(event), typeInfo.getTypeClass());
                } catch (IllegalArgumentException ex) {
                    LOGGER.error("Failed to map event: " + event + " into type: " + typeInfo, ex);
                    throw ex;
                }
                collectedRecords.add(new StreamRecord<R>(obj, event.getTimestamp()));
            } else {
                throw new IllegalArgumentException("Unable to format " + event + " as type " + typeInfo);
            }
        }
    }


    @Override
    public synchronized void stopProcessing() {
        super.stopProcessing();
        this.collectedRecords.clear();
    }

    private Map<String, Object> toMap(Event event) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < definition.getAttributeNameArray().length; i++) {
            map.put(definition.getAttributeNameArray()[i], event.getData(i));
        }
        return map;
    }

    private <T extends Tuple> T toTuple(Event event) {
        return SiddhiTupleFactory.newTuple(event.getData());
    }

    public LinkedList<StreamRecord<R>> getCollectedRecords() {
        return collectedRecords;
    }
}
