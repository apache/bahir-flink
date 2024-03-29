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

package org.apache.flink.streaming.siddhi.utils;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Siddhi Type Utils for conversion between Java Type, Siddhi Field Type, Stream Definition, and Flink Type Information.
 */
public class SiddhiTypeFactory {
    private static final Map<Class<?>, Attribute.Type> JAVA_TO_SIDDHI_TYPE = new HashMap<>();
    private static final Map<Attribute.Type, Class<?>> SIDDHI_TO_JAVA_TYPE = new HashMap<>();

    static {
        registerType(String.class, Attribute.Type.STRING);
        registerType(Integer.class, Attribute.Type.INT);
        registerType(int.class, Attribute.Type.INT);
        registerType(Long.class, Attribute.Type.LONG);
        registerType(long.class, Attribute.Type.LONG);
        registerType(Float.class, Attribute.Type.FLOAT);
        registerType(float.class, Attribute.Type.FLOAT);
        registerType(Double.class, Attribute.Type.DOUBLE);
        registerType(double.class, Attribute.Type.DOUBLE);
        registerType(Boolean.class, Attribute.Type.BOOL);
        registerType(boolean.class, Attribute.Type.BOOL);
    }

    public static void registerType(Class<?> javaType, Attribute.Type siddhiType) {
        if (JAVA_TO_SIDDHI_TYPE.containsKey(javaType)) {
            throw new IllegalArgumentException("Java type: " + javaType + " or siddhi type: " + siddhiType + " were already registered");
        }
        JAVA_TO_SIDDHI_TYPE.put(javaType, siddhiType);
        SIDDHI_TO_JAVA_TYPE.put(siddhiType, javaType);
    }

    public static AbstractDefinition getStreamDefinition(String executionPlan, String streamId) {
        SiddhiManager siddhiManager = null;
        SiddhiAppRuntime runtime = null;
        try {
            siddhiManager = new SiddhiManager();
            runtime = siddhiManager.createSiddhiAppRuntime(executionPlan);
            Map<String, StreamDefinition> definitionMap = runtime.getStreamDefinitionMap();
            if (definitionMap.containsKey(streamId)) {
                return definitionMap.get(streamId);
            } else {
                throw new IllegalArgumentException("Unknown stream id" + streamId);
            }
        } finally {
            if (runtime != null) {
                runtime.shutdown();
            }
            if (siddhiManager != null) {
                siddhiManager.shutdown();
            }
        }
    }

    public static <T extends Tuple> TypeInformation<T> getTupleTypeInformation(AbstractDefinition definition) {
        int tupleSize = definition.getAttributeList().size();
        TypeInformation[] typeInformations = new TypeInformation[tupleSize];
        List<Attribute> attributes = definition.getAttributeList();
        try {
            for (int i = 0; i < attributes.size() ; i++) {
                Class<?> clazz = getJavaType(attributes.get(i).getType());
                typeInformations[i] = TypeInformation.of(clazz);
            }
            return Types.TUPLE(typeInformations);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Failed to get Type Information.", ex);
        }
    }

    public static <T extends Tuple> TypeInformation<T> getTupleTypeInformation(String executionPlan, String streamId) {
        return getTupleTypeInformation(getStreamDefinition(executionPlan, streamId));
    }

    @SuppressWarnings("unchecked")
    private static final TypeInformation<?> MAP_TYPE_INFORMATION = TypeExtractor.createTypeInfo(new HashMap<String, Object>().getClass());

    public static TypeInformation<Map<String, Object>> getMapTypeInformation() {
        return (TypeInformation<Map<String, Object>>) MAP_TYPE_INFORMATION;
    }

    public static <F> Attribute.Type getAttributeType(TypeInformation<F> fieldType) {
        if (JAVA_TO_SIDDHI_TYPE.containsKey(fieldType.getTypeClass())) {
            return JAVA_TO_SIDDHI_TYPE.get(fieldType.getTypeClass());
        } else {
            return Attribute.Type.OBJECT
                ;
        }
    }

    public static Class<?> getJavaType(Attribute.Type attributeType) {
        if (!SIDDHI_TO_JAVA_TYPE.containsKey(attributeType)) {
            throw new IllegalArgumentException("Unable to get java type for siddhi attribute type: " + attributeType);
        }
        return SIDDHI_TO_JAVA_TYPE.get(attributeType);
    }

    public static <T> TypeInformation<Tuple2<String, T>> getStreamTupleTypeInformation(TypeInformation<T> typeInformation) {
        return Types.TUPLE(Types.STRING, typeInformation);
    }
}
