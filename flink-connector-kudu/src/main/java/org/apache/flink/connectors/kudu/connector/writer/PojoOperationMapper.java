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
package org.apache.flink.connectors.kudu.connector.writer;

import org.apache.flink.annotation.PublicEvolving;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@PublicEvolving
public class PojoOperationMapper<T> extends AbstractSingleOperationMapper<T> {

    private final Field[] fields;

    protected PojoOperationMapper(Class<T> pojoClass, String[] columnNames) { this(pojoClass, columnNames, null); }

    public PojoOperationMapper(Class<T> pojoClass, String[] columnNames, KuduOperation operation) {
        super(columnNames, operation);
        fields = initFields(pojoClass, columnNames);
    }

    public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
        fields.addAll(Arrays.asList(type.getDeclaredFields()));

        if (type.getSuperclass() != null) {
            getAllFields(fields, type.getSuperclass());
        }

        return fields;
    }

    private Field[] initFields(Class<T> pojoClass, String[] columnNames) {
        Map<String, Field> allFields = new HashMap<>();
        getAllFields(new ArrayList<>(), pojoClass).stream().forEach(f -> {
            if (!allFields.containsKey(f.getName())) {
                allFields.put(f.getName(), f);
            }
        });

        Field[] fields = new Field[columnNames.length];

        for (int i = 0; i < columnNames.length; i++) {
            Field f = allFields.get(columnNames[i]);
            if (f == null) {
                throw new RuntimeException("Cannot find field " + columnNames[i] + ". List of detected fields: " + allFields.keySet());
            }
            f.setAccessible(true);
            fields[i] = f;
        }

        return fields;
    }

    @Override
    public Object getField(T input, int i) {
        try {
            return fields[i].get(input);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("This is a bug");
        }
    }
}
