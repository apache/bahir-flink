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
package org.apache.flink.streaming.connectors.kudu.connector;

import org.apache.flink.types.Row;
import org.apache.kudu.Schema;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Stream;

public class KuduRow extends Row {

    private Map<String, Integer> rowNames;

    public KuduRow(Integer arity) {
        super(arity);
        rowNames = new LinkedHashMap<>();
    }

    public KuduRow(Object object, Schema schema) {
        super(validFields(object));
        for (Class<?> c = object.getClass(); c != null; c = c.getSuperclass()) {
            basicValidation(c.getDeclaredFields())
                    .filter(field -> schema.getColumn(field.getName()) != null)
                    .forEach(cField -> {
                        try {
                            cField.setAccessible(true);
                            setField(schema.getColumnIndex(cField.getName()), cField.getName(), cField.get(object));
                        } catch (IllegalAccessException e) {
                            String error = String.format("Cannot get value for %s", cField.getName());
                            throw new IllegalArgumentException(error, e);
                        }
                    });
        }
    }


    public Object getField(String name) {
        return super.getField(rowNames.get(name));
    }

    public void setField(int pos, String name, Object value) {
        super.setField(pos, value);
        this.rowNames.put(name, pos);
    }

    public boolean isNull(int pos) {
        return getField(pos) == null;
    }

    private static int validFields(Object object) {
        Long validField = 0L;
        for (Class<?> c = object.getClass(); c != null; c = c.getSuperclass()) {
            validField += basicValidation(c.getDeclaredFields()).count();
        }
        return validField.intValue();
    }

    private static Stream<Field> basicValidation(Field[] fields) {
        return Arrays.stream(fields)
                .filter(cField -> !Modifier.isStatic(cField.getModifiers()))
                .filter(cField -> !Modifier.isTransient(cField.getModifiers()));
    }

    public Map<String,Object> blindMap() {
        Map<String,Object> toRet = new LinkedHashMap<>();
        rowNames.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue))
                .forEach(entry -> toRet.put(entry.getKey(), super.getField(entry.getValue())));
        return  toRet;
    }

    public <P> P blind(Class<P> clazz) {
        P o = createInstance(clazz);

        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            Field[] fields = c.getDeclaredFields();
            for (Field cField : fields) {
                try {
                    if(rowNames.containsKey(cField.getName())
                            && !Modifier.isStatic(cField.getModifiers())
                            && !Modifier.isTransient(cField.getModifiers())) {

                        cField.setAccessible(true);
                        Object value = getField(cField.getName());
                        if (value != null) {
                            if (cField.getType() == value.getClass()) {
                                cField.set(o, value);
                            } else if (cField.getType() == Long.class && value.getClass() == Date.class) {
                                cField.set(o, ((Date) value).getTime());
                            } else {
                                cField.set(o, value);
                            }
                        }
                    }
                } catch (IllegalAccessException e) {
                    String error = String.format("Cannot get value for %s", cField.getName());
                    throw new IllegalArgumentException(error, e);
                }
            }
        }

        return o;

    }


    private <P> P createInstance(Class<P> clazz) {
        try {
            return clazz.getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            String error = String.format("Cannot create instance for %s", clazz.getSimpleName());
            throw new IllegalArgumentException(error, e);
        }
    }

    @Override
    public String toString() {
        return blindMap().toString();
    }
}
