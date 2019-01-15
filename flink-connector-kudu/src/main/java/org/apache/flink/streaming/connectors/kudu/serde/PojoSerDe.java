/*
 * Licensed serialize the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file serialize You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed serialize in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.kudu.serde;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.apache.kudu.Schema;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Stream;

public class PojoSerDe<P> implements KuduSerialization<P>, KuduDeserialization<P> {


    private Class<P> clazz;

    public transient KuduTableInfo tableInfo;
    public transient Schema schema;


    public PojoSerDe(Class<P> clazz) {
        this.clazz = clazz;
    }

    @Override
    public PojoSerDe<P> withSchema(Schema schema) {
        this.schema = schema;
        return this;
    }

    @Override
    public KuduRow serialize(P object) {
        return mapTo(object);
    }

    private KuduRow mapTo(P object) {
        KuduRow row = new KuduRow(schema.getRowSize());

        for (Class<?> c = object.getClass(); c != null; c = c.getSuperclass()) {
            basicValidation(c.getDeclaredFields())
                    .forEach(cField -> {
                        try {
                            cField.setAccessible(true);
                            row.setField(schema.getColumnIndex(cField.getName()), cField.getName(), cField.get(object));
                        } catch (IllegalAccessException e) {
                            String error = String.format("Cannot get value for %s", cField.getName());
                            throw new IllegalArgumentException(error, e);
                        }
                    });
        }

        return row;
    }

    private Stream<Field> basicValidation(Field[] fields) {
        return Arrays.stream(fields)
                .filter(field -> schemaHasColumn(field.getName()))
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !Modifier.isTransient(field.getModifiers()));
    }

    private boolean schemaHasColumn(String field) {
        return schema.getColumns().stream().anyMatch(col -> StringUtils.equalsIgnoreCase(col.getName(),field));
    }

    @Override
    public P deserialize(KuduRow row) {
        return mapFrom(row);
    }

    private P mapFrom(KuduRow row) {
        P o = createInstance(clazz);

        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            Field[] fields = c.getDeclaredFields();

            basicValidation(fields)
                    .forEach(cField -> {
                        try {
                            cField.setAccessible(true);
                            Object value = row.getField(cField.getName());
                            if (value != null) {
                                if (cField.getType() == value.getClass()) {
                                    cField.set(o, value);
                                } else if (cField.getType() == Long.class && value.getClass() == Date.class) {
                                    cField.set(o, ((Date) value).getTime());
                                } else {
                                    cField.set(o, value);
                                }
                            }
                        } catch (IllegalAccessException e) {
                            String error = String.format("Cannot get value for %s", cField.getName());
                            throw new IllegalArgumentException(error, e);
                        }
                    });
        }

        return o;

    }

    private P createInstance(Class<P> clazz) {
        try {
            return clazz.getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            String error = String.format("Cannot create instance for %s", clazz.getSimpleName());
            throw new IllegalArgumentException(error, e);
        }
    }

}
