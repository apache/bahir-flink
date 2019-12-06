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
package org.apache.flink.connectors.kudu.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

@PublicEvolving
public class KuduRow extends Row {

    private Map<String, Integer> rowNames;

    private int curRowPos = 0;

    public KuduRow(Integer arity) {
        super(arity);
        rowNames = new LinkedHashMap<>();
    }

    public Object getField(String name) {
        return super.getField(rowNames.get(name));
    }

    public boolean hasField(String name) {
        return rowNames.get(name) != null;
    }

    public void setField(String name, Object value) {
        super.setField(curRowPos, value);
        this.rowNames.put(name, curRowPos);
        curRowPos++;
    }

    public boolean isNull(String name) {
        return isNull(rowNames.get(name));
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

    @Override
    public String toString() {
        return blindMap().toString();
    }
}
