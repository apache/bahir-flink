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

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class GenericRecord implements Serializable {
    private final Map<String, Object> map;

    public GenericRecord() {
        this.map = new LinkedHashMap<>();
    }

    public GenericRecord(Map<String, Object> map) {
        this.map = new LinkedHashMap<>(map);
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public void setMap(Map<String, Object> map) {
        this.map.putAll(map);
    }

    public Object get(String field) {
        return map.get(field);
    }

    public void put(String field, Object value) {
        this.map.put(field, value);
    }

    @Override
    public String toString() {
        return this.getMap().toString();
    }
}
