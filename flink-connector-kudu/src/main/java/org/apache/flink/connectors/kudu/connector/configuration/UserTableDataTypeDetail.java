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
package org.apache.flink.connectors.kudu.connector.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.apache.kudu.shaded.com.google.common.collect.Maps;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

@Internal
public class UserTableDataTypeDetail {
    private List<StreamingColumn> streamingCols = Lists.newArrayList();
    private Map<String, ReflectionTypeDetail> reflectionTypeDetailByColNames = Maps.newHashMap();
    private Constructor<?> userTableDataTypeConstructor;

    public List<StreamingColumn> getStreamingCols() {
        return streamingCols;
    }

    public void setStreamingCols(List<StreamingColumn> streamingCols) {
        this.streamingCols = streamingCols;
    }

    public Map<String, ReflectionTypeDetail> getReflectionTypeDetailByColNames() {
        return reflectionTypeDetailByColNames;
    }

    public void setReflectionTypeDetailByColNames(Map<String, ReflectionTypeDetail> reflectionTypeDetailByColNames) {
        this.reflectionTypeDetailByColNames = reflectionTypeDetailByColNames;
    }

    public Constructor<?> getUserTableDataTypeConstructor() {
        return userTableDataTypeConstructor;
    }

    public void setUserTableDataTypeConstructor(Constructor<?> userTableDataTypeConstructor) {
        this.userTableDataTypeConstructor = userTableDataTypeConstructor;
    }
}
