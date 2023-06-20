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
package org.apache.flink.connectors.kudu.connector.convertor.builder;


import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.kudu.connector.configuration.UserTableDataTypeDetail;

@Internal
public class UserTableDataTypeBuilder<T> {
    private UserTableDataTypeDetail userTableDataTypeDetail;

    public UserTableDataTypeBuilder(UserTableDataTypeDetail userTableDataTypeDetail) {
        this.userTableDataTypeDetail = userTableDataTypeDetail;
    }

    public void build(T userTableDataInstance, String colName, Object[] params) {
        try {
            userTableDataTypeDetail
                    .getReflectionTypeDetailByColNames()
                    .get(colName)
                    .getMethod()
                    .invoke(userTableDataInstance, params);
        } catch (Exception e) {
            throw new IllegalArgumentException("Fail to invoke by column name: " + colName);
        }
    }
}
