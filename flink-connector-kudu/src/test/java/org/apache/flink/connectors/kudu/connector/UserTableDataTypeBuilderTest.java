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

import org.apache.flink.connectors.kudu.connector.configuration.UserTableDataTypeDetail;
import org.apache.flink.connectors.kudu.connector.convertor.builder.UserTableDataTypeBuilder;
import org.apache.flink.connectors.kudu.connector.convertor.parser.UserTableDataTypeParser;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class UserTableDataTypeBuilderTest {
    @Test
    public void build() throws Exception {
        UserTableDataTypeDetail detail = UserTableDataTypeParser.getInstance().parse(UserType.class);

        UserTableDataTypeBuilder builder = new UserTableDataTypeBuilder(detail);

        UserType userType = (UserType)detail.getUserTableDataTypeConstructor().newInstance();
        builder.build(userType, "id_col", new Long[]{123L});
        builder.build(userType, "name_col", new String[]{"hello_world"});
        builder.build(userType, "age_col", new Integer[]{100});

        Assertions.assertEquals(Long.valueOf(123L), userType.getId());
        Assertions.assertEquals("hello_world", userType.getName());
        Assertions.assertEquals(Integer.valueOf(100), userType.getAge());
    }
}
