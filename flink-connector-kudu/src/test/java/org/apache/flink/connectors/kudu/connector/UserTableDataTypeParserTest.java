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

import org.apache.flink.connectors.kudu.connector.configuration.ReflectionTypeDetail;
import org.apache.flink.connectors.kudu.connector.configuration.UserTableDataTypeDetail;
import org.apache.flink.connectors.kudu.connector.convertor.parser.UserTableDataTypeParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class UserTableDataTypeParserTest {
    @Test
    public void testParse() throws Exception {
        UserTableDataTypeDetail detail = UserTableDataTypeParser.getInstance().parse(UserType.class);

        Assert.assertEquals(2, detail.getStreamingCols().size());
        Assert.assertEquals("name_col", detail.getStreamingCols().get(0).getColName());
        Assert.assertEquals("id_col", detail.getStreamingCols().get(1).getColName());

        Map<String, ReflectionTypeDetail> reflectionTypeDetailMap = detail.getReflectionTypeDetailByColNames();

        Assert.assertEquals("id", reflectionTypeDetailMap.get("id_col").getField().getName());
        Assert.assertEquals("name", reflectionTypeDetailMap.get("name_col").getField().getName());
        Assert.assertEquals("age", reflectionTypeDetailMap.get("age_col").getField().getName());

        Assert.assertEquals("setId", reflectionTypeDetailMap.get("id_col").getMethod().getName());
        Assert.assertEquals("setName", reflectionTypeDetailMap.get("name_col").getMethod().getName());
        Assert.assertEquals("setAge", reflectionTypeDetailMap.get("age_col").getMethod().getName());

        Assert.assertEquals("org.apache.flink.connectors.kudu.connector.UserType",
                detail.getUserTableDataTypeConstructor().getName());
    }
}
