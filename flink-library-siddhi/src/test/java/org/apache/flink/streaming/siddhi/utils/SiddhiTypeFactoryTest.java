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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.Assert;
import org.junit.Test;

public class SiddhiTypeFactoryTest {
    @Test
    public void testTypeInfoParser() {
        TypeInformation<Tuple3<String, Long, Object>> type1 =
                Types.TUPLE(Types.STRING, Types.LONG, Types.GENERIC(Object.class));
        Assert.assertNotNull(type1);
        TypeInformation<Tuple4<String, Long, Object, InnerPojo>> type2 =
                Types.TUPLE(Types.STRING, Types.LONG, Types.GENERIC(Object.class), Types.GENERIC(InnerPojo.class));
        Assert.assertNotNull(type2);
    }

    public static class InnerPojo {
    }

    @Test
    public void testBuildTypeInformationForSiddhiStream() {
        String query = "define stream inputStream (timestamp long, name string, value double);"
            + "from inputStream select name, value insert into outputStream;";
        TypeInformation<Tuple3<Long, String, Double>> inputStreamType = SiddhiTypeFactory.getTupleTypeInformation(query, "inputStream");
        TypeInformation<Tuple2<String, Double>> outputStreamType = SiddhiTypeFactory.getTupleTypeInformation(query, "outputStream");

        Assert.assertNotNull(inputStreamType);
        Assert.assertNotNull(outputStreamType);
    }
}
