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


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connectors.kudu.connector.configuration.StreamingColumn;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class StreamingKeyOffsetManagerTest {
    @Test
    public void testUpdate() throws Exception {
        List<StreamingColumn> streamingColumns = Lists.newArrayList();

        StreamingColumn one = new StreamingColumn("name_col", "name", String.class, 1, Locale.US);
        StreamingColumn two = new StreamingColumn("id_col", "id", Long.class, 2, Locale.US);

        streamingColumns.add(one);
        streamingColumns.add(two);

        UserType row = new UserType();
        row.setId(1L);
        row.setName("hello_world");
        row.setAge(1);

        UserType row2 = new UserType();
        row2.setId(2L);
        row2.setName("hello_world");
        row2.setAge(1);

        UserType row3 = new UserType();
        row3.setId(1L);
        row3.setName("hello_world2");
        row3.setAge(1);

        StreamingLocalEventsManager<UserType> streamingLocalEventsManager =
                new StreamingLocalEventsManager<>(Arrays.asList(one, two), null, null);

        Assertions.assertEquals(StringUtils.EMPTY, streamingLocalEventsManager.getCurrentHWMStr());

        streamingLocalEventsManager.update(row);
        Assertions.assertEquals("hello_world|1", streamingLocalEventsManager.getCurrentHWMStr());

        streamingLocalEventsManager.update(row2);
        Assertions.assertEquals("hello_world|2", streamingLocalEventsManager.getCurrentHWMStr());

        Assertions.assertEquals(2, streamingLocalEventsManager.getSortedLocalEvents().size());
        Iterator<UserType> itr = streamingLocalEventsManager.getSortedLocalEvents().iterator();
        long id = 1L;
        while (itr.hasNext()) {
            Assertions.assertEquals(Long.valueOf(id++), itr.next().getId());
        }

        streamingLocalEventsManager.update(row3);
        Assertions.assertEquals("hello_world2|1", streamingLocalEventsManager.getCurrentHWMStr());

        String[] upperBoundKey = streamingLocalEventsManager.getUserConfiguredUpperKey();
        Assertions.assertNull(upperBoundKey);
    }
}
