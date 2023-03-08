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

package org.apache.flink.connectors.kudu.writer;

import org.apache.flink.connectors.kudu.connector.KuduTestBase;
import org.apache.flink.connectors.kudu.connector.KuduTestBase.BookInfo;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.PojoOperationMapper;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class PojoOperationMapperTest extends AbstractOperationTest {

    @Test
    void testPojoMapper() {

        PojoOperationMapper<BookInfo> mapper = new PojoOperationMapper<>(BookInfo.class, KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT);

        BookInfo bookInfo = KuduTestBase.booksDataPojo().get(0);

        assertEquals(bookInfo.id, mapper.getField(bookInfo, 0));
        assertEquals(bookInfo.title, mapper.getField(bookInfo, 1));
        assertEquals(bookInfo.author, mapper.getField(bookInfo, 2));
        assertEquals(bookInfo.price, mapper.getField(bookInfo, 3));
        assertEquals(bookInfo.quantity, mapper.getField(bookInfo, 4));

        List<Operation> operations = mapper.createOperations(bookInfo, mockTable);
        assertEquals(1, operations.size());

        PartialRow row = operations.get(0).getRow();
        Mockito.verify(row, Mockito.times(1)).addObject("id", bookInfo.id);
        Mockito.verify(row, Mockito.times(1)).addObject("quantity", bookInfo.quantity);

        Mockito.verify(row, Mockito.times(1)).addObject("title", bookInfo.title);
        Mockito.verify(row, Mockito.times(1)).addObject("author", bookInfo.author);

        Mockito.verify(row, Mockito.times(1)).addObject("price", bookInfo.price);
    }

    @Test
    public void testFieldInheritance() {
        PojoOperationMapper<Second> mapper = new PojoOperationMapper<>(Second.class, new String[]{"s1", "i1", "i2"}, AbstractSingleOperationMapper.KuduOperation.INSERT);
        Second s = new Second();
        assertEquals("s1", mapper.getField(s, 0));
        assertEquals(1, mapper.getField(s, 1));
        assertEquals(2, mapper.getField(s, 2));
    }

    private static class First {
        private int i1 = 1;
        public int i2 = 2;
        private String s1 = "ignore";
    }

    private static class Second extends First {
        private String s1 = "s1";
    }
}
