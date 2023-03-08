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

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connectors.kudu.connector.KuduTestBase;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.TupleOperationMapper;
import org.apache.kudu.client.Operation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

public class TupleOpertaionMapperTest extends AbstractOperationTest {
    @Test
    void testGetField() {
        TupleOperationMapper<Tuple5<Integer, String, String, Double, Integer>> mapper =
                new TupleOperationMapper<>(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT);
        Tuple5<Integer, String, String, Double, Integer> inputTuple = KuduTestBase.booksDataTuple().get(0);

        for (int i = 0; i < inputTuple.getArity(); i++) {
            Assertions.assertEquals(inputTuple.getField(i), mapper.getField(inputTuple, i));
        }
    }

    @Test
    void testCorrectOperationInsert() {
        TupleOperationMapper<Tuple5<Integer, String, String, Double, Integer>> mapper =
                new TupleOperationMapper<>(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT);
        Tuple5<Integer, String, String, Double, Integer> inputTuple = KuduTestBase.booksDataTuple().get(0);

        List<Operation> operations = mapper.createOperations(inputTuple, mockTable);

        assertEquals(1, operations.size());
        verify(mockTable).newInsert();
    }

    @Test
    void testCorrectOperationUpsert() {
        TupleOperationMapper<Tuple5<Integer, String, String, Double, Integer>> mapper =
                new TupleOperationMapper<>(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.UPSERT);
        Tuple5<Integer, String, String, Double, Integer> inputTuple = KuduTestBase.booksDataTuple().get(0);

        List<Operation> operations = mapper.createOperations(inputTuple, mockTable);

        assertEquals(1, operations.size());
        verify(mockTable).newUpsert();
    }
}
