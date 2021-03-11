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
package org.apache.flink.connectors.kudu.table;

import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.KuduTestBase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AND;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.EQUALS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit Tests for {@link KuduTableSource}.
 */
public class KuduTableSourceTest extends KuduTestBase {
    private KuduTableSource kuduTableSource;
    private KuduCatalog catalog;

    private static final ScalarFunction DUMMY_FUNCTION = new ScalarFunction() {
        // dummy
    };

    @BeforeEach
    public void init() {
        KuduTableInfo tableInfo = booksTableInfo("books", true);
        setUpDatabase(tableInfo);
        catalog = new KuduCatalog(harness.getMasterAddressesAsString());
        ObjectPath op = new ObjectPath(EnvironmentSettings.DEFAULT_BUILTIN_DATABASE, "books");
        try {
            kuduTableSource = catalog.getKuduTableFactory().createTableSource(op, catalog.getTable(op));
        } catch (TableNotExistException e) {
            fail(e.getMessage());
        }
    }

    @AfterEach
    public void clean() {
        KuduTableInfo tableInfo = booksTableInfo("books", true);
        cleanDatabase(tableInfo);
    }

    @Test
    void testGetTableSchema() throws Exception {
        TableSchema tableSchema = kuduTableSource.getTableSchema();
        assertNotNull(tableSchema);
        assertArrayEquals(getFieldNames(), tableSchema.getFieldNames());
        assertArrayEquals(getFieldDataTypes(), tableSchema.getFieldDataTypes());

    }

    @Test
    void testGetProducedDataType() throws Exception {
        DataType producedDataType = kuduTableSource.getProducedDataType();
        assertNotNull(producedDataType);
        assertEquals(getReturnDataType(getFieldNames(), getFieldDataTypes()).notNull(), producedDataType);
    }

    @Test
    void testProjectFields() throws Exception {
        KuduTableSource projectedTableSource = (KuduTableSource) kuduTableSource.projectFields(
            new int[]{3, 4, 1});
        // ensure copy is returned
        assertTrue(kuduTableSource != projectedTableSource);
        // ensure table schema is identical
        assertEquals(kuduTableSource.getTableSchema(), projectedTableSource.getTableSchema());
        // ensure IF is configured with selected fields
        String[] fieldNames = getFieldNames();
        DataType[] fieldDataTypes = getFieldDataTypes();
        String[] projectedFieldNames = new String[] {fieldNames[3], fieldNames[4], fieldNames[1]};
        DataType[] projectedDataTypes = new DataType[] {fieldDataTypes[3], fieldDataTypes[4],
            fieldDataTypes[1]};
        assertEquals(getReturnDataType(projectedFieldNames, projectedDataTypes),
            projectedTableSource.getProducedDataType());
    }

    @Test
    void testApplyPredicate() throws Exception {
        // expressions for supported predicates
        FieldReferenceExpression fieldReferenceExpression = new FieldReferenceExpression(
            "id", DataTypes.INT(), 0, 0);
        ValueLiteralExpression valueLiteralExpression = new ValueLiteralExpression(1);
        List<ResolvedExpression> args = new ArrayList<>(
            Arrays.asList(fieldReferenceExpression, valueLiteralExpression));
        Expression supportedPred = new CallExpression(
            EQUALS,
            args,
            DataTypes.BOOLEAN());
        // unsupported predicate
        Expression unsupportedPred = new CallExpression(
            new ScalarFunctionDefinition("dummy", DUMMY_FUNCTION),
            singletonList(new ValueLiteralExpression(1)),
            DataTypes.INT());
        // invalid predicate
        Expression invalidPred = new CallExpression(
            AND,
            Collections.emptyList(),
            DataTypes.ARRAY(DataTypes.INT()));

        ArrayList<Expression> preds = new ArrayList<>(
            Arrays.asList(supportedPred, unsupportedPred, invalidPred));
        // apply predicates on TableSource
        KuduTableSource filteredTableSource = (KuduTableSource) kuduTableSource.applyPredicate(preds);
        // ensure the unable push down expressions are reserved
        assertEquals(preds.size(), 2);
        assertSame(unsupportedPred, preds.get(0));
        assertSame(invalidPred, preds.get(1));
        // ensure copy is returned
        assertNotSame(kuduTableSource, filteredTableSource);
        // ensure table schema is identical
        assertEquals(kuduTableSource.getTableSchema(), filteredTableSource.getTableSchema());
        // ensure return type is identical
        assertEquals(kuduTableSource.getProducedDataType(), filteredTableSource.getProducedDataType());
        // ensure filter pushdown is correct
        assertTrue(filteredTableSource.isFilterPushedDown());
        assertFalse(kuduTableSource.isFilterPushedDown());
    }

    private String[] getFieldNames() {
        return new String[] {
            "id", "title", "author", "price", "quantity"
        };
    }

    private DataType[] getFieldDataTypes() {
        return new DataType[]{
            DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.INT(),
        };
    }

    private DataType getReturnDataType(String[] fieldNames, DataType[] dataTypes) {
        DataTypes.Field[] fields = new DataTypes.Field[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            fields[i] = DataTypes.FIELD(fieldNames[i], dataTypes[i]);
        }
        return DataTypes.ROW(fields);
    }
}
