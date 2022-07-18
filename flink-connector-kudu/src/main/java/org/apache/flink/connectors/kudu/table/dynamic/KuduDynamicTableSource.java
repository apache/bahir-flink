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
package org.apache.flink.connectors.kudu.table.dynamic;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.connectors.kudu.connector.KuduFilterInfo;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.convertor.RowResultRowDataConvertor;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connectors.kudu.format.KuduRowDataInputFormat;
import org.apache.flink.connectors.kudu.table.function.lookup.KuduLookupOptions;
import org.apache.flink.connectors.kudu.table.function.lookup.KuduRowDataLookupFunction;
import org.apache.flink.connectors.kudu.table.utils.KuduTableUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.table.utils.TableSchemaUtils.containsPhysicalColumnsOnly;

/**
 * A {@link DynamicTableSource} for Kudu.
 */
public class KuduDynamicTableSource implements ScanTableSource, SupportsProjectionPushDown,
        SupportsLimitPushDown, LookupTableSource, SupportsFilterPushDown {

    private static final Logger LOG = LoggerFactory.getLogger(KuduDynamicTableSource.class);
    private final KuduTableInfo tableInfo;
    private final KuduLookupOptions kuduLookupOptions;
    private final KuduRowDataInputFormat kuduRowDataInputFormat;
    private final transient List<KuduFilterInfo> predicates = Lists.newArrayList();
    private KuduReaderConfig.Builder configBuilder;
    private TableSchema physicalSchema;
    private String[] projectedFields;
    private transient List<ResolvedExpression> filters;

    public KuduDynamicTableSource(KuduReaderConfig.Builder configBuilder, KuduTableInfo tableInfo,
                                  TableSchema physicalSchema, String[] projectedFields,
                                  KuduLookupOptions kuduLookupOptions) {
        this.configBuilder = configBuilder;
        this.tableInfo = tableInfo;
        this.physicalSchema = physicalSchema;
        this.projectedFields = projectedFields;
        this.kuduRowDataInputFormat = new KuduRowDataInputFormat(configBuilder.build(),
                new RowResultRowDataConvertor(), tableInfo,
                predicates,
                projectedFields == null ? null : Lists.newArrayList(projectedFields));
        this.kuduLookupOptions = kuduLookupOptions;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        int keysLen = context.getKeys().length;
        String[] keyNames = new String[keysLen];
        for (int i = 0; i < keyNames.length; ++i) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(innerKeyArr.length == 1, "Kudu only support non-nested look up keys");
            keyNames[i] = this.physicalSchema.getFieldNames()[innerKeyArr[0]];
        }
        KuduRowDataLookupFunction rowDataLookupFunction = KuduRowDataLookupFunction.Builder.options()
                .keyNames(keyNames)
                .kuduReaderConfig(configBuilder.build())
                .projectedFields(projectedFields)
                .tableInfo(tableInfo)
                .kuduLookupOptions(kuduLookupOptions)
                .build();
        return TableFunctionProvider.of(rowDataLookupFunction);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        if (CollectionUtils.isNotEmpty(this.filters)) {
            for (ResolvedExpression filter : this.filters) {
                Optional<KuduFilterInfo> kuduFilterInfo = KuduTableUtils.toKuduFilterInfo(filter);
                if (kuduFilterInfo != null && kuduFilterInfo.isPresent()) {
                    this.predicates.add(kuduFilterInfo.get());
                }

            }
        }
        KuduRowDataInputFormat inputFormat = new KuduRowDataInputFormat(configBuilder.build(),
                new RowResultRowDataConvertor(), tableInfo,
                this.predicates,
                projectedFields == null ? null : Lists.newArrayList(projectedFields));
        return InputFormatProvider.of(inputFormat);
    }

    @Override
    public DynamicTableSource copy() {
        return new KuduDynamicTableSource(this.configBuilder, this.tableInfo, this.physicalSchema,
                this.projectedFields, this.kuduLookupOptions);
    }

    @Override
    public String asSummaryString() {
        return "kudu";
    }

    @Override
    public boolean supportsNestedProjection() {
        //  planner doesn't support nested projection push down yet.
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        // parser projectFields
        this.physicalSchema = projectSchema(this.physicalSchema, projectedFields);
        this.projectedFields = physicalSchema.getFieldNames();
    }

    private TableSchema projectSchema(TableSchema tableSchema, int[][] projectedFields) {
        checkArgument(
                containsPhysicalColumnsOnly(tableSchema),
                "Projection is only supported for physical columns.");
        TableSchema.Builder builder = TableSchema.builder();

        FieldsDataType fields =
                (FieldsDataType)
                        DataTypeUtils.projectRow(tableSchema.toRowDataType(), projectedFields);
        RowType topFields = (RowType) fields.getLogicalType();
        for (int i = 0; i < topFields.getFieldCount(); i++) {
            builder.field(topFields.getFieldNames().get(i), fields.getChildren().get(i));
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KuduDynamicTableSource that = (KuduDynamicTableSource) o;
        return Objects.equals(configBuilder, that.configBuilder) && Objects.equals(tableInfo, that.tableInfo) && Objects.equals(physicalSchema, that.physicalSchema) && Arrays.equals(projectedFields, that.projectedFields) && Objects.equals(kuduLookupOptions, that.kuduLookupOptions) && Objects.equals(kuduRowDataInputFormat, that.kuduRowDataInputFormat) && Objects.equals(filters, that.filters) && Objects.equals(predicates, that.predicates);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(configBuilder, tableInfo, physicalSchema,
                kuduLookupOptions, kuduRowDataInputFormat, filters, predicates);
        result = 31 * result + Arrays.hashCode(projectedFields);
        return result;
    }

    @Override
    public void applyLimit(long limit) {
        this.configBuilder = this.configBuilder.setRowLimit((int) limit);
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        this.filters = filters;
        return Result.of(Collections.emptyList(), filters);
    }
}
