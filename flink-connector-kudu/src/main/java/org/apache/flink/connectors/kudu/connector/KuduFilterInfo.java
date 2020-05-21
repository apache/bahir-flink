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

import org.apache.flink.annotation.PublicEvolving;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;

import java.io.Serializable;
import java.util.List;

@PublicEvolving
public class KuduFilterInfo implements Serializable {

    private String column;
    private FilterType type;
    private Object value;

    private KuduFilterInfo() { }

    public KuduPredicate toPredicate(Schema schema) {
        return toPredicate(schema.getColumn(this.column));
    }

    public KuduPredicate toPredicate(ColumnSchema column) {
        KuduPredicate predicate;
        switch (this.type) {
            case IS_IN:
                predicate = KuduPredicate.newInListPredicate(column, (List<?>) this.value);
                break;
            case IS_NULL:
                predicate = KuduPredicate.newIsNullPredicate(column);
                break;
            case IS_NOT_NULL:
                predicate = KuduPredicate.newIsNotNullPredicate(column);
                break;
            default:
                predicate = predicateComparator(column);
                break;
        }
        return predicate;
    }

    private KuduPredicate predicateComparator(ColumnSchema column) {

        KuduPredicate.ComparisonOp comparison = this.type.comparator;

        KuduPredicate predicate;

        switch (column.getType()) {
            case STRING:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (String) this.value);
                break;
            case FLOAT:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (float) this.value);
                break;
            case INT8:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (byte) this.value);
                break;
            case INT16:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (short) this.value);
                break;
            case INT32:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (int) this.value);
                break;
            case INT64:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (long) this.value);
                break;
            case DOUBLE:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (double) this.value);
                break;
            case BOOL:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (boolean) this.value);
                break;
            case UNIXTIME_MICROS:
                Long time = (Long) this.value;
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, time * 1000);
                break;
            case BINARY:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (byte[]) this.value);
                break;
            default:
                throw new IllegalArgumentException("Illegal var type: " + column.getType());
        }
        return predicate;
    }

    public enum FilterType {
        GREATER(KuduPredicate.ComparisonOp.GREATER),
        GREATER_EQUAL(KuduPredicate.ComparisonOp.GREATER_EQUAL),
        EQUAL(KuduPredicate.ComparisonOp.EQUAL),
        LESS(KuduPredicate.ComparisonOp.LESS),
        LESS_EQUAL(KuduPredicate.ComparisonOp.LESS_EQUAL),
        IS_NOT_NULL(null),
        IS_NULL(null),
        IS_IN(null);

        final KuduPredicate.ComparisonOp comparator;

        FilterType(KuduPredicate.ComparisonOp comparator) {
            this.comparator = comparator;
        }

    }

    public static class Builder {
        private KuduFilterInfo filter;

        private Builder(String column) {
            this.filter = new KuduFilterInfo();
            this.filter.column = column;
        }

        public static Builder create(String column) {
            return new Builder(column);
        }

        public Builder greaterThan(Object value) {
            return filter(FilterType.GREATER, value);
        }

        public Builder lessThan(Object value) {
            return filter(FilterType.LESS, value);
        }

        public Builder equalTo(Object value) {
            return filter(FilterType.EQUAL, value);
        }

        public Builder greaterOrEqualTo(Object value) {
            return filter(FilterType.GREATER_EQUAL, value);
        }

        public Builder lessOrEqualTo(Object value) {
            return filter(FilterType.LESS_EQUAL, value);
        }

        public Builder isNotNull() {
            return filter(FilterType.IS_NOT_NULL, null);
        }

        public Builder isNull() {
            return filter(FilterType.IS_NULL, null);
        }

        public Builder isIn(List<?> values) {
            return filter(FilterType.IS_IN, values);
        }

        public Builder filter(FilterType type, Object value) {
            this.filter.type = type;
            this.filter.value = value;
            return this;
        }

        public KuduFilterInfo build() {
            return filter;
        }
    }

}
