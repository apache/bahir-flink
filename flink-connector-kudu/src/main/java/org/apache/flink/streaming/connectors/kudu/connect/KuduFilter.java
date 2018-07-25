package org.apache.flink.streaming.connectors.kudu.connect;

import java.util.List;
import org.apache.commons.lang3.ClassUtils;
import org.apache.kudu.client.KuduPredicate;


public class KuduFilter {

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


    private String column;
    private FilterType filter;
    private Object value;

    protected KuduFilter(String column,FilterType filter,Object value){
        this.column = column;
        this.filter = filter;
        this.value = value;
    }

    public String getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }

    public Long getLongValue() {
        if (ClassUtils.isAssignable(value.getClass(), Number.class)) {
            return ((Number)value).longValue();
        } else {
            return null;
        }
    }

    public FilterType getFilter() {
        return filter;
    }

    public static class Builder {
        private String column;
        private FilterType filter;
        private Object value;


        private Builder(String column) {
            this.column = column;
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

        public Builder isIn(List values) {
            return filter(FilterType.IS_IN, values);
        }

        public Builder filter(FilterType type, Object value) {
            this.filter = type;
            this.value = value;
            return this;
        }

        public KuduFilter build() {
            return new KuduFilter(this.column, this.filter, this.value);
        }
    }

}
