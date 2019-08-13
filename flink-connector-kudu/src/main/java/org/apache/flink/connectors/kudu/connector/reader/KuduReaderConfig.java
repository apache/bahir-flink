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
package org.apache.flink.connectors.kudu.connector.reader;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

@PublicEvolving
public class KuduReaderConfig implements Serializable {

    private final String masters;
    private final Integer rowLimit;

    private KuduReaderConfig(
            String masters,
            Integer rowLimit) {

        this.masters = checkNotNull(masters, "Kudu masters cannot be null");
        this.rowLimit = checkNotNull(rowLimit, "Kudu rowLimit cannot be null");;
    }

    public String getMasters() {
        return masters;
    }

    public Integer getRowLimit() {
        return rowLimit;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masters", masters)
                .append("rowLimit", rowLimit)
                .toString();
    }

    /**
     * Builder for the {@link KuduReaderConfig}.
     */
    public static class Builder {
        private String masters;
        private Integer rowLimit = 0;

        private Builder(String masters) {
            this.masters = masters;
        }

        public static Builder setMasters(String masters) {
            return new Builder(masters);
        }

        public Builder setRowLimit(Integer rowLimit) {
            this.rowLimit = rowLimit;
            return this;
        }

        public KuduReaderConfig build() {
            return new KuduReaderConfig(
                    masters,
                    rowLimit);
        }
    }
}
