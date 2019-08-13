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
package org.apache.flink.connectors.kudu.connector.writer;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.kudu.client.SessionConfiguration.FlushMode;

@PublicEvolving
public class KuduWriterConfig implements Serializable {

    private final String masters;
    private final FlushMode flushMode;
    private final KuduWriterMode writeMode;

    private KuduWriterConfig(
            String masters,
            FlushMode flushMode,
            KuduWriterMode writeMode) {

        this.masters = checkNotNull(masters, "Kudu masters cannot be null");
        this.flushMode = checkNotNull(flushMode, "Kudu flush mode cannot be null");
        this.writeMode = checkNotNull(writeMode, "Kudu write mode cannot be null");
    }

    public String getMasters() {
        return masters;
    }

    public KuduWriterMode getWriteMode() {
        return writeMode;
    }

    public FlushMode getFlushMode() {
        return flushMode;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masters", masters)
                .append("flushMode", flushMode)
                .append("writeMode", writeMode)
                .toString();
    }

    /**
     * Builder for the {@link KuduWriterConfig}.
     */
    public static class Builder {
        private String masters;
        private KuduWriterMode writeMode = KuduWriterMode.UPSERT;
        private FlushMode flushMode = FlushMode.AUTO_FLUSH_BACKGROUND;

        private Builder(String masters) {
            this.masters = masters;
        }

        public static Builder setMasters(String masters) {
            return new Builder(masters);
        }

        public Builder setWriteMode(KuduWriterMode writeMode) {
            this.writeMode = writeMode;
            return this;
        }
        public Builder setUpsertWrite() {
            return setWriteMode(KuduWriterMode.UPSERT);
        }
        public Builder setInsertWrite() {
            return setWriteMode(KuduWriterMode.INSERT);
        }
        public Builder setUpdateWrite() {
            return setWriteMode(KuduWriterMode.UPDATE);
        }

        public Builder setConsistency(FlushMode flushMode) {
            this.flushMode = flushMode;
            return this;
        }
        public Builder setEventualConsistency() {
            return setConsistency(FlushMode.AUTO_FLUSH_BACKGROUND);
        }
        public Builder setStrongConsistency() {
            return setConsistency(FlushMode.AUTO_FLUSH_SYNC);
        }

        public KuduWriterConfig build() {
            return new KuduWriterConfig(
                    masters,
                    flushMode,
                    writeMode);
        }
    }
}
