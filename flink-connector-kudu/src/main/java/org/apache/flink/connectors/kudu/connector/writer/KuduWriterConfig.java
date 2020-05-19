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

import org.apache.flink.annotation.PublicEvolving;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.kudu.client.SessionConfiguration.FlushMode;

/**
 * Configuration used by {@link org.apache.flink.connectors.kudu.streaming.KuduSink} and {@link org.apache.flink.connectors.kudu.batch.KuduOutputFormat}.
 * Specifies connection and other necessary properties.
 */
@PublicEvolving
public class KuduWriterConfig implements Serializable {

    private final String masters;
    private final FlushMode flushMode;

    private KuduWriterConfig(
            String masters,
            FlushMode flushMode) {

        this.masters = checkNotNull(masters, "Kudu masters cannot be null");
        this.flushMode = checkNotNull(flushMode, "Kudu flush mode cannot be null");
    }

    public String getMasters() {
        return masters;
    }

    public FlushMode getFlushMode() {
        return flushMode;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masters", masters)
                .append("flushMode", flushMode)
                .toString();
    }

    /**
     * Builder for the {@link KuduWriterConfig}.
     */
    public static class Builder {
        private String masters;
        private FlushMode flushMode = FlushMode.AUTO_FLUSH_BACKGROUND;

        private Builder(String masters) {
            this.masters = masters;
        }

        public static Builder setMasters(String masters) {
            return new Builder(masters);
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
                    flushMode);
        }
    }
}
