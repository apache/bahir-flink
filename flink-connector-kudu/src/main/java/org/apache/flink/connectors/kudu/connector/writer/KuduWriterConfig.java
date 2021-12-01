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
import org.apache.kudu.client.AsyncKuduClient;

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
    private final int flushIntervalMillis;
    private final int mutationBufferMaxOps;
    private final long timeoutMillis;

    private KuduWriterConfig(
            String masters,
            FlushMode flushMode,
            int flushIntervalMillis,
            int mutationBufferMaxOps,
            long timeoutMillis) {

        this.masters = checkNotNull(masters, "Kudu masters cannot be null");
        this.flushMode = checkNotNull(flushMode, "Kudu flush mode cannot be null");
        this.flushIntervalMillis = checkNotNull(flushIntervalMillis, "Kudu flushIntervalMillis cannot be null");
        this.mutationBufferMaxOps = checkNotNull(mutationBufferMaxOps, "Kudu mutationBufferMaxOps cannot be null");
        this.timeoutMillis = checkNotNull(timeoutMillis, "Kudu timeoutMillis cannot be null");
    }

    public String getMasters() {
        return masters;
    }

    public FlushMode getFlushMode() {
        return flushMode;
    }

    public int getFlushIntervalMillis() {
        return flushIntervalMillis;
    }

    public int getMutationBufferMaxOps() {
        return mutationBufferMaxOps;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masters", masters)
                .append("flushMode", flushMode)
                .append("flushIntervalMillis", flushIntervalMillis)
                .append("mutationBufferMaxOps", mutationBufferMaxOps)
                .append("timeoutMillis", timeoutMillis)
                .toString();
    }

    /**
     * Builder for the {@link KuduWriterConfig}.
     */
    public static class Builder {
        private String masters;
        private FlushMode flushMode = FlushMode.AUTO_FLUSH_BACKGROUND;
        private int flushIntervalMillis = 1000;
        private int mutationBufferMaxOps = 1000;
        private long timeoutMillis = AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS;

        private Builder(String masters) {
            this.masters = masters;
        }

        public static Builder newInstance(String masters) {
            return new Builder(masters);
        }

        public Builder setConsistency(FlushMode flushMode) {
            this.flushMode = flushMode;
            return this;
        }

        public Builder setFlushIntervalMillis(int intervalMillis) {
            this.flushIntervalMillis = intervalMillis;
            return this;
        }

        public Builder setMutationBufferMaxOps(int mutationBufferMaxOps) {
            this.mutationBufferMaxOps = mutationBufferMaxOps;
            return this;
        }

        public Builder setTimeoutMillis(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
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
                    flushIntervalMillis,
                    mutationBufferMaxOps,
                    timeoutMillis
            );
        }
    }
}
