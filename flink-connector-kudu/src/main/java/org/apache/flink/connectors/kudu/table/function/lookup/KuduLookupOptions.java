/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.connectors.kudu.table.function.lookup;

/**
 * Options for the Kudu lookup.
 */
public class KuduLookupOptions {
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    public static Builder builder() {
        return new Builder();
    }

    public KuduLookupOptions(long cacheMaxSize, long cacheExpireMs, int maxRetryTimes) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }


    public long getCacheExpireMs() {
        return cacheExpireMs;
    }


    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }


    public static final class Builder {
        private long cacheMaxSize;
        private long cacheExpireMs;
        private int maxRetryTimes;

        public static Builder options() {
            return new Builder();
        }

        public Builder withCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public Builder withCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }

        public Builder withMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public KuduLookupOptions build() {
            return new KuduLookupOptions(cacheMaxSize, cacheExpireMs, maxRetryTimes);
        }
    }
}
