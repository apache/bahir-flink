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
import org.apache.flink.connectors.kudu.connector.configuration.UserTableDataQueryDetail;

import java.io.Serializable;
import java.util.List;

/**
 * Kudu source connector options
 *
 * @param <T> The mapped Java type against the Kudu table.
 */
@PublicEvolving
public class KuduStreamingSourceConfiguration<T> implements Serializable {
    private static final long serialVersionUID = 529352279313155517L;

    private String masterAddresses;
    private String tableName;
    private List<UserTableDataQueryDetail> userTableDataQueryDetailList;
    private Long batchRunningInterval;
    private KuduStreamingRunningMode runningMode;
    private Class<T> targetKuduRowClz;

    KuduStreamingSourceConfiguration(String masterAddresses,
                                     String tableName,
                                     List<UserTableDataQueryDetail> userTableDataQueryDetailList,
                                     Long batchRunningInterval,
                                     KuduStreamingRunningMode runningMode,
                                     Class<T> targetKuduRowClz) {
        this.masterAddresses = masterAddresses;
        this.tableName = tableName;
        this.userTableDataQueryDetailList = userTableDataQueryDetailList;
        this.batchRunningInterval = batchRunningInterval;
        this.runningMode = runningMode;
        this.targetKuduRowClz = targetKuduRowClz;
    }

    public String getMasterAddresses() {
        return masterAddresses;
    }


    public String getTableName() {
        return tableName;
    }

    public List<UserTableDataQueryDetail> getUserTableDataQueryDetailList() {
        return userTableDataQueryDetailList;
    }

    public Long getBatchRunningInterval() {
        return batchRunningInterval;
    }

    public KuduStreamingRunningMode getRunningMode() {
        return runningMode;
    }

    public Class<T> getTargetKuduRowClz() {
        return targetKuduRowClz;
    }

    public static <T> Builder<T> builder() {
        return new Builder();
    }

    public static class Builder<T> {
        private String masterAddresses;
        private String tableName;
        private List<UserTableDataQueryDetail> userTableDataQueryDetailList;
        private Long batchRunningInterval;
        private KuduStreamingRunningMode runningMode;
        private Class<T> targetKuduRowClz;

        public Builder<T> masterAddresses(String masterAddresses) {
            this.masterAddresses = masterAddresses;
            return this;
        }

        public Builder<T> tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder<T> userTableDataQueryDetailList(List<UserTableDataQueryDetail> userTableDataQueryDetailList) {
            this.userTableDataQueryDetailList = userTableDataQueryDetailList;
            return this;
        }

        public Builder<T> batchRunningInterval(Long batchRunningInterval) {
            this.batchRunningInterval = batchRunningInterval;
            return this;
        }

        public Builder<T> runningMode(KuduStreamingRunningMode runningMode) {
            this.runningMode = runningMode;
            return this;
        }

        public Builder<T> targetKuduRowClz(Class<T> targetKuduRowClz) {
            this.targetKuduRowClz = targetKuduRowClz;
            return this;
        }

        public KuduStreamingSourceConfiguration<T> build() {
            return new KuduStreamingSourceConfiguration(this.masterAddresses,
                    this.tableName,
                    this.userTableDataQueryDetailList,
                    this.batchRunningInterval,
                    this.runningMode,
                    this.targetKuduRowClz);
        }

    }
}
