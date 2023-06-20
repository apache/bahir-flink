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
package org.apache.flink.connectors.kudu.connector.discover;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.kudu.connector.KuduDataSplit;
import org.apache.flink.connectors.kudu.connector.KuduFilterInfo;
import org.apache.flink.connectors.kudu.connector.reader.KuduReader;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.shaded.com.google.common.collect.Lists;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Internal
public class KuduDataSplitsDiscoverer {
    private KuduReader reader;
    private List<KuduFilterInfo> filterInfoList;
    private List<String> projectedColumnList;

    public KuduDataSplitsDiscoverer(KuduReader reader, List<KuduFilterInfo> filterInfoList, List<String> projectedColumnList) {
        this.reader = reader;
        this.filterInfoList = filterInfoList;
        this.projectedColumnList = projectedColumnList;
    }

    /**
     * Get all the data splits against the filterInfoList
     *
     * @return all the data splits against the filterInfoList
     * @throws Exception
     */
    public List<KuduDataSplit> getAllKuduDataSplits() throws Exception {
        List<KuduScanToken> kuduScanTokenList = reader.scanTokens(filterInfoList, projectedColumnList, null);

        List<KuduDataSplit> splits = Lists.newArrayList();
        for (KuduScanToken kuduScanToken : kuduScanTokenList) {
            KuduDataSplit split = new KuduDataSplit();
            split.setScanToken(kuduScanToken.serialize());
            split.setTabletId(new String(kuduScanToken.getTablet().getTabletId(), StandardCharsets.UTF_8));

            splits.add(split);
        }
        return splits;
    }

    public static KuduDataSplitsDiscoverer.Builder builder() {
        return new KuduDataSplitsDiscoverer.Builder();
    }

    public static class Builder {
        private KuduReader reader;
        private List<KuduFilterInfo> filterInfoList;
        private List<String> projectedColumnList;

        public Builder reader(KuduReader reader) {
            this.reader = reader;
            return this;
        }

        public Builder filterInfoList(List<KuduFilterInfo> filterInfoList) {
            this.filterInfoList = filterInfoList;
            return this;
        }

        public Builder projectedColumnList(List<String> projectedColumnList) {
            this.projectedColumnList = projectedColumnList;
            return this;
        }

        public KuduDataSplitsDiscoverer build() {
            return new KuduDataSplitsDiscoverer(reader, filterInfoList, projectedColumnList);
        }
    }
}
