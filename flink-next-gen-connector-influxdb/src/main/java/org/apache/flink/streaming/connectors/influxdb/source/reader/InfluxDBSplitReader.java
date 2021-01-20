/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.influxdb.source.reader;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.streaming.connectors.influxdb.source.split.InfluxDBSplit;

/**
 * A {@link SplitReader} implementation that reads records from InfluxDB splits.
 *
 * <p>The returned type are in the format of {@code tuple2(record and timestamp}.
 *
 * @param <T> the type of the record to be emitted from the Source.
 */
public class InfluxDBSplitReader<T> implements SplitReader<Tuple2<T, Long>, InfluxDBSplit> {

    @Override
    public RecordsWithSplitIds<Tuple2<T, Long>> fetch() throws IOException {
        final InfluxDBSplitRecords<Tuple2<T, Long>> recordsBySplits = new InfluxDBSplitRecords<>();
        final Collection<Tuple2<T, Long>> recordsForSplit = recordsBySplits.recordsForSplit("0");
        recordsForSplit.add(new Tuple2(1L, 1L));
        recordsForSplit.add(new Tuple2(2L, 2L));
        recordsForSplit.add(new Tuple2(3L, 3L));
        recordsBySplits.prepareForRead();
        recordsBySplits.addFinishedSplit("0");
        return recordsBySplits;
    }

    @Override
    public void handleSplitsChanges(final SplitsChange<InfluxDBSplit> splitsChange) {}

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {}

    // ---------------- private helper class ------------------------

    private static class InfluxDBSplitRecords<T> implements RecordsWithSplitIds<T> {
        private final Map<String, Collection<T>> recordsBySplits;
        private final Set<String> finishedSplits;
        private Iterator<Map.Entry<String, Collection<T>>> splitIterator;
        private String currentSplitId;
        private Iterator<T> recordIterator;

        private InfluxDBSplitRecords() {
            this.recordsBySplits = new HashMap<>();
            this.finishedSplits = new HashSet<>();
        }

        private Collection<T> recordsForSplit(final String splitId) {
            return this.recordsBySplits.computeIfAbsent(splitId, id -> new ArrayList<>());
        }

        private void addFinishedSplit(final String splitId) {
            this.finishedSplits.add(splitId);
        }

        private void prepareForRead() {
            this.splitIterator = this.recordsBySplits.entrySet().iterator();
        }

        @Override
        @Nullable
        public String nextSplit() {
            if (this.splitIterator.hasNext()) {
                final Map.Entry<String, Collection<T>> entry = this.splitIterator.next();
                this.currentSplitId = entry.getKey();
                this.recordIterator = entry.getValue().iterator();
                return this.currentSplitId;
            } else {
                this.currentSplitId = null;
                this.recordIterator = null;
                return null;
            }
        }

        @Override
        @Nullable
        public T nextRecordFromSplit() {
            checkNotNull(
                    this.currentSplitId,
                    "Make sure nextSplit() did not return null before "
                            + "iterate over the records split.");
            if (this.recordIterator.hasNext()) {
                return this.recordIterator.next();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return this.finishedSplits;
        }
    }
}
