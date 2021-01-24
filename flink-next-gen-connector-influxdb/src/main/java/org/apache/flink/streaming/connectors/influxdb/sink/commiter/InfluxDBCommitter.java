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
package org.apache.flink.streaming.connectors.influxdb.sink.commiter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.sink.Committer;

public class InfluxDBCommitter implements Committer<String>, Serializable {

    @Nullable private Queue<String> committedData;

    private boolean isClosed;

    @Nullable private final Supplier<Queue<String>> queueSupplier;

    public InfluxDBCommitter(@Nullable Supplier<Queue<String>> queueSupplier) {
        this.queueSupplier = queueSupplier;
        this.isClosed = false;
        this.committedData = null;
    }

    public List<String> getCommittedData() {
        if (committedData != null) {
            return new ArrayList<>(committedData);
        } else {
            return Collections.emptyList();
        }
    }

    // This method is called only when a checkpoint is set
    @Override
    public List<String> commit(List<String> committables) throws IOException {
        if (committedData == null) {
            committedData = queueSupplier.get();
        }
        committedData.addAll(committables);
        return Collections.emptyList();
    }

    public void close() throws Exception {
        isClosed = true;
    }

    public boolean isClosed() {
        return isClosed;
    }
}
