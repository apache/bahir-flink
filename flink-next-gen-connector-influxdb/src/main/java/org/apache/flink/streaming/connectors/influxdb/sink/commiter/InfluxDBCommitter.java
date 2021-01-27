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
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.connector.sink.Committer;

public class InfluxDBCommitter implements Committer<Void>, Serializable {

    // This method is called only when a checkpoint is set
    @Override
    public List<Void> commit(final List<Void> committables) throws IOException {
        // TODO: Write point to influxDB with currentTimestamp
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {}
}
