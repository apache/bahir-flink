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
package org.apache.flink.connectors.kudu.connector.assigner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.kudu.connector.KuduDataSplit;
import org.apache.flink.connectors.kudu.connector.KuduStreamingRunningMode;

@Internal
public class KuduDataSplitsAssigner {
    /**
     * Assign the data split to the flink TM subTask. The data splits within the same tablet will be handled
     * by the same subTask.
     *
     * @param dataSplit The allocated data split for the specific subtask of kudu source connector
     * @param numParallelSubtasks Total number of subtasks for kudu source connector
     * @param runningMode Running mode of the current kudu source connector
     * @return the calculated value which will be compared with the subTask index, {@link KuduStreamingRunningMode}
     */
    public static int assign(KuduDataSplit dataSplit, int numParallelSubtasks, KuduStreamingRunningMode runningMode) {
        if (runningMode == KuduStreamingRunningMode.INCREMENTAL) {
            return 0; // Always pick up the first subtask to handle
        } else {
            return ((dataSplit.getTabletId().hashCode() * 31) & 0x7FFFFFFF) % numParallelSubtasks;
        }
    }
}
