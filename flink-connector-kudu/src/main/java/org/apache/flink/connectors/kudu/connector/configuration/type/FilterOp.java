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
package org.apache.flink.connectors.kudu.connector.configuration.type;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.kudu.connector.KuduFilterInfo;

@Internal
public enum FilterOp {

    GREATER(KuduFilterInfo.FilterType.GREATER),
    GREATER_EQUAL(KuduFilterInfo.FilterType.GREATER_EQUAL),
    EQUAL(KuduFilterInfo.FilterType.EQUAL),
    LESS(KuduFilterInfo.FilterType.LESS),
    LESS_EQUAL(KuduFilterInfo.FilterType.LESS_EQUAL),
    ;

    private KuduFilterInfo.FilterType kuduFilterType;

    FilterOp(KuduFilterInfo.FilterType kuduFilterType) {
        this.kuduFilterType = kuduFilterType;
    }

    public KuduFilterInfo.FilterType getKuduFilterType() {
        return kuduFilterType;
    }
}
