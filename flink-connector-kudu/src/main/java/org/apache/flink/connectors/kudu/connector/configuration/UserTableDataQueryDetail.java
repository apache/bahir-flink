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
package org.apache.flink.connectors.kudu.connector.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.kudu.connector.configuration.type.UserTableDataQueryFilter;
import org.apache.kudu.shaded.com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

@Internal
public class UserTableDataQueryDetail implements Serializable {
    private static final long serialVersionUID = -5260450483244281444L;
    private List<String> projectedColumns = Lists.newArrayList();
    private List<UserTableDataQueryFilter> userTableDataQueryFilters = Lists.newArrayList();

    /**
     * Lower and upper bound keys for query in the incremental mode, the query range will be (lowerBoundKey, upperBoundKey).
     * If lowerBoundKey or upperBoundKey is null, the value will be replaced with -inf and +inf.
     */
    private String lowerBoundKey;
    private String upperBoundKey;

    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    public void setProjectedColumns(List<String> projectedColumns) {
        this.projectedColumns = projectedColumns;
    }

    public List<UserTableDataQueryFilter> getUserTableDataQueryFilters() {
        return userTableDataQueryFilters;
    }

    public void setUserTableDataQueryFilters(List<UserTableDataQueryFilter> userTableDataQueryFilters) {
        this.userTableDataQueryFilters = userTableDataQueryFilters;
    }

    public String getLowerBoundKey() {
        return lowerBoundKey;
    }

    public void setLowerBoundKey(String lowerBoundKey) {
        this.lowerBoundKey = lowerBoundKey;
    }

    public String getUpperBoundKey() {
        return upperBoundKey;
    }

    public void setUpperBoundKey(String upperBoundKey) {
        this.upperBoundKey = upperBoundKey;
    }
}
