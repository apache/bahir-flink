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
import org.apache.flink.connectors.kudu.connector.query.UserTableDataQueryFilterValueResolver;

import java.io.Serializable;

@Internal
public class UserTableDataQueryFilter implements Serializable {
    private static final long serialVersionUID = -8244400106392939137L;

    private String colName;
    private FilterOp filterOp;
    private UserTableDataQueryFilterValueResolver filterValueResolver;

    public UserTableDataQueryFilter(String colName, FilterOp filterOp, UserTableDataQueryFilterValueResolver filterValueResolver) {
        this.colName = colName;
        this.filterOp = filterOp;
        this.filterValueResolver = filterValueResolver;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public FilterOp getFilterOp() {
        return filterOp;
    }

    public void setFilterOp(FilterOp filterOp) {
        this.filterOp = filterOp;
    }

    public UserTableDataQueryFilterValueResolver getFilterValueResolver() {
        return filterValueResolver;
    }

    public void setFilterValueResolver(UserTableDataQueryFilterValueResolver filterValueResolver) {
        this.filterValueResolver = filterValueResolver;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String colName;
        private FilterOp filterOp;
        private UserTableDataQueryFilterValueResolver filterValueResolver;

        public Builder colName(String colName) {
            this.colName = colName;
            return this;
        }

        public Builder filterOp(FilterOp filterOp) {
            this.filterOp = filterOp;
            return this;
        }

        public Builder filterValueResolver(UserTableDataQueryFilterValueResolver filterValueResolver) {
            this.filterValueResolver = filterValueResolver;
            return this;
        }

        public UserTableDataQueryFilter build() {
            return new UserTableDataQueryFilter(colName, filterOp, filterValueResolver);
        }
    }
}
