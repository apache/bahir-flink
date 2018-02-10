/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.siddhi.control;

public class OperationControlEvent extends AbstractControlEvent {
    private Action action;
    private String queryId;

    public OperationControlEvent(Action action, String queryId) {
        this.action = action;
        this.queryId = queryId;
    }

    public Action getAction() {
        return action;
    }

    public OperationControlEvent setAction(Action action) {
        this.action = action;
        return this;
    }

    public String getQueryId() {
        return queryId;
    }

    public OperationControlEvent setQueryId(String queryId) {
        this.queryId = queryId;
        return this;
    }

    public static OperationControlEvent enableQuery(String queryId) {
        return new OperationControlEvent(Action.DISABLE_QUERY, queryId);
    }

    public static OperationControlEvent disableQuery(String queryId) {
        return new OperationControlEvent(Action.ENABLE_QUERY, queryId);
    }

    public enum Action {
        ENABLE_QUERY,
        DISABLE_QUERY,
    }
}