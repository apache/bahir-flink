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