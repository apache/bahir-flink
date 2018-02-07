package org.apache.flink.streaming.siddhi.event;

public class InternalEvent {

    public static final String DEFAULT_INTERNAL_EVENT_STREAM = "default_internal_event_stream";

    private EventType type;
    private Object payload;

    public InternalEvent(EventType type, Object payload) {
        this.type = type;
        this.payload = payload;
    }

    public EventType getType() {
        return type;
    }

    public InternalEvent setType(EventType type) {
        this.type = type;
        return this;
    }

    public <T> T getPayload(Class<T> clazz) {
        return (T) payload;
    }

    public InternalEvent setPayload(Object payload) {
        this.payload = payload;
        return this;
    }

    public enum EventType {
        ExecutionPlanAdded,
        ExecutionPlanUpdated,
        ExecutionPlanDeleted
    }

    public static InternalEvent of(EventType type, Object payload) {
        return new InternalEvent(type, payload);
    }
}