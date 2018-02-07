package org.apache.flink.streaming.siddhi.event;

import java.util.EventListener;

public interface InternalEventListener extends EventListener {
    void onInternalEvent(InternalEvent event);
}
