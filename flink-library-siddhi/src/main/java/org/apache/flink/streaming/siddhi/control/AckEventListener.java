package org.apache.flink.streaming.siddhi.control;

import java.util.EventListener;

public interface AckEventListener extends EventListener {
    void onEventAcked(ControlEvent event);
}