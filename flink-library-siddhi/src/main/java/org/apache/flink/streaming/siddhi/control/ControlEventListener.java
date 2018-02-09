package org.apache.flink.streaming.siddhi.control;

import java.util.EventListener;

public interface ControlEventListener extends EventListener {
    void onEventReceived(ControlEvent event);
}