package org.apache.flink.streaming.siddhi.control;

import java.io.Serializable;
import java.util.Date;

public interface ControlEvent extends Serializable {
    String DEFAULT_INTERNAL_CONTROL_STREAM = "_internal_control_stream";

    String getName();

    Date getCreatedTime();

    Date getExpiredTime();

    boolean isExpired();

    void expire();
}