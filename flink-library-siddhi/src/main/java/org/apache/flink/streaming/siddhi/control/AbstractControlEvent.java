package org.apache.flink.streaming.siddhi.control;

import java.util.Date;

public abstract class AbstractControlEvent implements ControlEvent {
    private final Date createdTime = new Date();
    private Date expiredTime;
    private boolean expired;

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public Date getCreatedTime() {
        return createdTime;
    }

    @Override
    public Date getExpiredTime() {
        return expiredTime;
    }

    @Override
    public boolean isExpired() {
        return expired;
    }

    @Override
    public void expire() {
        this.expired = true;
        this.expiredTime = new Date();
    }
}