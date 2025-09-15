package com.sincera.npam.config;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class PipelineMetrics {
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong pushed = new AtomicLong();

    public void incrementConsumed(long count) {
        consumed.addAndGet(count);
    }

    public void incrementPushed(long count) {
        pushed.addAndGet(count);
    }

    public long getConsumed() {
        return consumed.get();
    }

    public long getPushed() {
        return pushed.get();
    }
}
