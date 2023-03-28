package org.apache.zookeeper.server;

import java.util.Iterator;

public class TestContext {
    public final Iterator<Received> requestTimelime;
    public final Iterator<Connected> connectionTimeline;
    public final Iterator<Expired> sessionExpirationTimeline;
    private boolean terminated;

    public TestContext(
        Iterator<Received> requestTimelime,
        Iterator<Connected> connectionTimeline,
        Iterator<Expired> sessionExpirationTimeline
    ) {
        this.requestTimelime = requestTimelime;
        this.connectionTimeline = connectionTimeline;
        this.sessionExpirationTimeline = sessionExpirationTimeline;
    }

    public synchronized boolean isTerminated() {
        return terminated;
    }

    public synchronized void terminate() {
        terminated = true;
    }
}
