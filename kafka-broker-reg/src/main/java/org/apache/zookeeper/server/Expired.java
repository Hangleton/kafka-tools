package org.apache.zookeeper.server;

public class Expired {
    private final long delayMs;

    public Expired(long delayMs) {
        this.delayMs = delayMs;
    }

    public void maybeInjectDelay() {
        System.out.println(">>>> SESSION EXPIRATION DELAY = " + delayMs + " ms");
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
