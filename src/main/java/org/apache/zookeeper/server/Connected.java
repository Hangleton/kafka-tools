package org.apache.zookeeper.server;

public class Connected {
    private final long delayMs;

    public Connected(long delayMs) {
        this.delayMs = delayMs;
    }

    public void maybeInjectDelay() {
        System.out.println(">>>> CONNECTION DELAY = " + delayMs + " ms");
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
