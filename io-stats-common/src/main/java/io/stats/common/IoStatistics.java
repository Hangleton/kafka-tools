package io.stats.common;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

public abstract class IoStatistics implements Serializable {
    public static final long serialVersionUID = 8785103947760534443L;

    protected int brokerId;
    protected long readsCompleted;
    protected long readTime;
    protected long writesCompleted;
    protected long writeTime;
    protected long queueTime;

    public static IoStatistics newIoStatistics(int brokerId, Instant time, String stat) {
        String[] stats = Arrays.stream(stat.split("\\D+"))
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);

        // https://www.kernel.org/doc/Documentation/block/stat.txt
        long readsCompleted = Long.parseLong(stats[0]);
        long readTime = Long.parseLong(stats[3]);
        long writesCompleted = Long.parseLong(stats[4]);
        long writeTime = Long.parseLong(stats[7]);
        long queueTime = Long.parseLong(stats[10]);

        return new Snapshot(brokerId, time, readsCompleted, readTime, writesCompleted, writeTime, queueTime);
    }

    public static final class Snapshot extends IoStatistics implements Serializable {
        public static final long serialVersionUID = 5411787353326436545L;

        private Instant time;

        public Snapshot() {
        }

        private Snapshot(int brokerId, Instant time, long readsCompleted, long readTime, long writesCompleted, long writeTime, long queueTime) {
            super(brokerId, readsCompleted, readTime, writesCompleted, writeTime, queueTime);
            this.time = time;
        }

        @Override
        public IoStatistics delta(IoStatistics origin) {
            if (!(origin instanceof Snapshot)) {
                throw new IllegalStateException("IoStatistics is not a snapshot, cannot compute delta.");
            }
            return new Delta(origin.brokerId, (Snapshot) origin, this);
        }

        @Override
        public double ioQueueSize() {
            throw new IllegalStateException("I/O queue size cannot be defined for an IoStatistics snapshot.");
        }

        @Override
        public String toString() {
            return "Broker " + brokerId + " " + time + " " + readsCompleted + " " + readTime + " " + writesCompleted + " " + writeTime;
        }

        public Instant time() {
            return time;
        }

        public long readsCompleted() {
            return readsCompleted;
        }

        public long readTime() {
            return readTime;
        }

        public long writesCompleted() {
            return writesCompleted;
        }

        public long writeTime() {
            return writeTime;
        }

        public long queueTime() {
            return queueTime;
        }
    }

    private static final class Delta extends IoStatistics {
        private final Duration timeSpan;

        private Delta(int brokerId, Snapshot origin, Snapshot stat) {
            super(
                brokerId,
                stat.readsCompleted - origin.readsCompleted,
                stat.readTime - origin.readTime,
                stat.writesCompleted - origin.writesCompleted,
                stat.writeTime - origin.writeTime,
                stat.queueTime - origin.queueTime
            );

            this.timeSpan = Duration.between(origin.time, stat.time);
        }

        @Override
        public IoStatistics delta(IoStatistics origin) {
            throw new IllegalStateException("IoStatistics is not a snapshot, cannot compute delta.");
        }

        @Override
        public double ioQueueSize() {
            return (double) queueTime / timeSpan.toMillis();
        }

        public String toString() {
            return "avg-read-latency: " + readOpsLatency() + " avg-write-latency: " + writeOpsLatency() + " avg-io-queue-size: " + ioQueueSize();
        }
    }

    protected IoStatistics(int brokerId, long readsCompleted, long readTime, long writesCompleted, long writeTime, long queueTime) {
        this.brokerId = brokerId;
        this.readsCompleted = readsCompleted;
        this.readTime = readTime;
        this.writesCompleted = writesCompleted;
        this.writeTime = writeTime;
        this.queueTime = queueTime;
    }

    protected IoStatistics() {
    }

    public abstract IoStatistics delta(IoStatistics origin);

    public abstract double ioQueueSize();

    public double readOpsLatency() {
        return (double) readTime / readsCompleted;
    }

    public double writeOpsLatency() {
        return (double) writeTime / writesCompleted;
    }

    public int brokerId() {
        return brokerId;
    }
}
