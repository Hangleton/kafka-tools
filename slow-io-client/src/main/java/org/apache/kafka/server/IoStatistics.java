package org.apache.kafka.server;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.time.Instant;

import static java.nio.charset.Charset.defaultCharset;

public abstract class IoStatistics implements Serializable {
    public static final long serialVersionUID = 8785103947760534443L;

    private static final Logger log = LoggerFactory.getLogger(IoStatistics.class);

    protected int brokerId;
    protected long readsCompleted;
    protected long readTime;
    protected long writesCompleted;
    protected long writeTime;
    protected long queueTime;

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
        public ProducerRecord<Long, byte[]> toProducerRecord() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(this);
                oos.flush();
                return new ProducerRecord<>("__io_statistics", time.toEpochMilli(), baos.toByteArray());

            } catch (IOException e) {
                log.error("Error generating record from IoStatistics", e);
                return new ProducerRecord<>("__io_statistics", time.toEpochMilli(), "<ERROR>".getBytes(defaultCharset()));
            }
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

    public static IoStatistics fromRecord(byte[] record) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(record);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (IoStatistics) ois.readObject();

        } catch (IOException | ClassNotFoundException e) {
            log.error("Error reading the IoStatistics record", e);
            return null;
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

        @Override
        public ProducerRecord<Long, byte[]> toProducerRecord() {
            throw new IllegalStateException("Delta Statistics are not authorized to be converted to records.");
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

    public abstract ProducerRecord<Long, byte[]> toProducerRecord();

    public double readOpsLatency() {
        return (double) readTime / readsCompleted;
    }

    public double writeOpsLatency() {
        return (double) writeTime / writesCompleted;
    }
}
