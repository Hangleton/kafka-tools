package org.apache.kafka.server;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import static java.nio.charset.Charset.defaultCharset;

public abstract class IoStatistics implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(IoStatistics.class);

    public static IoStatistics newIoStatistics(Instant time, String stat) {
        String[] stats = Arrays.stream(stat.split("\\D+"))
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);

        // https://www.kernel.org/doc/Documentation/block/stat.txt
        long readsCompleted = Long.parseLong(stats[0]);
        long readTime = Long.parseLong(stats[3]);
        long writesCompleted = Long.parseLong(stats[4]);
        long writeTime = Long.parseLong(stats[7]);
        long queueTime = Long.parseLong(stats[10]);

        return new Snapshot(time, readsCompleted, readTime, writesCompleted, writeTime, queueTime);
    }

    protected long readsCompleted;
    protected long readTime;
    protected long writesCompleted;
    protected long writeTime;
    protected long queueTime;

    private static final class Snapshot extends IoStatistics implements Serializable {
        public static final long serialVersionUID = 5411787353326436545L;

        private Instant time;

        public Snapshot() {
        }

        private Snapshot(Instant time, long readsCompleted, long readTime, long writesCompleted, long writeTime, long queueTime) {
            super(readsCompleted, readTime, writesCompleted, writeTime, queueTime);
            this.time = time;
        }

        @Override
        public IoStatistics delta(IoStatistics origin) {
            if (!(origin instanceof Snapshot)) {
                throw new IllegalStateException("IoStatistics is not a snapshot, cannot compute delta.");
            }
            return new Delta((Snapshot) origin, this);
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
            return time + " " + readsCompleted + " " + readTime + " " + writesCompleted + " " + writeTime;
        }
    }

    public static IoStatistics fromRecord(ConsumerRecord<Long, byte[]> record) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(record.value());
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (IoStatistics) ois.readObject();

        } catch (IOException | ClassNotFoundException e) {
            log.error("Error reading the IoStatistics record", e);
            return null;
        }
    }

    private static final class Delta extends IoStatistics {
        private final Duration timeSpan;

        private Delta(Snapshot origin, Snapshot stat) {
            super(
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
            return timeSpan + " " + readOpsLatency() + " " + writeOpsLatency() + " " + ioQueueSize();
        }
    }

    protected IoStatistics(long readsCompleted, long readTime, long writesCompleted, long writeTime, long queueTime) {
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
