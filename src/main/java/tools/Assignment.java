package tools;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import static java.lang.String.format;

public final class Assignment {
    final List<Rack> racks = new ArrayList<>();

    public void rebalance() {
        if (!racksBalanced()) {
            throw new IllegalStateException("Unbalanced racks: " + toString());
        }

        for (Rack rack: racks) {
            rebalance(rack, broker -> broker.leaders);
            rebalance(rack, broker -> broker.followers);
        }
    }

    private void rebalance(Rack rack, Function<Broker, BlockingQueue<Replica>> getReplicas) {
        final int nodeCount = rack.nodes.size();

        int partitionCount = 0;
        for (Broker broker: rack.nodes) {
            partitionCount += getReplicas.apply(broker).size();
        }

        final int targetCount = 1 + partitionCount / nodeCount;
        final BlockingQueue<Replica> toMove = new LinkedBlockingQueue<>();

        for (Broker broker: rack.nodes) {
            int delta = getReplicas.apply(broker).size() - targetCount;
            if (delta > 0) {
                broker.leaders.drainTo(toMove, delta);
            }
        }

        for (Broker broker: rack.nodes) {
            int delta = getReplicas.apply(broker).size() - targetCount;
            if (delta < -1) {
                toMove.drainTo(broker.leaders, -(delta + 1));
            }
        }
    }

    private boolean racksBalanced() {
        int total = 0;
        for (Rack rack: racks) {
            total += rack.partitionCount();
        }
        int avg = 1 + total / racks.size();
        for (Rack rack: racks) {
            if (rack.partitionCount() != avg && rack.partitionCount() != avg - 1) {
                return false;
            }
        }
        return true;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Rack rack: racks) {
            sb.append(format("Rack %s:[", rack.id));
            for (Broker broker: rack.nodes) {
                sb.append(format("Broker %d [leaders=", broker.id));
                sb.append(broker.leaders);
                sb.append(", followers=");
                sb.append(broker.followers);
                sb.append("] ");
            }
            sb.append("]\n");
        }
        return sb.toString();
    }

    public static final class Broker {
        private final int id;
        final BlockingQueue<Replica> leaders = new LinkedBlockingQueue<>();
        final BlockingQueue<Replica> followers = new LinkedBlockingQueue<>();

        public Broker(int id) {
            this.id = id;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Broker[leaders={");
            for (Replica replica: leaders) {
                sb.append(replica).append(", ");
            }
            sb.append("}; followers={");
            for (Replica replica: leaders) {
                sb.append(replica).append(", ");
            }
            sb.append("}]");
            return sb.toString();
        }
    }

    public static final class Rack {
        private final String id;
        final List<Broker> nodes = new ArrayList<>();

        public Rack(String id) {
            this.id = id;
        }

        public int partitionCount() {
            int t = 0;
            for (Broker broker: nodes) {
                t += broker.leaders.size() + broker.followers.size();
            }
            return t;
        }
    }

    public static final class Replica {
        private final TopicPartition topicPartition;
        private final int volumeSize;

        public Replica(TopicPartition topicPartition, int volumeSize) {
            this.topicPartition = topicPartition;
            this.volumeSize = volumeSize;
        }

        public String toString() {
            return topicPartition.toString();
        }
    }
}
