package tools;

import com.google.gson.Gson;
import org.apache.kafka.common.TopicPartition;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Collections.emptySet;

public final class AssignmentSpec {
    private final int version;
    private final List<AssignedTopicPartition> partitions;

    public String toString() {
        return format("tools.AssignmentSpec[%d assignments]", partitions.size());
    }

    public void printAssignmentByNode() {
        final Map<Integer, Set<TopicPartition>> leaders = new HashMap<>();
        final Map<Integer, Set<TopicPartition>> followers = new HashMap<>();

        for (AssignedTopicPartition assignment: partitions) {
            final TopicPartition topicPartition = new TopicPartition(assignment.topic, assignment.partition);
            final Iterator<Integer> nodes = assignment.replicas.iterator();

            leaders.computeIfAbsent(nodes.next(), node -> new HashSet<>()).add(topicPartition);
            while (nodes.hasNext()) {
                followers.computeIfAbsent(nodes.next(), node -> new HashSet<>()).add(topicPartition);
            }
        }

        final List<Integer> nodes = new ArrayList<>(leaders.keySet());
        Collections.sort(nodes);

        for (int node: nodes) {
            System.out.printf("Node %d: [leaders: %d, followers: %d]\n",
                    node, leaders.get(node).size(), followers.getOrDefault(node, emptySet()).size());
        }
    }

    private static final class AssignedTopicPartition {
        private final String topic;
        private final int partition;
        private final List<Integer> replicas;

        public AssignedTopicPartition(final String topic, final int partition, final List<Integer> replicas) {
            this.topic = topic;
            this.partition = partition;
            this.replicas = replicas;
        }
    }

    public AssignmentSpec(final int version, final List<AssignedTopicPartition> partitions) {
        this.version = version;
        this.partitions = partitions;
    }

    public static void main(String[] args) {
        Gson gson = new Gson();
        Reader reader = new InputStreamReader(AssignmentSpec.class.getResourceAsStream("plan.json"));
        AssignmentSpec plan = gson.fromJson(reader, AssignmentSpec.class);

        plan.printAssignmentByNode();
    }
}
