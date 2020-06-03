/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import static java.util.Collections.unmodifiableMap;

public final class AssignmentMap {
    private final int version;
    private final List<AssignedTopicPartition> partitions;

    public AssignmentDelta calculateDelta(final AssignmentMap alt) {
        final Map<Integer, Set<TopicPartition>> assignment = groupByNode(this);
        final Map<Integer, Set<TopicPartition>> altAssignment = groupByNode(alt);

        final Set<Integer> nodes = new HashSet<>();
        nodes.addAll(assignment.keySet());
        nodes.addAll(altAssignment.keySet());
        for (int node: nodes) {
            assignment.computeIfAbsent(node, x -> new HashSet<>());
            altAssignment.computeIfAbsent(node, x -> new HashSet<>());
        }

        final Map<Integer, TopicPartitionsDelta> deltas = new HashMap<>();

        for (Map.Entry<Integer, Set<TopicPartition>> e: assignment.entrySet()) {
            final int node = e.getKey();
            final Set<TopicPartition> assigned = e.getValue();
            final Set<TopicPartition> altAssigned = altAssignment.get(node);

            final TopicPartitionsDelta delta = new TopicPartitionsDelta();
            delta.added.addAll(diff(assigned, altAssigned));
            delta.removed.addAll(diff(altAssigned, assigned));

            deltas.put(node, delta);
        }

        return new AssignmentDelta(deltas);
    }

    private static Map<Integer, Set<TopicPartition>> groupByNode(final AssignmentMap plan) {
        final Map<Integer, Set<TopicPartition>> assignments = new HashMap<>();

        for (final AssignedTopicPartition p: plan.partitions) {
            final TopicPartition tp = new TopicPartition(p.topic, p.partition);
            for (int node: p.replicas) {
                assignments.computeIfAbsent(node, x -> new HashSet<>()).add(tp);
            }
        }

        return assignments;
    }

    private static Set<TopicPartition> diff(final Set<TopicPartition> a, final Set<TopicPartition> b) {
        final Set<TopicPartition> minusSet = new HashSet<>();
        minusSet.addAll(a);
        minusSet.removeAll(b);
        return minusSet;
    }

    public String toString() {
        return format("AssignmentMap[%d assignments]", partitions.size());
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

        int replicaCount = 0;

        for (int node: nodes) {
            int lcount = leaders.get(node).size();
            int fcount = followers.getOrDefault(node, emptySet()).size();
            replicaCount += lcount + fcount;

            System.out.printf("Node %d: [leaders: %d, followers: %d]\n", node, lcount, fcount);
        }

        System.out.printf("Total: %d topic-partitions; %d replicas", partitions.size(), replicaCount);
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

    public AssignmentMap(final int version, final List<AssignedTopicPartition> partitions) {
        this.version = version;
        this.partitions = partitions;
    }

    public static final class AssignmentDelta {
        private final Map<Integer, TopicPartitionsDelta> deltas;

        public AssignmentDelta(final Map<Integer, TopicPartitionsDelta> deltas) {
            this.deltas = unmodifiableMap(deltas);
        }

        public String toString() {
            final StringBuilder sb = new StringBuilder();
            int wmoves = 0;

            for (Map.Entry<Integer, TopicPartitionsDelta> e: deltas.entrySet()) {
                final TopicPartitionsDelta delta = e.getValue();
                wmoves += delta.added.size();
                sb.append(format("Node %d: %s\n", e.getKey(), e.getValue()));
            }
            sb.append(format("Total moved: %d replicas", wmoves));
            return sb.toString();
        }
    }

    public static final class TopicPartitionsDelta {
        private final Set<TopicPartition> added = new HashSet<>();
        private final Set<TopicPartition> removed = new HashSet<>();

        public String toString() {
            return format("%d partitions added, %d partitions removed", added.size(), removed.size());
        }
    }

    private static Gson gson = new Gson();

    private static AssignmentMap openPlan(String fn) {
        Reader reader = new InputStreamReader(AssignmentMap.class.getClassLoader().getResourceAsStream(fn));
        return gson.fromJson(reader, AssignmentMap.class);
    }

    public static void main(String[] args) {
        final AssignmentMap a = openPlan("original.json");
        final AssignmentMap b = openPlan("plan.json");
        System.out.println(b.calculateDelta(a));

        System.out.println();
        a.printAssignmentByNode();

        System.out.println("\n");
        b.printAssignmentByNode();
    }
}
