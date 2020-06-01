package tools;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import tools.Assignment.Broker;
import tools.Assignment.Rack;
import tools.Assignment.Replica;

import static java.util.Arrays.asList;

public final class AssignmentTest {

    @Test
    public void test() {
        Rack rack1 = new Rack("rack1");
        Rack rack2 = new Rack("rack2");
        Rack rack3 = new Rack("rack3");

        Broker broker1 = new Broker(1);
        Broker broker2 = new Broker(2);
        Broker broker3 = new Broker(3);
        Broker broker4 = new Broker(4);
        Broker broker5 = new Broker(5);
        Broker broker6 = new Broker(6);

        rack1.nodes.add(broker1);
        rack1.nodes.add(broker4);
        rack2.nodes.add(broker2);
        rack2.nodes.add(broker5);
        rack3.nodes.add(broker3);
        rack3.nodes.add(broker6);

        broker1.leaders.add(new Replica(new TopicPartition("topic", 0), 0));
        broker1.leaders.add(new Replica(new TopicPartition("topic", 1), 0));
        broker1.leaders.add(new Replica(new TopicPartition("topic", 2), 0));

        broker2.leaders.add(new Replica(new TopicPartition("topic", 3), 0));
        broker2.leaders.add(new Replica(new TopicPartition("topic", 4), 0));
        broker2.leaders.add(new Replica(new TopicPartition("topic", 5), 0));

        broker3.leaders.add(new Replica(new TopicPartition("topic", 6), 0));
        broker3.leaders.add(new Replica(new TopicPartition("topic", 7), 0));

        Assignment assignment = new Assignment();
        assignment.racks.addAll(asList(rack1, rack2, rack3));

        assignment.rebalance();
        System.out.println(assignment.toString());
    }

}
