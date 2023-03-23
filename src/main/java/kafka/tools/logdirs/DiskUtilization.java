package kafka.tools.logdirs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.tools.logdirs.DescribeLogDirs.Broker;
import kafka.tools.logdirs.DescribeLogDirs.Partition;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsResponseDataJsonConverter;

import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class DiskUtilization {
    private static DecimalFormat df = new DecimalFormat("###,###,###");

    public static void main(String[] args) {
        try {
            InputStream is = DiskUtilization.class.getClassLoader().getResourceAsStream("kafka-logs-dir.output.json");
            ObjectMapper mapper = new ObjectMapper();
            DescribeLogDirs data = mapper.readValue(is, DescribeLogDirs.class);

            for (Broker broker: data.brokers) {
                List<Partition> partitions = broker.logDirs.stream()
                    .flatMap(d -> d.partitions.stream())
                    .collect(toList());

                Collections.sort(partitions, Comparator.comparing(p -> -p.size));
                long totalBytes = partitions.stream().mapToLong(p -> p.size).sum();
                System.out.println("Broker " + broker.broker + " Total Size: " + df.format(mb(totalBytes)) + " MB");

                // Top 10.
                for (int i = 0; i < Math.min(20, partitions.size()); ++i) {
                    Partition partition = partitions.get(i);
                    System.out.println("  " + partition.partition + ": " + df.format(mb(partition.size)) + " MB");
                }

                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static long mb(long size) {
        return size / (1024 * 1024);
    }
}
