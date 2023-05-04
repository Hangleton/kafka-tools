package io.slow;

import common.table.AsciiTable;
import common.table.Table;
import common.table.Table.Color;
import common.table.Table.Formatter;
import common.table.Tables;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.server.IoStatistics;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import static java.util.Collections.singletonList;

public class IoStatisticsConsumer {

    public static void main(String[] args) {
        TimestreamWriteClient writeClient = buildWriteClient();
        TimestreamQueryClient queryClient = buildQueryClient();

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "iostats_consumer_group");

        try {
            KafkaConsumer<Long, byte[]> consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(singletonList("__io_statistics"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                }
            });

            IoStatistics last = null;
            List<Record> timestreamRecords = new ArrayList<>();

            String header = Tables.newAsciiTable()
                .newRow()
                    .addColumn("t")
                    .addColumn("r:w:q")
                .render();

            System.out.println(header);

            MetricsFormatter formatter = new MetricsFormatter();

            while (true) {
                ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofSeconds(5));

                for (ConsumerRecord record: records) {
                    IoStatistics.Snapshot stats = (IoStatistics.Snapshot) IoStatistics.fromRecord(record);

                    if (last != null) {
                        IoStatistics delta = stats.delta(last);
                        String table = Tables.newAsciiTable()
                            .newRow()
                                .addColumn(stats.time())
                                .addColumn(delta, formatter)
                            .render();

                        System.out.print(table);

                        List<Dimension> dimensions = new ArrayList<>();
                        dimensions.add(Dimension.builder().name("Snoopy").value("Snoopy").build());

                        Record timestreamRecord = Record.builder()
                            .dimensions(dimensions)
                            .measureName("WriteOpsLatency")
                            .measureValue("" + (Double.isNaN(delta.writeOpsLatency()) ? 0 : delta.writeOpsLatency()))
                            .time("" + stats.time().toEpochMilli())
                            .build();

                        timestreamRecords.add(timestreamRecord);

                        if (timestreamRecords.size() == 1) {
                            WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                                .databaseName("DiskStats")
                                .tableName("IoStatistics")
                                .records(timestreamRecords)
                                .build();

                            writeClient.writeRecords(writeRecordsRequest);
                            timestreamRecords = new ArrayList<>();
                        }
                    }

                    last = stats;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static TimestreamWriteClient buildWriteClient() {
        ApacheHttpClient.Builder httpClientBuilder =
                ApacheHttpClient.builder();
        httpClientBuilder.maxConnections(5000);

        RetryPolicy.Builder retryPolicy =
                RetryPolicy.builder();
        retryPolicy.numRetries(10);

        ClientOverrideConfiguration.Builder overrideConfig =
                ClientOverrideConfiguration.builder();
        overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(20));
        overrideConfig.retryPolicy(retryPolicy.build());

        return TimestreamWriteClient.builder()
                .httpClientBuilder(httpClientBuilder)
                .overrideConfiguration(overrideConfig.build())
                .region(Region.US_EAST_1)
                .build();
    }

    private static TimestreamQueryClient buildQueryClient() {
        return TimestreamQueryClient.builder()
            .region(Region.US_EAST_1)
            .build();
    }

    private static class MetricsFormatter implements Formatter<IoStatistics> {
        @Override
        public String format(IoStatistics stats) {
            return stringify(stats.readOpsLatency(), 2)
                + ":" + stringify(stats.writeOpsLatency(), 2)
                + ":" + stringify(stats.ioQueueSize(), 2);
        }

        private String stringify(double latency, double threshold) {
            Color color = latency > threshold ? Color.red : Color.green;
            return String.format("%s%.2f%s", color, latency, Color.reset);
        }
    }
}
