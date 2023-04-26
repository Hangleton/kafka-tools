package io.slow;

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
                    consumer.seekToBeginning(singletonList(new TopicPartition("__io_statistics", 0)));
                }
            });

            IoStatistics last = null;
            List<Record> timestreamRecords = new ArrayList<>();

            while (true) {
                ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofSeconds(5));

                for (ConsumerRecord record: records) {
                    IoStatistics.Snapshot stats = (IoStatistics.Snapshot) IoStatistics.fromRecord(record);

                    if (last != null) {
                        IoStatistics delta = stats.delta(last);
                        System.out.println(stats.time() + " " + delta);
                    }

                    last = stats;

                    List<Dimension> dimensions = new ArrayList<>();
                    dimensions.add(Dimension.builder().name("Snoopy").value("Snoopy").build());

                    Record timestreamRecord = Record.builder()
                        .dimensions(dimensions)
                        .measureName("ReadTime")
                        .measureValue("" + stats.readTime())
                        .time("" + stats.time().toEpochMilli())
                        .build();

                    timestreamRecords.add(timestreamRecord);

                    if (timestreamRecords.size() == 100) {
                        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                            .databaseName("DiskStats")
                            .tableName("IoStatistics")
                            .records(timestreamRecords)
                            .build();

                        writeClient.writeRecords(writeRecordsRequest);
                        timestreamRecords = new ArrayList<>();
                    }
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
}
