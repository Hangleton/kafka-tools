package io.slow;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.server.IoStatistics;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

import static java.util.Collections.singletonList;

public class IoStatisticsConsumer {

    public static void main(String[] args) {
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

            while (true) {
                ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofSeconds(5));

                for (ConsumerRecord record: records) {
                    IoStatistics stats = IoStatistics.fromRecord(record);

                    if (last != null) {
                        IoStatistics delta = stats.delta(last);
                        System.out.println(stats);
                        System.out.println(delta);
                    }

                    last = stats;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
