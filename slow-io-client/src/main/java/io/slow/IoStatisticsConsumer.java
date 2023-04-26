package io.slow;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.server.IoStatistics;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class IoStatisticsConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<Long, byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("__io_statistics"));

        IoStatistics last = null;

        while (true) {
            ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofSeconds(5));

            for (ConsumerRecord record: records) {
                IoStatistics stats = IoStatistics.fromRecord(record);

                if (last != null) {
                    IoStatistics delta = stats.delta(last);
                    System.out.println(delta);
                }

                last = stats;
            }
        }
    }
}
