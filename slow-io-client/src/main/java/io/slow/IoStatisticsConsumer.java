package io.slow;

import common.table.Table.Color;
import common.table.Table.Formatter;
import common.table.Tables;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.server.IoStatistics;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
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
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.util.Collections.singletonList;

public class IoStatisticsConsumer {

    public static void main(String[] args) {
        TimestreamWriteClient writeClient = buildWriteClient();
        TimestreamQueryClient queryClient = buildQueryClient();

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "phoque");

        try {
            KafkaConsumer<Long, byte[]> consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(singletonList("__io_statistics"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    consumer.seekToEnd(partitions);
                }
            });

            IoStatistics last = null;
            List<Record> timestreamRecords = new ArrayList<>();

            IostatsFormatter iostatsFormatter = new IostatsFormatter();
            TimestampFormatter timestampFormatter = new TimestampFormatter();

            StreamsBuilder streamsBuilder = new StreamsBuilder();
            streamsBuilder.stream("__io_statistics").foreach(((key, value) -> System.out.println(value)));
            KafkaStreams s = new KafkaStreams(streamsBuilder.build(), properties);
            s.start();

            System.in.read();

            while (true) {
                ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofSeconds(5));

                for (ConsumerRecord<Long, byte[]> record: records) {
                    IoStatistics.Snapshot stats = (IoStatistics.Snapshot) IoStatistics.fromRecord(record.value());

                    if (last != null) {
                        IoStatistics delta = stats.delta(last);
                        String table = Tables.newAsciiTable()
                            .newRow()
                                .addColumn(stats.time(), timestampFormatter)
                                .addColumn(delta, iostatsFormatter)
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

    private static class IostatsFormatter implements Formatter<IoStatistics> {
        @Override
        public String format(IoStatistics stats) {
            return stringify(stats.readOpsLatency(), 2)
                + ":" + stringify(stats.writeOpsLatency(), 2)
                + ":" + stringify(stats.ioQueueSize(), 2);
        }

        private String stringify(double latency, double threshold) {
            String color = "";
            if (!Double.isNaN(latency)) {
                color = latency > threshold ? Color.red.code() : Color.green.code();
            }

            String text = Double.isNaN(latency) ? "NaN " : String.format("%.2f", latency);
            return color + text + Color.reset;
        }
    }

    private static class TimestampFormatter implements Formatter<Instant> {
        private static DateTimeFormatter formatter = DateTimeFormatter
            .ofPattern("YYYY-MM-dd HH:mm:ss")
            .withLocale(Locale.getDefault())
            .withZone(ZoneId.systemDefault());

        @Override
        public String format(Instant instant) {
            return formatter.format(instant);
        }
    }
}
