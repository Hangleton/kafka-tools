package io.slow;

import common.table.Table;
import common.table.Table.Color;
import common.table.Table.FormattedString;
import common.table.Table.Formatter;
import common.table.Table.Row;
import common.table.Tables;
import io.stats.serialization.InstantSerde;
import io.stats.serialization.IoStatisticsSerde;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.server.IoStatistics;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
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
import java.time.temporal.ChronoUnit;
import java.util.*;

public class IoStatisticsConsumer {

    public static void main(String[] args) {
        TimestreamWriteClient writeClient = buildWriteClient();
        TimestreamQueryClient queryClient = buildQueryClient();

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, InstantSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, IoStatisticsSerde.class.getName());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "phoque2");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "2000");

        try {
            /*KafkaConsumer<Long, byte[]> consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(singletonList("__io_statistics"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    consumer.seekToEnd(partitions);
                }
            });*/

            IoStatistics last = null;
            List<Record> timestreamRecords = new ArrayList<>();

            IostatsFormatter iostatsFormatter = new IostatsFormatter();
            TimestampFormatter timestampFormatter = new TimestampFormatter();

            StreamsBuilder streamsBuilder = new StreamsBuilder();
            streamsBuilder
                .stream(
                    "__io_statistics",
                    Consumed.with(
                        new InstantSerde(),
                        new IoStatisticsSerde(),
                        (record, partitionTime) -> ((Instant) record.key()).toEpochMilli(),
                        Topology.AutoOffsetReset.LATEST
                    )
                )
                .groupBy((key, value) -> key.truncatedTo(ChronoUnit.SECONDS))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(2)))
                .aggregate(
                    () -> new ArrayList<>(),
                    (key, value, aggregate) -> {
                        aggregate.add(value);
                        return aggregate;
                    },
                    Materialized.with(new InstantSerde(), Serdes.ListSerde(ArrayList.class, new IoStatisticsSerde()))
                )
                .toStream()
                .foreach(new IostatsPrinter());

            KafkaStreams s = new KafkaStreams(streamsBuilder.build(), properties);
            s.cleanUp();
            s.start();

            System.in.read();

            KafkaConsumer<Long, byte[]> consumer = null;
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

    private static class IostatsPrinter implements ForeachAction<Windowed<Instant>, List<IoStatistics>> {
        private final IostatsFormatter iostatsFormatter = new IostatsFormatter();
        private final TimestampFormatter timestampFormatter = new TimestampFormatter();
        private final HeaderFormatter headerFormatter = new HeaderFormatter();
        private final Map<Integer, IoStatistics> lastStats = new HashMap<>();
        private int it = 0;

        @Override
        public void apply(Windowed<Instant> window, List<IoStatistics> values) {
            Collections.sort(values, Comparator.comparing(IoStatistics::brokerId));
            Row row = Tables.newAsciiTable().newRow();

            if (it == 0) {
                row = row.newRow().addColumn("Timestamp", headerFormatter);
                for (IoStatistics stats: values) {
                    row.addColumn("Broker " + stats.brokerId(), headerFormatter);
                }

                row = row.newRow();
            }

            try {
                row.addColumn(window.key(), timestampFormatter);

                for (IoStatistics stats: values) {
                    if (lastStats.containsKey(stats.brokerId())) {
                        IoStatistics.Snapshot last = (IoStatistics.Snapshot) lastStats.get(stats.brokerId());
                        IoStatistics delta = stats.delta(last);
                        row.addColumn(delta, iostatsFormatter);
                    }

                    lastStats.put(stats.brokerId(), stats);
                }

                System.out.print(row.render());
                it = ++it % 30;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class IostatsFormatter implements Formatter<IoStatistics> {
        @Override
        public FormattedString format(IoStatistics stats) {
            return stringify(stats.readOpsLatency(), 2)
                .concat(FormattedString.of(":"))
                .concat(stringify(stats.writeOpsLatency(), 2))
                .concat(stringify(stats.ioQueueSize(), 2));
        }

        private FormattedString stringify(double latency, double threshold) {
            String color = "";
            if (!Double.isNaN(latency)) {
                color = latency > threshold ? Color.red.code() : Color.green.code();
            }

            String text = Double.isNaN(latency) ? "NaN " : String.format("%.2f", latency);
            return new FormattedString(color + text + Color.reset, text.length());
        }
    }

    private static class TimestampFormatter implements Formatter<Instant> {
        private static DateTimeFormatter formatter = DateTimeFormatter
            .ofPattern("YYYY-MM-dd HH:mm:ss")
            .withLocale(Locale.getDefault())
            .withZone(ZoneId.systemDefault());

        @Override
        public FormattedString format(Instant instant) {
            return FormattedString.of(formatter.format(instant));
        }
    }

    private static class HeaderFormatter implements Formatter<String> {
        @Override
        public FormattedString format(String content) {
            return new FormattedString(Color.blue.code() + content + Color.reset, content.length());
        }
    }
}
