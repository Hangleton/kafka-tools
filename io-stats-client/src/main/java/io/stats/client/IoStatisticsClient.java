package io.stats.client;

import io.stats.common.InstantSerde;
import io.stats.common.IoStatisticsSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

public class IoStatisticsClient {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, args[0]);
        properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, InstantSerde.class.getName());
        properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, IoStatisticsSerde.class.getName());
        properties.put(APPLICATION_ID_CONFIG, "phoque2");
        properties.put(COMMIT_INTERVAL_MS_CONFIG, "2000");

        try {
            StreamsBuilder streamsBuilder = new StreamsBuilder();
            streamsBuilder
                .stream(
                    "__io_statistics",
                    Consumed.with(
                        new InstantSerde(),
                        new IoStatisticsSerde(),
                        (record, t) -> ((Instant) record.key()).toEpochMilli(),
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
                    Materialized.with(
                        new InstantSerde(),
                        Serdes.ListSerde(ArrayList.class, new IoStatisticsSerde())
                    )
                )
                .toStream()
                .foreach(new IostatsDeltaGenerator(singletonList(new IoStatisticsPrinter())));

            try (KafkaStreams s = new KafkaStreams(streamsBuilder.build(), properties)) {
                s.start();
                System.in.read();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
