package io.stats.serialization;

import org.apache.kafka.common.serialization.*;

import java.time.Instant;

public class InstantSerde implements Serde<Instant> {
    private final LongSerializer innerSerializer = new LongSerializer();
    private final LongDeserializer innerDeserializer = new LongDeserializer();

    @Override
    public Serializer<Instant> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Instant> deserializer() {
        return deserializer;
    }

    private final Serializer<Instant> serializer = (topic, data) -> {
        return innerSerializer.serialize(topic, data.toEpochMilli());
    };

    private final Deserializer<Instant> deserializer = (topic, data) -> {
        return Instant.ofEpochMilli(innerDeserializer.deserialize(topic, data));
    };
}
