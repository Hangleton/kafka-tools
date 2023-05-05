package io.stats.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class IoStatisticsSerde implements Serde<IoStatistics> {

    @Override
    public Serializer<IoStatistics> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<IoStatistics> deserializer() {
        return deserializer;
    }

    private static Serializer<IoStatistics> serializer = (topic, data) -> {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(data);
            oos.flush();
            return baos.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    };

    private static Deserializer<IoStatistics> deserializer = (topic, data) -> {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (IoStatistics) ois.readObject();

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    };
}
