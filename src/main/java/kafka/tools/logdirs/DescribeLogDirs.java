package kafka.tools.logdirs;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DescribeLogDirs {

    public static final class Broker {
        @JsonProperty
        int broker;

        @JsonProperty
        List<LogDir> logDirs;
    }

    public static final class LogDir {
        @JsonProperty
        String logDir;

        @JsonProperty
        String error;

        @JsonProperty
        List<Partition> partitions;
    }

    public static final class Partition {
        @JsonProperty
        String partition;

        @JsonProperty
        long size;

        @JsonProperty
        long offsetLag;

        @JsonProperty
        boolean isFuture;

        @Override
        public String toString() {
            return partition + ": " + size + " bytes";
        }
    }

    @JsonProperty
    int version;

    @JsonProperty
    List<Broker> brokers;
}
