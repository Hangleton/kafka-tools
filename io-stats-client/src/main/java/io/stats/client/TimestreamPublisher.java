package io.stats.client;

import io.stats.common.IoStatistics;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Windowed;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TimestreamPublisher implements ForeachAction<Windowed<Instant>, List<IoStatistics>> {
    private final TimestreamWriteClient writeClient = buildWriteClient();
    private List<Record> records = new ArrayList<>();

    @Override
    public void apply(Windowed<Instant> key, List<IoStatistics> deltas) {
        List<Dimension> dimensions = new ArrayList<>();
        dimensions.add(Dimension.builder().name("Snoopy").value("Snoopy").build());

        for (IoStatistics delta: deltas) {
            Record record = Record.builder()
                .dimensions(dimensions)
                .measureName("WriteOpsLatency")
                .measureValue("" + (Double.isNaN(delta.writeOpsLatency()) ? 0 : delta.writeOpsLatency()))
                .time("" + key.key().toEpochMilli())
                .build();

            records.add(record);

            if (records.size() == 50) {
                WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                    .databaseName("DiskStats")
                    .tableName("IoStatistics")
                    .records(records)
                    .build();

                writeClient.writeRecords(writeRecordsRequest);
                records = new ArrayList<>();
            }
        }
    }

    private static TimestreamWriteClient buildWriteClient() {
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();
        httpClientBuilder.maxConnections(5000);

        RetryPolicy.Builder retryPolicy = RetryPolicy.builder();
        retryPolicy.numRetries(10);

        ClientOverrideConfiguration.Builder overrideConfig = ClientOverrideConfiguration.builder();
        overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(20));
        overrideConfig.retryPolicy(retryPolicy.build());

        return TimestreamWriteClient.builder()
            .httpClientBuilder(httpClientBuilder)
            .overrideConfiguration(overrideConfig.build())
            .region(Region.US_EAST_1)
            .build();
    }
}
