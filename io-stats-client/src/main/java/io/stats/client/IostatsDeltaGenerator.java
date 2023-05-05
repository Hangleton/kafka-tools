package io.stats.client;

import io.stats.common.IoStatistics;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IostatsDeltaGenerator implements ForeachAction<Windowed<Instant>, List<IoStatistics>> {
    private final Map<Integer, IoStatistics> lastStats = new HashMap<>();
    private final List<ForeachAction<Windowed<Instant>, List<IoStatistics>>> inners;

    public IostatsDeltaGenerator(List<ForeachAction<Windowed<Instant>, List<IoStatistics>>> inners) {
        this.inners = Collections.unmodifiableList(inners);
    }

    @Override
    public void apply(Windowed<Instant> key, List<IoStatistics> snapshots) {
        List<IoStatistics> deltas = new ArrayList<>();

        snapshots.forEach(snapshot -> {
            int brokerId = snapshot.brokerId();
            if (lastStats.containsKey(brokerId)) {
                deltas.add(snapshot.delta(lastStats.get(brokerId)));
            }
            lastStats.put(brokerId, snapshot);
        });

        inners.forEach(inner -> inner.apply(key, deltas));
    }
}
