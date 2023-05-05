package io.stats.client;

import common.table.Table;
import common.table.Table.FormattedString;
import common.table.Tables;
import io.stats.common.IoStatistics;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class IoStatisticsPrinter implements ForeachAction<Windowed<Instant>, List<IoStatistics>> {
    private final IostatsFormatter iostatsFormatter = new IostatsFormatter();
    private final TimestampFormatter timestampFormatter = new TimestampFormatter();
    private final HeaderFormatter headerFormatter = new HeaderFormatter();

    private int it = 0;

    @Override
    public void apply(Windowed<Instant> window, List<IoStatistics> deltas) {
        if (deltas.isEmpty()) {
            return;
        }

        Collections.sort(deltas, Comparator.comparing(IoStatistics::brokerId));
        Table.Row row = Tables.newAsciiTable().newRow();

        if (it == 0) {
            row = row.newRow().addColumn("Timestamp", headerFormatter);
            for (IoStatistics stats: deltas) {
                row.addColumn("Broker " + stats.brokerId(), headerFormatter);
            }

            row = row.newRow();
        }

        try {
            row.addColumn(window.key(), timestampFormatter);

            for (int i = 0; i < deltas.get(0).brokerId() - 1; ++i) {
                row.addColumn(" ");
            }

            for (IoStatistics delta: deltas) {
                row.addColumn(delta, iostatsFormatter);
            }

            System.out.print(row.render());
            it = ++it % 30;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class IostatsFormatter implements Table.Formatter<IoStatistics> {
        @Override
        public FormattedString format(IoStatistics stats) {
            return stringify(stats.readOpsLatency(), 2)
                .concat(FormattedString.of(":"))
                .concat(stringify(stats.writeOpsLatency(), 2))
                .concat(FormattedString.of(":"))
                .concat(stringify(stats.ioQueueSize(), 2));
        }

        private FormattedString stringify(double latency, double threshold) {
            String color = "";
            if (!Double.isNaN(latency)) {
                color = latency > threshold ? Table.Color.red.code() : Table.Color.green.code();
            }

            String text = Double.isNaN(latency) ? " NaN " : String.format("%.3f", latency);
            return new FormattedString(color + text + Table.Color.reset, text.length());
        }
    }

    private static class TimestampFormatter implements Table.Formatter<Instant> {
        private static DateTimeFormatter formatter = DateTimeFormatter
            .ofPattern("YYYY-MM-dd HH:mm:ss")
            .withLocale(Locale.getDefault())
            .withZone(ZoneId.systemDefault());

        @Override
        public FormattedString format(Instant instant) {
            return FormattedString.of(formatter.format(instant));
        }
    }

    private static class HeaderFormatter implements Table.Formatter<String> {
        @Override
        public FormattedString format(String content) {
            return new FormattedString(Table.Color.blue.code() + content + Table.Color.reset, content.length());
        }
    }
}
