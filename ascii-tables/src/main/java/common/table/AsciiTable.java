package common.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

public class AsciiTable implements Table {
    private final List<AsciiRow> rows = new ArrayList<>();
    private String title = "";

    @Override
    public synchronized Row newRow() {
        AsciiRow row = new AsciiRow();
        rows.add(row);
        return row;
    }

    @Override
    public Table title(String t) {
        title = ofNullable(t).orElse("");
        return this;
    }

    @Override
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    @Override
    public synchronized String render() {
        Optional<Integer> maybeColumns = rows.stream().map(r -> r.columns.size()).max(Integer::compare);

        return maybeColumns.map(columns -> {
            Map<Integer, Integer> lengths = new HashMap<>();

            for (int i = 0; i < columns; ++i) {
                int column = i;
                for (AsciiRow row: rows) {
                    if (i >= row.columns.size()) {
                        continue;
                    }
                    lengths.compute(i, (k, v) -> max(ofNullable(v).orElse(0), row.columns.get(column).length()));
                }
            }

            StringBuilder sb = new StringBuilder();

            if (!title.isBlank()) {
                sb.append(title);
            }

            Map<Integer, String> formats =
                    lengths.entrySet().stream().collect(toMap(e -> e.getKey(), e -> format("%%%ds | ", e.getValue())));

            for (AsciiRow row: rows) {
                sb.append("   | ");

                for (int i = 0; i < columns; ++i) {
                    if (i >= row.columns.size()) {
                        continue;
                    }

                    sb.append(format(formats.get(i), row.columns.get(i)));
                }

                sb.append("\n");
            }

            return sb.toString();

        }).orElse("");
    }

    public class AsciiRow implements Row {
        private final List<String> columns = new ArrayList<>();

        @Override
        public Row addColumn(Object content) {
            synchronized (AsciiTable.this) {
                columns.add(content instanceof Double ?  format("%.3f", content) : valueOf(content));
                return this;
            }
        }

        public Row addColumn(Object content, Function<Object, Color> color) {
            String c = color.apply(content).code();
            synchronized (AsciiTable.this) {
                columns.add(content instanceof Number ?
                    format("%s%.3f%s", c, ((Number) content).doubleValue(), reset)
                    : valueOf(content));
                return this;
            }
        }

        @Override
        public Row newRow() {
            return AsciiTable.this.newRow();
        }

        @Override
        public String render() {
            return AsciiTable.this.render();
        }
    }

    private static final String reset = "\u001B[0m";

    public static void main(String[] args) {
        Function<Object, Color> color = o -> {
            if (!(o instanceof Number)) {
                return Color.black;
            }

            return (((Number)o).doubleValue() > 7d) ? Color.red : Color.green;
        };

        Table table = Tables.newAsciiTable();
        table.newRow().addColumn("C1").addColumn("C2").newRow().addColumn(4.5, color).addColumn(2, color);
        table.newRow().addColumn(7.5, color).addColumn(6.3, color);
        System.out.println(table.render());
    }
}
