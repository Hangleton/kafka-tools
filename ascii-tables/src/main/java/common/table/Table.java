package common.table;

import java.util.function.Function;

public interface Table {

    Row newRow();

    String render();

    Table title(String t);

    boolean isEmpty();

    interface Row {

        Row addColumn(Object content);

        Row addColumn(Object content, Function<Object, Color> color);

        Row newRow();

        String render();
    }

    enum Color {
        black("\u001B[30m"),
        red("\u001B[31m"),
        green("\u001B[32m"),
        blue("\u001B[34m");

        private final String asciiCode;

        Color(String asciiCode) {
            this.asciiCode = asciiCode;
        }

        public String code() {
            return asciiCode;
        }
    }
}
