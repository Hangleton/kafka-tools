package common.table;

import java.util.function.Function;

public interface Table {

    Row newRow();

    String render();

    Table title(String t);

    boolean isEmpty();

    interface Row {

        Row addColumn(Object content);

        <T> Row addColumn(Object content, Formatter<T> formatter);

        Row newRow();

        String render();
    }

    interface Formatter<T> {
        String format(T content);
    }

    enum Color {
        black("\u001B[30m"),
        red("\u001B[31m"),
        green("\u001B[32m"),
        blue("\u001B[34m");

        public static final String reset = "\u001B[0m";

        private final String asciiCode;

        Color(String asciiCode) {
            this.asciiCode = asciiCode;
        }

        public String code() {
            return asciiCode;
        }
    }
}
