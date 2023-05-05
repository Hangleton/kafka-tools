package common.table;

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
        FormattedString format(T content);
    }

    final class FormattedString {
        private final String string;
        private final int size;

        public FormattedString(String string, int size) {
            this.string = string;
            this.size = size;
        }

        @Override
        public String toString() {
            return string;
        }

        public int size() {
            return size;
        }

        public int escapedSize() {
            return string.length() - size;
        }

        public FormattedString concat(FormattedString s) {
            return new FormattedString(string + s, size + s.size);
        }

        public static FormattedString of(String s) {
            return new FormattedString(s, s.length());
        }
    }

    enum Color {
        black("\u001B[30m"),
        white("\u001B[37m"),
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
