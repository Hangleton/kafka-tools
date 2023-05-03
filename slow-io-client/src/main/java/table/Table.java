package table;

public interface Table {

    Row newRow();

    String render();

    Table title(String t);

    boolean isEmpty();

    interface Row {

        Row addColumn(Object content);

        Row newRow();

        String render();
    }
}
