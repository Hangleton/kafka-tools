package io.slow;

import common.table.Table;
import common.table.Tables;

import java.time.Instant;

public class TableMain {

    public static void main(String[] args) {
        IoStatisticsConsumer.IostatsFormatter iostatsFormatter = new IoStatisticsConsumer.IostatsFormatter();
        IoStatisticsConsumer.TimestampFormatter timestampFormatter = new IoStatisticsConsumer.TimestampFormatter();
        IoStatisticsConsumer.HeaderFormatter headerFormatter = new IoStatisticsConsumer.HeaderFormatter();

        Table.Row row = Tables.newAsciiTable().newRow();

        row = row.newRow()
            .addColumn("Timestamp", headerFormatter)
            .addColumn("Broker 0", headerFormatter);

        row = row.newRow();
        row.addColumn(Instant.now(), timestampFormatter);


        System.out.println(row.render());

    }

}
