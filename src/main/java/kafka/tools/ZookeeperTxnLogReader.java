package kafka.tools;

import kafka.zk.BrokerIdZNode;
import kafka.zk.BrokerInfo;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.Record;
import org.apache.jute.RecordReader;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.LogFormatter;
import org.apache.zookeeper.server.persistence.FileHeader;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class ZookeeperTxnLogReader {
    private static final Logger LOG = LoggerFactory.getLogger(LogFormatter.class);

    static class Rec {
        private Instant time;
        private TxnHeader header;
        private Record record;
        Rec(Instant time, TxnHeader header, Record record) {
            this.time = time;
            this.header = header;
            this.record = record;
        }
    }

    static ZonedDateTime b = ZonedDateTime.of(2023, 3,5, 16, 0, 0, 0, ZoneId.systemDefault());
    static ZonedDateTime e = ZonedDateTime.of(2023, 3,5, 16, 40, 0, 0, ZoneId.systemDefault());

    public static void main(String[] args) {
        try {
            FileInputStream fis = new FileInputStream("");
            BinaryInputArchive logStream = BinaryInputArchive.getArchive(fis);
            FileHeader fhdr = new FileHeader();
            fhdr.deserialize(logStream, "fileheader");

            List<Rec> records = new ArrayList<>();

            if (fhdr.getMagic() != FileTxnLog.TXNLOG_MAGIC) {
                System.err.println("Invalid magic number for " + args[0]);
                System.exit(2);
            }
            System.out.println("ZooKeeper Transactional Log File with dbid "
                    + fhdr.getDbid() + " txnlog format version "
                    + fhdr.getVersion());

            int count = 0;
            while (true) {
                long crcValue;
                byte[] bytes;
                try {
                    crcValue = logStream.readLong("crcvalue");

                    bytes = logStream.readBuffer("txnEntry");
                } catch (EOFException e) {
                    System.out.println("EOF reached after " + count + " txns.");
                    break;
                }
                if (bytes.length == 0) {
                    // Since we preallocate, we define EOF to be an
                    // empty transaction
                    System.out.println("EOF reached after " + count + " txns.");
                    break;
                }
                Checksum crc = new Adler32();
                crc.update(bytes, 0, bytes.length);
                if (crcValue != crc.getValue()) {
                    throw new IOException("CRC doesn't match " + crcValue +
                            " vs " + crc.getValue());
                }
                TxnHeader hdr = new TxnHeader();
                Record txn = null; //SerializeUtils.deserializeTxn(bytes, hdr);
                Instant instant = Instant.ofEpochMilli(hdr.getTime());

                if (instant.isAfter(b.toInstant()) && instant.isBefore(e.toInstant())
                        && hdr.getClientId() == 216172783240153793L ) {
                    records.add(new Rec(instant, hdr, txn));
                }


                /*System.out.println(DateFormat.getDateTimeInstance(DateFormat.SHORT,
                        DateFormat.LONG).format(new Date(hdr.getTime()))
                        + " session 0x"
                        + Long.toHexString(hdr.getClientId())
                        + " cxid 0x"
                        + Long.toHexString(hdr.getCxid())
                        + " zxid 0x"
                        + Long.toHexString(hdr.getZxid())
                        + " " + TraceFormatter.op2String(hdr.getType()) + " " + txn);*/


                if (logStream.readByte("EOR") != 'B') {
                    LOG.error("Last transaction was partial.");
                    throw new EOFException("Last transaction was partial.");
                }
                count++;
            }

            records.toString();

            byte[] phoque = ((MultiTxn)records.get(1).record).getTxns().get(0).getData();
            byte[] p2 = ((MultiTxn)records.get(1).record).getTxns().get(1).getData();

            CreateRequest r = new CreateRequest();
            RecordReader reader = new RecordReader(new ByteArrayInputStream(phoque), "binary");
            reader.read(r);

            SetDataRequest r2 = new SetDataRequest();
            reader = new RecordReader(new ByteArrayInputStream(p2), "binary");
            reader.read(r2);

            BrokerInfo info = BrokerIdZNode.decode(18, r2.getData());

            System.out.println("");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
