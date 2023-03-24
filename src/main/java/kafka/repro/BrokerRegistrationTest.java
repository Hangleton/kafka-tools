package kafka.repro;

import kafka.cluster.Broker;
import kafka.server.KafkaConfig;
import kafka.zk.BrokerInfo;
import kafka.zk.KafkaZkClient;
import kafka.zk.ZookeeperSessionRenewer;
import kafka.zookeeper.InstrumentedZooKeeperClient;
import kafka.zookeeper.ZooKeeperClient;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.admin.AdminServerFactory;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;

public class BrokerRegistrationTest {
    private static final KafkaConfig kafkaConfig;
    private static final BrokerInfo brokerInfo;

    static {
        Map<Object, Object> config = new HashMap<>();
        config.put("zookeeper.connect", "localhost:2181");
        kafkaConfig = new KafkaConfig(config);

        ListenerName listener = ListenerName.forSecurityProtocol(PLAINTEXT);
        Broker broker = new Broker(18, "localhost", 9092, listener, PLAINTEXT);
        brokerInfo = new BrokerInfo(broker, 0, 9999);
    }

    private static class Zookeeper extends Thread {
        private final CountDownLatch zookeeperStopLatch;
        private final CountDownLatch zookeeperStartLatch = new CountDownLatch(1);
        private ZookeeperRequestProcessor processor;

        Zookeeper(CountDownLatch zookeeperStopLatch) {
            this.zookeeperStopLatch = zookeeperStopLatch;
        }

        public void run() {
            ZooKeeperServer zookeeper = null;
            ServerCnxnFactory cnxnFactory = null;

            try {
                QuorumPeerConfig config = new QuorumPeerConfig();
                config.parse("config/zookeeper.properties");
                FileTxnSnapLog txnLog = new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir());

                zookeeper = new ZooKeeperServer(
                    null,
                    txnLog,
                    config.getTickTime(),
                    config.getMinSessionTimeout(),
                    config.getMaxSessionTimeout(),
                    config.getClientPortListenBacklog(),
                    null,
                    config.getInitialConfig()) {

                    @Override
                    protected void setupRequestProcessors() {
                        processor = new ZookeeperRequestProcessor(this);
                        RequestProcessor syncProcessor = new SyncRequestProcessor(this, processor);
                        ((SyncRequestProcessor) syncProcessor).start();
                        firstProcessor = new PrepRequestProcessor(this, syncProcessor);
                        ((PrepRequestProcessor) firstProcessor).start();
                    }
                };


                cnxnFactory = ServerCnxnFactory.createFactory();
                cnxnFactory.configure(
                    config.getClientPortAddress(),
                    config.getMaxClientCnxns(),
                    config.getClientPortListenBacklog(),
                    false);
                cnxnFactory.startup(zookeeper);

                zookeeperStartLatch.countDown();
                zookeeperStopLatch.await();

            } catch (Exception e) {
                e.printStackTrace();

            } finally {
                try {
                    if (cnxnFactory != null)
                        cnxnFactory.shutdown();
                    if (zookeeper != null)
                        zookeeper.shutdown(true);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        void introduce(ZookeeperSessionRenewer renewer) {
            this.processor.introduce(renewer);
        }
    }

    public static void main(String[] args) {
        try {
            // Instantiates a standalone single-node Zookeeper server.
            CountDownLatch zookeeperStopLatch = new CountDownLatch(1);
            Zookeeper zookeeper = new Zookeeper(zookeeperStopLatch);
            zookeeper.start();
            zookeeper.zookeeperStartLatch.await();

            // Instantiates the Zookeeper client running in Kafka.
            InstrumentedZooKeeperClient zookeeperClient = new InstrumentedZooKeeperClient(kafkaConfig);
            KafkaZkClient client = new KafkaZkClient(zookeeperClient, false, Time.SYSTEM);

            // Used to force recreation of a ZK session.
            ZookeeperSessionRenewer sessionRenewer = new ZookeeperSessionRenewer(client);
            zookeeper.introduce(sessionRenewer);
            zookeeperClient.introduce(sessionRenewer);

            try {
                try {
                    client.registerBroker(brokerInfo);

                    // The expected error log is something like:
                    // ERROR Error while creating ephemeral at /brokers/ids/18, node already exists and owner '72071046321995776' does not match current session '72071046321995777' (kafka.zk.KafkaZkClient$CheckedEphemeral)

                    throw new AssertionError("Broker registration should fail");

                } catch (Exception e) {
                    e.printStackTrace();
                    //
                    // Should be:
                    //
                    // org.apache.zookeeper.KeeperException$NodeExistsException: KeeperErrorCode = NodeExists
                    //     at org.apache.zookeeper.KeeperException.create(KeeperException.java:126)
                    //     at kafka.zk.KafkaZkClient$CheckedEphemeral.getAfterNodeExists(KafkaZkClient.scala:2185)
                    //     at kafka.zk.KafkaZkClient$CheckedEphemeral.create(KafkaZkClient.scala:2123)
                    //     at kafka.zk.KafkaZkClient.checkedEphemeralCreate(KafkaZkClient.scala:2090)
                    //     at kafka.zk.KafkaZkClient.registerBroker(KafkaZkClient.scala:102)
                    //     at kafka.repro.BrokerRegistrationTest.main(BrokerRegistrationTest.java:137)
                    //
                    if (!(e instanceof KeeperException.NodeExistsException)) {
                        throw new AssertionError("Invalid failure mode");
                    }
                }
            } finally {
                // Delete znode so that we don't need for the znode to expire to rerun the test.
                client.deletePath(brokerInfo.path(), -1, false);
                client.close();
                zookeeperStopLatch.countDown();
                sessionRenewer.shutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
