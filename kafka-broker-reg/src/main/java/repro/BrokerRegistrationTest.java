package repro;

import kafka.cluster.Broker;
import kafka.server.KafkaConfig;
import kafka.zk.BrokerInfo;
import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;
import static org.apache.zookeeper.client.ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET;

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
        private final TestContext spec;
        private InstrumentedRequestProcessor processor;

        Zookeeper(CountDownLatch zookeeperStopLatch, TestContext spec) {
            this.zookeeperStopLatch = zookeeperStopLatch;
            this.spec = spec;
        }

        public void run() {
            ZooKeeperServer zookeeper = null;
            ServerCnxnFactory cnxnFactory = null;

            try {
                QuorumPeerConfig config = new QuorumPeerConfig();
                config.parse("config/zookeeper.properties");
                FileTxnSnapLog txnLog = new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir());

                zookeeper = new InstrumentedZooKeeperServer(
                    null,
                    txnLog,
                    config.getTickTime(),
                    config.getMinSessionTimeout(),
                    config.getMaxSessionTimeout(),
                    config.getClientPortListenBacklog(),
                    null,
                    config.getInitialConfig(),
                    spec) {

                    @Override
                    protected void setupRequestProcessors() {
                        processor = new InstrumentedRequestProcessor(this, spec);
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
    }

    public static void main(String[] args) {
        // The second session is created on the server, but the response not sent to the client.
        Received secondZookeeperSession = new Received(ZooDefs.OpCode.createSession, false);

        Iterator<Received> requestTimeline = asList(
            // (1) First sessions is successfully established and the response sent to the client.
            new Received(ZooDefs.OpCode.createSession, true),

            // (2) First znode creation for broker registration, which is successful.
            new Received(ZooDefs.OpCode.multi, true),

            // (3) We let the ping timeout, and the reconnection timeout, so the client will have to create a
            // second session, for which we do not send the response.
            secondZookeeperSession,

            // (4) The znode for broker registration is received and processed (the znode created in (2) has been
            // deleted after the session created in (1) expired, so a new znode can be successfully created.).
            new Received(ZooDefs.OpCode.multi, false),

            // (5) After the connection timeout, the client re-attempts to create a new session, which is
            // successful this time.
            new Received(ZooDefs.OpCode.createSession, true),

            // (6) The multi request which failed in (4) is retried, and its response NODEEXISTS is returned
            // to the client.
            new Received(ZooDefs.OpCode.multi, true),

            // (7) The client perfoms a GetData in order to get the owner of the ephemeral znode.
            new Received(ZooDefs.OpCode.getData, true)
        ).iterator();

        Iterator<Connected> connectionTimeline = asList(
            new Connected(0),
            // The connection timeout is 18 seconds. Make this connection fails so that the session expires.
            new Connected(18500),
            new Connected(0),
            new Connected(0),
            new Connected(0),
            new Connected(0)
        ).iterator();

        Iterator<Expired> sessionExpirationTimeline = asList(
            new Expired(0),
            // This allows to slightly delay the processing of the expiration (not the expiration itself) of
            // the second session. This operation is asynchronous and can intrinsically happen before or after
            // the processing of the third multi request. Test run show it can randomly be before or after.
            // Introducing this artificial delay makes the test more deterministic (although it assumed the
            // third multi request won't be delayed by 3 seconds too!).
            new Expired(3000),
            new Expired(0)
        ).iterator();

        TestContext testContext = new TestContext(requestTimeline, connectionTimeline, sessionExpirationTimeline);

        try {
            // Instantiates a standalone single-node Zookeeper server.
            CountDownLatch zookeeperStopLatch = new CountDownLatch(1);
            Zookeeper zookeeper = new Zookeeper(zookeeperStopLatch, testContext);
            zookeeper.start();
            zookeeper.zookeeperStartLatch.await();

            System.setProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET, "ClientCnxnSocketNetty");

            // Instantiates the Zookeeper client running in Kafka.
            ZooKeeperClient zookeeperClient = new ZooKeeperClient(
                kafkaConfig.zkConnect(),
                kafkaConfig.zkSessionTimeoutMs(),
                kafkaConfig.zkConnectionTimeoutMs(),
                kafkaConfig.zkMaxInFlightRequests(),
                Time.SYSTEM,
                "kafka.server",
                "SessionExpireListener",
                new ZKClientConfig(),
                "ZkClient");

            KafkaZkClient client = new KafkaZkClient(zookeeperClient, false, Time.SYSTEM);

            client.registerBroker(brokerInfo);

            try {
                try {
                    // Send the multi(14) to create the broker znode only once the second session is created
                    // on the server although not acknowledged by the client.
                    secondZookeeperSession.awaitProcessed();
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
                testContext.terminate();

                // Delete znode so that we don't need for the znode to expire to rerun the test.
                client.deletePath(brokerInfo.path(), -1, false);
                client.close();
                zookeeperStopLatch.countDown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
