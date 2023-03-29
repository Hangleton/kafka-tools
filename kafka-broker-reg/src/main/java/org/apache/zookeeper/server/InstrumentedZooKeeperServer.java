package org.apache.zookeeper.server;

import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.util.JvmPauseMonitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class InstrumentedZooKeeperServer extends ZooKeeperServer {
    private final TestContext testContext;
    private final Set<Long> sessionIds = new ConcurrentSkipListSet<>();

    public InstrumentedZooKeeperServer(
        JvmPauseMonitor jvmPauseMonitor,
        FileTxnSnapLog txnLogFactory,
        int tickTime,
        int minSessionTimeout,
        int maxSessionTimeout,
        int clientPortListenBacklog,
        ZKDatabase zkDb,
        String initialConfig,
        TestContext testContext
    ) {
        super(
            jvmPauseMonitor,
            txnLogFactory,
            tickTime,
            minSessionTimeout,
            maxSessionTimeout,
            clientPortListenBacklog,
            zkDb,
            initialConfig
        );
        this.testContext = testContext;
    }

    @Override
    public void processConnectRequest(ServerCnxn cnxn, ByteBuffer incomingBuffer) throws IOException, ClientCnxnLimitException {
        if (!testContext.isTerminated()) {
            testContext.connectionTimeline.next().maybeInjectDelay();
        }
        super.processConnectRequest(cnxn, incomingBuffer);
        long sessionId = cnxn.getSessionId();
        if (sessionId != 0) {
            sessionIds.add(sessionId);
        }
    }

    @Override
    public void expire(SessionTracker.Session session) {
        // Check if sessionIds contains the session to avoid including sessions created outside the
        // test in the sequence of session expiration events.
        if (!testContext.isTerminated() && sessionIds.contains(session.getSessionId())) {
            testContext.sessionExpirationTimeline.next().maybeInjectDelay();
        }
        super.expire(session);
    }
}
