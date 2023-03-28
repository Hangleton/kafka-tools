package org.apache.zookeeper.server;

import org.apache.zookeeper.ZooDefs;

public class InstrumentedRequestProcessor extends FinalRequestProcessor {
    private final Received ping = new Received(ZooDefs.OpCode.ping, false);
    private final Received closeSession = new Received(ZooDefs.OpCode.closeSession, true);

    private final TestContext testContext;

    public InstrumentedRequestProcessor(ZooKeeperServer zks, TestContext testContext) {
        super(zks);
        this.testContext = testContext;
    }

    @Override
    public void processRequest(Request request) {
        if (testContext.isTerminated()) {
            super.processRequest(request);
            return;
        }

        Received expectedReceived;

        if (request.type == ZooDefs.OpCode.ping) {
            expectedReceived = ping;
        } else if (request.type == ZooDefs.OpCode.closeSession) {
            expectedReceived = closeSession;
        } else {
            if (!testContext.requestTimelime.hasNext()) {
                throw new AssertionError(request);
            }

            expectedReceived = testContext.requestTimelime.next();
        }


        if (!expectedReceived.matches(request)) {
            throw new AssertionError("Expected: " + expectedReceived + " Actual: " + request);
        }

        request = expectedReceived.maybeDecorate(request);

        System.out.println(request);
        expectedReceived.serverReceived();

        try {
            super.processRequest(request);
        } finally {
            expectedReceived.serverProcessed();
        }
    }
}
