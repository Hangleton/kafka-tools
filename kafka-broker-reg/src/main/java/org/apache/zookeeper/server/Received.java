package org.apache.zookeeper.server;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Received {
    private final int opCode;
    private final Lock lock = new ReentrantLock();
    private final Condition received = lock.newCondition();
    private final Condition processed = lock.newCondition();
    private State state = State.notReceived;
    private boolean sendResponse;

    public Received(int opCode, boolean sendResponse) {
        this.opCode = opCode;
        this.sendResponse = sendResponse;
    }

    public boolean matches(Request request) {
        return request.type == opCode;
    }

    public void serverReceived() {
        lock.lock();
        try {
            state = Collections.max(Arrays.asList(state, State.received));
            received.signalAll();

        } finally {
            lock.unlock();
        }
    }

    public void serverProcessed() {
        lock.lock();
        try {
            state = Collections.max(Arrays.asList(state, State.processed));
            processed.signalAll();

        } finally {
            lock.unlock();
        }
    }

    public Request maybeDecorate(Request request) {
        if (sendResponse) {
            return request;
        }
        ServerCnxn unresponsiveCxn = new MutedServerCxn(request.cnxn);
        return new DelegatingRequest(unresponsiveCxn, request);
    }

    public void awaitProcessed() throws InterruptedException {
        lock.lock();
        try {
            while (state.compareTo(State.processed) < 0) {
                processed.await();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "Received: " + Request.op2String(opCode);
    }

    enum State {
        notReceived,
        received,
        processed;
    }
}
