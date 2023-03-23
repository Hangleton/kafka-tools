package kafka.zookeeper;

import kafka.server.KafkaConfig;
import kafka.zk.ZookeeperSessionRenewer;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.client.ZKClientConfig;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class InstrumentedZooKeeperClient extends ZooKeeperClient {
    private  ZookeeperSessionRenewer sessionRenewer;

    public InstrumentedZooKeeperClient(KafkaConfig kafkaConfig) {
        super(
            kafkaConfig.zkConnect(),
            kafkaConfig.zkSessionTimeoutMs(),
            kafkaConfig.zkConnectionTimeoutMs(),
            kafkaConfig.zkMaxInFlightRequests(),
            Time.SYSTEM,
            "kafka.server",
            "SessionExpireListener",
            new ZKClientConfig(),
            "ZkClient"
        );
    }

    public void introduce(ZookeeperSessionRenewer sessionRenewer) {
        this.sessionRenewer = sessionRenewer;
    }

    @Override
    public void waitUntilConnected() {
        waitUntilConnected(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    private void waitUntilConnected(long timeout, TimeUnit timeUnit) {
        try {
            info(() -> "Waiting until connected.");
            long nanos = timeUnit.toNanos(timeout);
            Lock isConnectedOrExpiredLock = get("isConnectedOrExpiredLock");
            Condition isConnectedOrExpiredCondition = get("isConnectedOrExpiredCondition");

            isConnectedOrExpiredLock.lock();
            try {
                sessionRenewer.maybeWaitForSessionRenewal();
                States state = connectionState();

                while (!state.isConnected() && state.isAlive()) {
                    if (nanos <= 0) {
                        throw new ZooKeeperClientTimeoutException(
                            "Timed out waiting for connection while in state: " + state);
                    }
                    nanos = isConnectedOrExpiredCondition.awaitNanos(nanos);
                    state = connectionState();
                }
                if (state == States.AUTH_FAILED) {
                    throw new ZooKeeperClientAuthFailedException(
                        "Auth failed either before or while waiting for connection");

                } else if (state == States.CLOSED) {
                    throw new ZooKeeperClientExpiredException(
                        "Session expired either before or while waiting for connection");
                }

                set("isFirstConnectionEstablished", true);

            } finally {
                isConnectedOrExpiredLock.unlock();
            }

            info(() -> "Connected.");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T get(String name) {
        try {
            Field field = ZooKeeperClient.class.getDeclaredField("kafka$zookeeper$ZooKeeperClient$$" + name);
            field.setAccessible(true);
            return (T) field.get(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private <T> void set(String name, T value) {
        try {
            Field field = ZooKeeperClient.class.getDeclaredField("kafka$zookeeper$ZooKeeperClient$$" + name);
            field.setAccessible(true);
            field.set(this, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
