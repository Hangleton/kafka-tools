package kafka.zk;

import kafka.zookeeper.ZooKeeperClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ZookeeperSessionRenewer {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final KafkaZkClient client;
    private final Lock lock = new ReentrantLock();
    private final Condition sessionRenewed = lock.newCondition();
    private boolean isRenewing;

    public ZookeeperSessionRenewer(KafkaZkClient client) {
        this.client = client;
    }

    public void renew() {
        executor.submit(() -> {
            try {
                ZooKeeperClient zkClient = (ZooKeeperClient) KafkaZkClient.class
                    .getDeclaredField("kafka$zk$KafkaZkClient$$zooKeeperClient")
                    .get(client);

                lock.lock();
                isRenewing = true;
                try {
                    // This closes the existing Zookeeper session, creates a new Zookeeper client
                    // and initiates a new session creation.
                    zkClient.forceReinitialize();
                } finally {
                    isRenewing = false;
                    sessionRenewed.signalAll();
                    lock.unlock();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public void maybeWaitForSessionRenewal() {
        try {
            lock.lock();
            try {
                while (isRenewing) {
                    sessionRenewed.await();
                }
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        executor.shutdown();
    }
}
