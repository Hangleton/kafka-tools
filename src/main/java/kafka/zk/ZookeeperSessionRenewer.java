package kafka.zk;

import kafka.zookeeper.ZooKeeperClient;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ZookeeperSessionRenewer {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final KafkaZkClient client;
    private final Lock lock = new ReentrantLock();

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
                try {
                    // This closes the existing Zookeeper session, creates a new Zookeeper client
                    // and initiates a new session creation.
                    zkClient.forceReinitialize();
                } finally {
                    lock.unlock();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public void maybeWaitForSessionRenewal() {
        lock.lock();
        lock.unlock();
    }

    public void shutdown() {
        executor.shutdown();
    }
}
