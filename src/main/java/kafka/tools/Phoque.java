package kafka.tools;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class Phoque {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,  "localhost:9092");

        AdminClient client = AdminClient.create(properties);

        System.out.println(client.describeTopics(Collections.singleton("phoque")).all().get());
        System.out.println(client.describeLogDirs(Collections.singletonList(0)).all().get());
    }


}
