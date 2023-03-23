package kafka.repro;

import kafka.zk.ZookeeperSessionRenewer;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;

public class ZookeeperRequestProcessor extends FinalRequestProcessor {
    // Confined on Zookeeper SyncThread.
    boolean isFirstRegistration = true;
    ZookeeperSessionRenewer renewer;

    public ZookeeperRequestProcessor(ZooKeeperServer zks) {
        super(zks);
    }

    public void introduce(ZookeeperSessionRenewer renewer) {
        this.renewer = renewer;
    }

    @Override
    public void processRequest(Request request) {
        System.out.println(request);

        if (request.type == ZooDefs.OpCode.multi && isFirstRegistration) {
            // This is the first broker registration. In this test, only broker registration uses
            // a multi-transaction with opcode 14. This multi-transaction includes a create znode
            // and set data.
            // Here, close the connection to ensure the client never receives the response to the
            // first request. Note that closing the connection does not expire the session.
            request.cnxn.close(ServerCnxn.DisconnectReason.CLIENT_CLOSED_CONNECTION);
        }

        // Even in the case of the first broker znode registration, let Zookeeper process the request
        // and materialize the znode /brokers/ids/18.
        super.processRequest(request);

        if (request.type == ZooDefs.OpCode.multi && isFirstRegistration) {
            // Close the existing Zookeeper session and create a new one with a new ID.
            renewer.renew();
            isFirstRegistration = false;
        }
    }
}
