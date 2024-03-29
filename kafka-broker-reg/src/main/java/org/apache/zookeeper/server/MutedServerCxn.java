package org.apache.zookeeper.server;

import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MutedServerCxn extends ServerCnxn {
    private final ServerCnxn delegate;

    public MutedServerCxn(ServerCnxn delegate) {
        super(delegate == null ? null : delegate.zkServer);
        this.delegate = delegate;
    }

    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag, String cacheKey, Stat stat, int opCode) {
        System.out.println("Dropping " + h + " " + r);
    }

    @Override
    void sendBuffer(ByteBuffer... buffers) {
        System.out.println("Dropping " + buffers);
    }

    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag) {
        System.out.println("Dropping " + h + " " + r);
    }

    @Override
    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return "[ALTERED CNXN] " + delegate.toString();
    }

    @Override
    public boolean isInvalid() {
        return delegate.isInvalid();
    }

    @Override
    public boolean isStale() {
        return delegate.isStale();
    }

    @Override
    protected long incrPacketsSent() {
        return delegate.incrPacketsSent();
    }

    @Override
    public void incrOutstandingAndCheckThrottle(RequestHeader h) {
        delegate.incrOutstandingAndCheckThrottle(h);
    }

    @Override
    protected long incrPacketsReceived() {
        return delegate.incrPacketsReceived();
    }

    @Override
    public boolean removeAuthInfo(Id id) {
        return delegate.removeAuthInfo(id);
    }

    @Override
    public long getOutstandingRequests() {
        return delegate.getOutstandingRequests();
    }

    @Override
    public Date getEstablished() {
        return delegate.getEstablished();
    }

    @Override
    public List<Id> getAuthInfo() {
        return delegate.getAuthInfo();
    }

    @Override
    public long getAvgLatency() {
        return delegate.getAvgLatency();
    }

    @Override
    public long getLastCxid() {
        return delegate.getLastCxid();
    }

    @Override
    public synchronized long getLastLatency() {
        return delegate.getLastLatency();
    }

    @Override
    public long getLastResponseTime() {
        return delegate.getLastResponseTime();
    }

    @Override
    public long getLastZxid() {
        return delegate.getLastZxid();
    }

    @Override
    public long getMaxLatency() {
        return delegate.getMaxLatency();
    }

    @Override
    public long getMinLatency() {
        return delegate.getMinLatency();
    }

    @Override
    public long getPacketsReceived() {
        return delegate.getPacketsReceived();
    }

    @Override
    public long getPacketsSent() {
        return delegate.getPacketsSent();
    }

    @Override
    public Map<String, Object> getConnectionInfo(boolean brief) {
        return delegate.getConnectionInfo(brief);
    }

    @Override
    public String getHostAddress() {
        return delegate.getHostAddress();
    }

    @Override
    public String getLastOperation() {
        return delegate.getLastOperation();
    }

    @Override
    public String getSessionIdHex() {
        return delegate.getSessionIdHex();
    }

    @Override
    public void decrOutstandingAndCheckThrottle(ReplyHeader h) {
        delegate.decrOutstandingAndCheckThrottle(h);
    }

    @Override
    public void addAuthInfo(Id id) {
        delegate.addAuthInfo(id);
    }

    @Override
    public void cleanupWriterSocket(PrintWriter pwriter) {
        delegate.cleanupWriterSocket(pwriter);
    }

    @Override
    public void dumpConnectionInfo(PrintWriter pwriter, boolean brief) {
        delegate.dumpConnectionInfo(pwriter, brief);
    }

    @Override
    public void resetStats() {
        delegate.resetStats();
    }

    @Override
    protected void packetReceived(long bytes) {
        delegate.packetReceived(bytes);
    }

    @Override
    protected void updateStatsForResponse(long cxid, long zxid, String op, long start, long end) {
        if (delegate != null) {
            delegate.updateStatsForResponse(cxid, zxid, op, start, end);
        }
    }

    @Override
    void disableRecv() {
        delegate.disableRecv();
    }

    @Override
    protected byte[] serializeRecord(Record record) throws IOException {
        return delegate.serializeRecord(record);
    }

    @Override
    protected ByteBuffer[] serialize(ReplyHeader h, Record r, String tag, String cacheKey, Stat stat, int opCode) throws IOException {
        return delegate.serialize(h, r, tag, cacheKey, stat, opCode);
    }

    @Override
    public void setInvalid() {
        delegate.setInvalid();
    }

    @Override
    public void setStale() {
        delegate.setStale();
    }

    @Override
    protected void packetSent() {
        delegate.packetSent();
    }

    @Override
    int getSessionTimeout() {
        return delegate.getSessionTimeout();
    }

    @Override
    public void close(DisconnectReason reason) {
        delegate.close(reason);
    }

    @Override
    public void sendCloseSession() {
        if (delegate != null) {
            delegate.sendCloseSession();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        delegate.process(event);
    }

    @Override
    public long getSessionId() {
        return delegate.getSessionId();
    }

    @Override
    void setSessionId(long sessionId) {
        delegate.setSessionId(sessionId);
    }


    @Override
    void enableRecv() {
        delegate.enableRecv();
    }

    @Override
    void disableRecv(boolean waitDisableRecv) {
        delegate.disableRecv(waitDisableRecv);
    }

    @Override
    void setSessionTimeout(int sessionTimeout) {
        delegate.setSessionTimeout(sessionTimeout);
    }

    @Override
    protected ServerStats serverStats() {
        return delegate.serverStats();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return delegate.getRemoteSocketAddress();
    }

    @Override
    public int getInterestOps() {
        return delegate.getInterestOps();
    }

    @Override
    public boolean isSecure() {
        return delegate.isSecure();
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        return delegate.getClientCertificateChain();
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {
        delegate.setClientCertificateChain(chain);
    }
}
