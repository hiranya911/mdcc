package edu.ucsb.cs.mdcc.messaging;

import edu.ucsb.cs.mdcc.config.Member;
import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ThriftConnectionPool extends BaseKeyedPoolableObjectFactory<Member,TTransport> {

    @Override
    public TTransport makeObject(Member member) throws Exception {
        String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new CachedTTransport(new TSocket(host, port));
        transport.open();
        return transport;
    }

    @Override
    public boolean validateObject(Member key, TTransport obj) {
        long lastReturnTime = ((CachedTTransport) obj).getLastReturnTime();
        return obj.isOpen() && (lastReturnTime < 0 ||
                System.currentTimeMillis() - lastReturnTime < 50000);
    }

    @Override
    public void destroyObject(Member key, TTransport obj) throws Exception {
        obj.close();
    }

    @Override
    public void passivateObject(Member key, TTransport obj) throws Exception {
        ((CachedTTransport) obj).setLastReturnTime(System.currentTimeMillis());
    }

    public static class CachedTTransport extends TFramedTransport {

        private long lastReturnTime = -1L;

        public CachedTTransport(TTransport transport) {
            super(transport);
        }

        public long getLastReturnTime() {
            return lastReturnTime;
        }

        public void setLastReturnTime(long lastReturnTime) {
            this.lastReturnTime = lastReturnTime;
        }
    }
}
