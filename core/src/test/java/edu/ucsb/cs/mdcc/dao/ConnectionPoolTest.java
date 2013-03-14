package edu.ucsb.cs.mdcc.dao;

import edu.ucsb.cs.mdcc.config.Member;
import junit.framework.TestCase;
import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPool;

public class ConnectionPoolTest extends TestCase {

    public void testPool() throws Exception {
        KeyedObjectPool<Member,Object> nonBlockingPool =
                new StackKeyedObjectPool<Member,Object>(new ThriftNonBlockingConnectionPool());
        Member member = new Member("localhost", 8080, "0", false);
        Object o1 = nonBlockingPool.borrowObject(member);
        nonBlockingPool.returnObject(member, o1);
        Object o2 = nonBlockingPool.borrowObject(member);
        assertTrue(o1 == o2);
    }

    public class ThriftNonBlockingConnectionPool extends BaseKeyedPoolableObjectFactory<Member,Object> {

        @Override
        public Object makeObject(Member member) throws Exception {
            System.out.println("[CREATE]");
            return new CachedObject();
        }

        @Override
        public boolean validateObject(Member key, Object obj) {
            long lastReturnTime = ((CachedObject) obj).getLastReturnTime();
            boolean valid = lastReturnTime < 0 || System.currentTimeMillis() - lastReturnTime < 50000;
            System.out.println("[VALIDATE] " + valid);
            return valid;
        }

        @Override
        public void destroyObject(Member key, Object obj) throws Exception {
            System.out.println("[DESTROY]");
        }

        @Override
        public void passivateObject(Member key, Object obj) throws Exception {
            System.out.println("[PASSIVATE]");
            ((CachedObject) obj).setLastReturnTime(System.currentTimeMillis());
        }

    }

    public static class CachedObject {

        private long lastReturnTime = -1L;

        public long getLastReturnTime() {
            return lastReturnTime;
        }

        public void setLastReturnTime(long lastReturnTime) {
            this.lastReturnTime = lastReturnTime;
        }
    }
}
