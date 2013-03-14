package edu.ucsb.cs.mdcc.messaging;

import edu.ucsb.cs.mdcc.config.Member;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TNonblockingSocket;

public class AsyncMethodCallbackDecorator implements AsyncMethodCallback {

    private AsyncMethodCallback callback;
    private TNonblockingSocket transport;
    private Member member;
    private KeyedObjectPool<Member,TNonblockingSocket> pool;

    public AsyncMethodCallbackDecorator(AsyncMethodCallback callback, TNonblockingSocket transport,
                                        Member member, KeyedObjectPool<Member, TNonblockingSocket> pool) {
        this.callback = callback;
        this.transport = transport;
        this.member = member;
        this.pool = pool;
    }

    public AsyncMethodCallbackDecorator(TNonblockingSocket transport, Member member,
                                        KeyedObjectPool<Member, TNonblockingSocket> pool) {
        this.transport = transport;
        this.member = member;
        this.pool = pool;
    }

    public void onComplete(Object o) {
        try {
            if (callback != null) {
                callback.onComplete(o);
            }
        } finally {
            try {
                pool.returnObject(member, transport);
            } catch (Exception ignored) {
            }
        }
    }

    public void onError(Exception e) {
        try {
            if (callback != null) {
                callback.onError(e);
            }
        } finally {
            try {
                pool.invalidateObject(member, transport);
            } catch (Exception ignored) {
            }
        }
    }
}
