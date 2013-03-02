package edu.ucsb.cs.mdcc.messaging;

import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransport;

public class AsyncMethodCallbackDecorator implements AsyncMethodCallback {

    private AsyncMethodCallback callback;
    private TTransport transport;

    public AsyncMethodCallbackDecorator(AsyncMethodCallback callback, TTransport transport) {
        this.callback = callback;
        this.transport = transport;
    }

    public void onComplete(Object o) {
        try {
            callback.onComplete(o);
        } finally {
            transport.close();
        }
    }

    public void onError(Exception e) {
        try {
            callback.onError(e);
        } finally {
            transport.close();
        }
    }
}
