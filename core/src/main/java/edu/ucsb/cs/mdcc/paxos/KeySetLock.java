package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class KeySetLock<T> {

    private Set<T> pool = new HashSet<T>();

    public void lock(T key) {
        synchronized (pool) {
            while (pool.contains(key)) {
                try {
                    pool.wait(10);
                } catch (InterruptedException e) {
                }
            }
            pool.add(key);
        }
    }

    public void lock(Collection<T> keys) {
        synchronized (pool) {
            for (T key : keys) {
                while (pool.contains(key)) {
                    try {
                        pool.wait(10);
                    } catch (InterruptedException e) {
                    }
                }
                pool.add(key);
            }
        }
    }

    public void unlock(T key) {
        synchronized (pool) {
            pool.remove(key);
            pool.notifyAll();
        }
    }

    public void unlock(Collection<T> keys) {
        synchronized (pool) {
            for (T key : keys) {
                pool.remove(key);
            }
            pool.notifyAll();
        }
    }
}
