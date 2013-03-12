package edu.ucsb.cs.mdcc.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LRUCache<K,V> extends LinkedHashMap<K,V> {

    private int maxEntries;
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public LRUCache(int maxEntries) {
        super(maxEntries + 1);
        this.maxEntries = maxEntries;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return super.size() > maxEntries && isRemovable(eldest);
    }

    protected boolean isRemovable(Map.Entry<K,V> eldest) {
        return true;
    }

    @Override
    public V get(Object key) {
        try {
            lock.readLock().lock();
            return super.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        try {
            lock.writeLock().lock();
            return super.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }
}