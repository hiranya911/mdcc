package edu.ucsb.cs.mdcc.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K,V> extends LinkedHashMap<K,V> {

    private int maxEntries;

    public LRUCache(int maxEntries) {
        super(maxEntries + 1);
        this.maxEntries = maxEntries;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return super.size() > maxEntries;
    }
}