package edu.ucsb.cs.mdcc;

import java.nio.ByteBuffer;

public class Option {

    private String key;
    private ByteBuffer value;
    private long oldVersion;

    public Option(String key, ByteBuffer value, long l) {
        this.key = key;
        this.value = value;
        this.oldVersion = l;
    }

    public String getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public long getOldVersion() {
        return oldVersion;
    }
}
