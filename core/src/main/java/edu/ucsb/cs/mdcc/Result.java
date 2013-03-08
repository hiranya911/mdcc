package edu.ucsb.cs.mdcc;

import java.nio.ByteBuffer;

public class Result {

    private String key;
    private ByteBuffer value;
    private long version;
    private boolean classic;
    private boolean deleted;

    public Result(String key, ByteBuffer value, long version, boolean classic) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.classic = classic;
    }

    public Result(String key, byte[] value, long version, boolean classic) {
        this.key = key;
        this.value = ByteBuffer.wrap(value);
        this.version = version;
        this.classic = classic;
    }

    public String getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public long getVersion() {
        return version;
    }

    public void setValue(ByteBuffer value) {
        this.value = value;
    }

    public void setValue(byte[] value) {
        this.value = ByteBuffer.wrap(value);
    }

    public boolean isClassic() {
        return classic;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }
}
