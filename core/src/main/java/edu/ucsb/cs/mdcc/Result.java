package edu.ucsb.cs.mdcc;

import java.nio.ByteBuffer;

public class Result {

    private String key;
    private ByteBuffer value;
    private long version;
    private boolean classic;

    public Result(String key, ByteBuffer value, long version, boolean classic) {
        this.key = key;
        this.value = value;
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

    public boolean isClassic() {
        return classic;
    }
}
