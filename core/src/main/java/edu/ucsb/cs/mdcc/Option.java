package edu.ucsb.cs.mdcc;

import java.nio.ByteBuffer;

public class Option {

    private String key;
    private ByteBuffer value;
    private long oldVersion;
    private boolean classic;

    public Option(String key, ByteBuffer value, long oldVersion, boolean classic) {
        this.key = key;
        this.value = value;
        this.oldVersion = oldVersion;
        this.classic = classic;
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

    public boolean isClassic() {
        return classic;
    }

    public void setClassic() {
        classic = true;
    }
}
