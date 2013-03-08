package edu.ucsb.cs.mdcc;

public class Result {

    private String key;
    private byte[] value;
    private long version;
    private boolean classic;
    private boolean deleted;

    public Result(String key, byte[] value, long version, boolean classic) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.classic = classic;
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getVersion() {
        return version;
    }

    public void setValue(byte[] value) {
        this.value = value;
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
