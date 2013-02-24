package edu.ucsb.cs.mdcc.paxos;

public class Result {

    private String key;
    private Object value;
    private long version;

    public Result(String key, Object value, long version2) {
        this.key = key;
        this.value = value;
        this.version = version2;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public long getVersion() {
        return version;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
