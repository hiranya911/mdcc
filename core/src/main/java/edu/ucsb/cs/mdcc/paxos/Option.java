package edu.ucsb.cs.mdcc.paxos;

public class Option {

    private String key;
    private Object value;
    private long oldVersion;

    public Option(String key, Object value, long l) {
        this.key = key;
        this.value = value;
        this.oldVersion = l;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public long getOldVersion() {
        return oldVersion;
    }
}
