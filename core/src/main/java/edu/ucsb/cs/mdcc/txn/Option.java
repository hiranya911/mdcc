package edu.ucsb.cs.mdcc.txn;

public class Option {

    private String key;
    private Object value;
    private int oldVersion;

    public Option(String key, Object value, int oldVersion) {
        this.key = key;
        this.value = value;
        this.oldVersion = oldVersion;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public int getOldVersion() {
        return oldVersion;
    }
}
