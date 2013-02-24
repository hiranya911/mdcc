package edu.ucsb.cs.mdcc.txn;

class Result {

    private String key;
    private Object value;
    private int version;

    Result(String key, Object value, int version) {
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
