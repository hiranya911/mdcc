package edu.ucsb.cs.mdcc.dao;

import edu.ucsb.cs.mdcc.paxos.BallotNumber;

public class Record extends Cacheable {

    private String key;
    private byte[] value = new byte[]{};
    private long version = 0;
    private long classicEndVersion = -1;
    private BallotNumber ballot = new BallotNumber(0, "");
    private boolean prepared = false;
    private String outstanding = null;
    private boolean dirty;

    public Record(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public long getVersion() {
        return version;
    }

    public String getOutstanding() {
        return outstanding;
    }

    public boolean isPrepared() {
        return prepared;
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

    public BallotNumber getBallot() {
        return ballot;
    }

    public void setBallot(BallotNumber ballot) {
        this.ballot = ballot;
    }

    public byte[] getValue() {
        return value;
    }

    public long getClassicEndVersion() {
        return classicEndVersion;
    }

    public void setClassicEndVersion(long classicEndVersion) {
        this.classicEndVersion = classicEndVersion;
    }

    public void setOutstanding(String outstanding) {
        this.outstanding = outstanding;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }
}
