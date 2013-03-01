package edu.ucsb.cs.mdcc.dao;

import edu.ucsb.cs.mdcc.paxos.BallotNumber;

import java.nio.ByteBuffer;

public class Record {

    private String key;
    private ByteBuffer value = ByteBuffer.wrap("".getBytes());
    private long version = 0;
    private long classicEndVersion = -1;
    private BallotNumber ballot = new BallotNumber(0, "");
    private boolean prepared = false;
    private boolean outstanding = false;
    private boolean outstandingClassic = false;

    public Record(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public long getVersion() {
        return version;
    }

    public boolean isOutstanding() {
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

    public ByteBuffer getValue() {
        return value;
    }

    public long getClassicEndVersion() {
        return classicEndVersion;
    }

    public void setClassicEndVersion(long classicEndVersion) {
        this.classicEndVersion = classicEndVersion;
    }

    public void setOutstanding(boolean outstanding) {
        this.outstanding = outstanding;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void setValue(ByteBuffer value) {
        this.value = value;
    }

    public boolean isOutstandingClassic() {
        return outstandingClassic;
    }

    public void setOutstandingClassic(boolean outstandingClassic) {
        this.outstandingClassic = outstandingClassic;
    }
}
