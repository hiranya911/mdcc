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
    private String outstanding = null;
    private String outstandingClassic = null;

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

    public ByteBuffer getValue() {
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

    public void setValue(ByteBuffer value) {
        this.value = value;
    }

    public String getOutstandingClassic() {
        return outstandingClassic;
    }

    public void setOutstandingClassic(String outstandingClassic) {
        this.outstandingClassic = outstandingClassic;
    }
}
