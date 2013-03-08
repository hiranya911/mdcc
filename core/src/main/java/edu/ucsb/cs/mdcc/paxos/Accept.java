package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.Option;

import java.nio.ByteBuffer;

public class Accept {

    private String transactionId;
    private BallotNumber ballotNumber;
    private String key;
    private long oldVersion;
    private byte[] value;

    public Accept(String transactionId, BallotNumber ballotNumber, Option option) {
        this.transactionId = transactionId;
        this.ballotNumber = ballotNumber;
        this.key = option.getKey();
        this.oldVersion = option.getOldVersion();
        this.value = option.getValue();
    }

    public Accept(String transactionId, BallotNumber ballotNumber, String key,
                  long oldVersion, byte[] value) {
        this.transactionId = transactionId;
        this.ballotNumber = ballotNumber;
        this.key = key;
        this.oldVersion = oldVersion;
        this.value = value;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public BallotNumber getBallotNumber() {
        return ballotNumber;
    }

    public void setBallotNumber(BallotNumber ballotNumber) {
        this.ballotNumber = ballotNumber;
    }

    public String getKey() {
        return key;
    }

    public long getOldVersion() {
        return oldVersion;
    }

    public byte[] getValue() {
        return value;
    }

    public String toString() {
        return "key=" + key + "; oldVersion=" + oldVersion + "; txn=" + transactionId;
    }
}
