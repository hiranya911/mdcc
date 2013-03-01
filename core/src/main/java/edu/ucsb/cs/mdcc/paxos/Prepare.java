package edu.ucsb.cs.mdcc.paxos;

public class Prepare {

    private String key;
    private BallotNumber ballotNumber;
    private long classicEndVersion;

    public Prepare(String key, BallotNumber ballotNumber, long classicEndVersion) {
        this.key = key;
        this.ballotNumber = ballotNumber;
        this.classicEndVersion = classicEndVersion;
    }

    public String getKey() {
        return key;
    }

    public BallotNumber getBallotNumber() {
        return ballotNumber;
    }

    public long getClassicEndVersion() {
        return classicEndVersion;
    }
}
