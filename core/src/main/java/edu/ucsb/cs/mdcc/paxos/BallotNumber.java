package edu.ucsb.cs.mdcc.paxos;

public class BallotNumber implements Comparable<BallotNumber> {

    private long number;
    private String processId;

    public BallotNumber(long number, String processId) {
        this.number = number;
        this.processId = processId;
    }
    
    public BallotNumber(String str) {
        str = str.substring(1, str.lastIndexOf(']'));
        String[] segments = str.split(":");
        this.number = Long.parseLong(segments[0]);
        if (segments.length == 2) {
            this.processId = segments[1];
        } else {
            this.processId = "";
        }
    }

    public long getNumber() {
        return number;
    }

    public String getProcessId() {
        return processId;
    }

    public void increment() {
        this.number++;
    }

    public boolean isFastBallot() {
        return number < 0;
    }

    public int compareTo(BallotNumber o) {
        if (o == null) {
            return 1;
        }
        int diff = (int) (number - o.number);
        if (diff != 0) {
            return diff;
        } else {
            return processId.compareTo(o.processId);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BallotNumber) {
            BallotNumber bal = (BallotNumber) obj;
            return bal.number == number && bal.processId.equals(processId);
        }
        return false;
    }

    @Override
    public String toString() {
        return "[" + number + ":" + processId + "]";
    }

}
