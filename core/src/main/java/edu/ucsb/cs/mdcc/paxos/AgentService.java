package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.messaging.BallotNumber;

public interface AgentService {
    
    public boolean onPrepare(String object, BallotNumber ballot);
    
    public boolean onAccept(String transaction, String object, long oldVersion, BallotNumber ballot, String value);
    
    public void onDecide(String transaction, boolean commit);
    
    public String onRead(String object);
}