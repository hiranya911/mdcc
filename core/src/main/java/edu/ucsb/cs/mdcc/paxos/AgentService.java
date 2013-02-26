package edu.ucsb.cs.mdcc.paxos;

import java.nio.ByteBuffer;

import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.ReadValue;

public interface AgentService {
    
    public boolean onPrepare(String object, BallotNumber ballot);
    
    public boolean onAccept(String transaction, String object, long oldVersion, BallotNumber ballot, ByteBuffer value);
    
    public void onDecide(String transaction, boolean commit);
    
    public ReadValue onRead(String object);
}