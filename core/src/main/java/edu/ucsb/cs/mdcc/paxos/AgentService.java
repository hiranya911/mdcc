package edu.ucsb.cs.mdcc.paxos;

import java.nio.ByteBuffer;
import java.util.Map;

import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.ReadValue;

public interface AgentService {
    
    public boolean onPrepare(String object, BallotNumber ballot, long classicEndVersion);
    
    public boolean onAccept(String transaction, String object, long oldVersion, BallotNumber ballot, ByteBuffer value);
    
    public boolean runClassic(String transaction, String object, long oldVersion, ByteBuffer value);
    
    public void onDecide(String transaction, boolean commit);
    
    public ReadValue onRead(String object);
    
    public Map<String, ReadValue> onRecover(Map<String, Long> versions);
}