package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.messaging.ReadValue;

import java.util.Map;

public interface AgentService {
    
    public boolean onPrepare(Prepare prepare);
    
    public boolean onAccept(Accept accept);
    
    public boolean runClassic(String transaction, String object,
                              long oldVersion, byte[] value);
    
    public void onDecide(String transaction, boolean commit);
    
    public ReadValue onRead(String object);
    
    public Map<String, ReadValue> onRecover(Map<String, Long> versions);
}