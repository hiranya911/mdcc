package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.messaging.RecordVersion;

public interface AgentService {
	public void onElection();

    public void onVictory(String hostName, int port);

    public boolean onLeaderQuery();
    
    public boolean onAccept(String transaction, String object, RecordVersion oldVersion, String processId, String value);
    
    public void onDecide(String transaction, boolean commit);
    
    public String onGet(String object);
}