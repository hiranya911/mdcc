package edu.ucsb.cs.mdcc.paxos;

import java.nio.ByteBuffer;
import java.util.Collection;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;
import edu.ucsb.cs.mdcc.messaging.ReadValue;

public class AppServer {

	private MDCCConfiguration configuration;
    private MDCCCommunicator communicator;

    public AppServer() {
        this.configuration = MDCCConfiguration.getConfiguration();
        this.communicator = new MDCCCommunicator();
	}
    
	public Result read(String key) {
        Member[] members = configuration.getMembers();
        ReadValue r = null;
        int memberIndex = 0;
        while (r == null && memberIndex < members.length) {
        	r = communicator.get(members[memberIndex], key);
            memberIndex++;
        }
		if (r == null) {
			return null;
        } else {
            boolean classic = r.getClassicEndVersion() >= r.getVersion();
			return new Result(key, ByteBuffer.wrap(r.getValue()), r.getVersion(), classic);
		}
	}
	
	public boolean commit(String txnId, Collection<Option> options) {
		boolean success;
        Member[] members = configuration.getMembers();
        
        VoteCollator collator = new VoteCollator(options, communicator, txnId);
        collator.start();

        int loopCount = 0;
        synchronized (collator) {
        	while (collator.getTotal() < options.size() && loopCount < 10) {
        		try {
					collator.wait(5000);
				} catch (InterruptedException ignored) {
				}
        		loopCount++;
        	}
        }
        
        success = collator.getAccepts() == options.size();
        for (Member member : members) {
            communicator.sendDecide(member, txnId, success);
        }
        return success;
	}

}
