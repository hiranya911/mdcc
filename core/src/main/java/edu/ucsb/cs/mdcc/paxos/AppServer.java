package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;

public class AppServer {

    public static final String DEFAULT_SERVER_ID = "AppServer";

	private int quorumSize;
	private MDCCConfiguration configuration;
    private MDCCCommunicator communicator;

    public AppServer() {
        this.configuration = MDCCConfiguration.getConfiguration();
        int n = configuration.getMembers().length;
		this.quorumSize = n - (n / 4) + ((n + 1) % 2);
        this.communicator = new MDCCCommunicator();
	}
    
	public Result read(String key) {
        Member[] members = configuration.getMembers();
		String readString = communicator.get(members[0], key);
		if (readString == null) {
			return null;
        } else {
        	long version = 0;
        	if (!readString.startsWith("|"))
        		version = Long.parseLong(readString.substring(0, readString.indexOf('|')));
			readString = readString.substring(readString.indexOf('|') + 1);
			readString = readString.substring(readString.indexOf('|') + 1);
			return new Result(key, readString, version);
		}
	}
	
	public boolean commit(String txnId, Collection<Option> options) {
		boolean success;
        Member[] members = configuration.getMembers();
        
        VoteCollator collator = new VoteCollator();
        for (Option option : options) {
        	BallotNumber ballot = new BallotNumber(-1, DEFAULT_SERVER_ID);
        	VoteCounter optionVoteCounter = new VoteCounter(option, quorumSize,
                    members.length, collator);
        	for (Member member : members) {
        		communicator.sendAcceptAsync(member, txnId, ballot,
                        option, optionVoteCounter);
        	}
        }

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
