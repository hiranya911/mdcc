package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;

import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;

public class AppServer {

	private int fastQuorum;
	MDCCConfiguration configuration;
    private String procId;
    private MDCCCommunicator communicator;
    private Member[] members;

	
    public AppServer(MDCCConfiguration configuration) {
		this.configuration = configuration;
		members = configuration.getMembers();
		procId = configuration.getMyProcId();
		int n = members.length;
		fastQuorum = n - (n / 4) + ((n + 1) % 2);
        this.communicator = new MDCCCommunicator();
	}
    
	public AppServer() {
		this(MDCCConfiguration.getConfiguration());
	}
	
	public Result read(String key) {
		String readString = communicator.get(members[0].getHostName(), members[0].getPort(), key);
		if (readString == null) {
			return null;
        } else {
        	long version = 0;
        	if (!readString.startsWith("|"))
        		version = Long.parseLong(readString.substring(0, readString.indexOf('|')));
			readString = readString.substring(readString.indexOf('|') + 1);
			readString = readString.substring(readString.indexOf('|') + 1);
			Result res = new Result(key, readString, version);
			return res;
		}
	}
	
	public boolean commit(String txnId, Collection<Option> options) {
		boolean success = true;
        for (Option option : options) {
            int accepts = 0;
            int rejects = 0;
            for(Member m : members) {
                BallotNumber ballot = new BallotNumber(-1, procId);
                if (communicator.sendAccept(m.getHostName(), m.getPort(), txnId, option.getKey(),
                        option.getOldVersion(), ballot, option.getValue().toString())) {
                    accepts++;
                } else {
                    rejects++;
                }

                if (members.length - rejects < fastQuorum)
                    break;
            }

            if (accepts < fastQuorum) {
                success = false;
                break;
            }
        }

        for (int i = 0; i < members.length; i++) {
            communicator.sendDecide(members[i].getHostName(),
                    members[i].getPort(), txnId, success);
        }
        return success;
	}

}
