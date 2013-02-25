package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;

public class AppServer {

	private int quorumSize;
	private MDCCConfiguration configuration;
    private MDCCCommunicator communicator;

    public AppServer(MDCCConfiguration configuration) {
        int n = configuration.getMembers().length;
        this.configuration = configuration;
		this.quorumSize = n - (n / 4) + ((n + 1) % 2);
        this.communicator = new MDCCCommunicator();
	}
    
	public AppServer() {
		this(MDCCConfiguration.getConfiguration());
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
		boolean success = true;
        Member[] members = configuration.getMembers();
        String serverId = configuration.getServerId();
        for (Option option : options) {
            int accepts = 0;
            int rejects = 0;
            for (Member member : members) {
                BallotNumber ballot = new BallotNumber(-1, serverId);
                if (communicator.sendAccept(member, txnId, ballot, option)) {
                    accepts++;
                } else {
                    rejects++;
                }

                if (members.length - rejects < quorumSize) {
                    break;
                }
            }

            if (accepts < quorumSize) {
                success = false;
                break;
            }
        }

        for (Member member : members) {
            communicator.sendDecide(member, txnId, success);
        }
        return success;
	}

}
