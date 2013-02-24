package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;

import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;

public class AppServer {

	private int fastQuorum;
    private String[] hosts;
    private int[] ports;
    private String procId;
    private MDCCCommunicator communicator;
	
	public AppServer(String[] hosts, int[] ports, String procId) {
		this.hosts = hosts;
		this.ports = ports;
		int n = hosts.length;
		fastQuorum = n + 1 - (int)Math.ceil((double)n / 4.0) + ((n + 1) % 2);
		this.procId = procId;
        this.communicator = new MDCCCommunicator();
	}
	
	public Result read(String key) {
		String readString = communicator.get(hosts[0], ports[0], key);
		if (readString == null) {
			return null;
        } else {
        	long version = 0;
        	if (!readString.startsWith("|")) {
        		version = Long.parseLong(readString.substring(0, readString.indexOf('|')));
            }
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
            for (int i = 0; i < hosts.length; i++) {
                BallotNumber ballot = new BallotNumber(-1, procId);
                if (communicator.sendAccept(hosts[i], ports[i], txnId, option.getKey(),
                        option.getOldVersion(), ballot, option.getValue().toString())) {
                    accepts++;
                } else {
                    rejects++;
                }

                if (hosts.length - rejects < fastQuorum)
                    break;
            }

            if (accepts < fastQuorum) {
                success = false;
                break;
            }
        }

        for (int i = 0; i < hosts.length; i++) {
            communicator.sendDecide(hosts[i], ports[i], txnId, success);
        }
        return success;
	}

}
