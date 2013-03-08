package edu.ucsb.cs.mdcc.paxos;

import java.nio.ByteBuffer;
import java.util.Collection;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.dao.Database;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;
import edu.ucsb.cs.mdcc.messaging.ReadValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AppServer {

    private static final Log log = LogFactory.getLog(AppServer.class);

	private MDCCConfiguration configuration;
    private MDCCCommunicator communicator;

    public AppServer() {
        this.configuration = MDCCConfiguration.getConfiguration();
        this.communicator = new MDCCCommunicator();
	}

    public void stop() {
        this.communicator.stop();
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
			Result result = new Result(key, ByteBuffer.wrap(r.getValue()),
                    r.getVersion(), classic);
            String value = new String(r.getValue());
            if (Database.DELETE_VALUE.equals(value)) {
                result.setDeleted(true);
            }
            return result;
		}
	}
	
	public boolean commit(String txnId, Collection<Option> options) {
		boolean success;
        Member[] members = configuration.getMembers();
        
        FastPaxosVoteListener voteListener = new FastPaxosVoteListener(options,
                communicator, txnId);
        voteListener.start();

        synchronized (voteListener) {
            long start = System.currentTimeMillis();
        	while (voteListener.getTotal() < options.size()) {
                try {
					voteListener.wait(5000);
				} catch (InterruptedException ignored) {
				}

                if (System.currentTimeMillis() - start > 60000) {
                    log.warn("Transaction " + txnId + " timed out");
                    break;
                }
        	}
        }
        
        success = voteListener.getAccepts() == options.size();
        // No need to call commit for read-only txns
        if (options.size() > 0) {
            for (Member member : members) {
                communicator.sendDecideAsync(member, txnId, success);
            }
        }

        if (!success && log.isDebugEnabled()) {
            log.debug("Expected accepts: " + options.size() +
                    "; Received accepts: " + voteListener.getAccepts());
        }
        return success;
	}

}
