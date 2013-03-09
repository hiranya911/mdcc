package edu.ucsb.cs.mdcc.paxos;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.AsyncClient.bulkAccept_call;

public class PaxosBulkVoteCounter implements AsyncMethodCallback {

    private static final Log log = LogFactory.getLog(PaxosBulkVoteCounter.class);

	private List<AtomicInteger> accepts = new ArrayList<AtomicInteger>();
	private List<AtomicInteger> rejects = new ArrayList<AtomicInteger>();
	private List<AtomicBoolean> outcomeFlags = new ArrayList<AtomicBoolean>();
	private VoteResultListener callback;
    private int classicQuorum;
    private int fastQuorum;
    private int numVoters;
    private List<Option> myOptions;
	
	public PaxosBulkVoteCounter(List<Option> options, VoteResultListener callback, int members) {
        this.numVoters = members;
		this.callback = callback;
		this.myOptions = options;
		this.classicQuorum = (numVoters / 2) + 1;
		this.fastQuorum = numVoters - (numVoters / 4) + ((numVoters + 1) % 2);
		
		accepts = new ArrayList<AtomicInteger>();
		rejects = new ArrayList<AtomicInteger>();
		outcomeFlags = new ArrayList<AtomicBoolean>();
		for (int i = 0; i < options.size(); i++) {
			accepts.add(new AtomicInteger(0));
			rejects.add(new AtomicInteger(0));
			outcomeFlags.add(new AtomicBoolean(false));
		}
	}
	
	public void onComplete(Object response) {
        if (response instanceof bulkAccept_call) {
        	try {
                List<Boolean> results = ((bulkAccept_call) response).getResult();
                for(int i = 0; i < results.size(); i++) {
                	if (results.get(i)) {
                		onAccept(i);
                	} else {
                		onReject(i);
                	}
                }
            } catch (TException e) {
            	for (int i = 0; i < myOptions.size(); i++) {
            		onReject(i);
            	}
            }
        }
	}

	public void onError(Exception exception) {
		for(int i = 0; i < myOptions.size(); i++) {
    		onReject(i);
    	}
	}

    private void onAccept(int i) {
        if (log.isDebugEnabled()) {
            log.debug("Key=" + myOptions.get(i).getKey() + " accept " + this.hashCode());
        }
        int acceptQuorum = (myOptions.get(i).isClassic() ? classicQuorum : fastQuorum);
        if (accepts.get(i).incrementAndGet() >= acceptQuorum &&
        		outcomeFlags.get(i).compareAndSet(false, true)) {
            callback.notifyOutcome(myOptions.get(i), true);
        }
    }

    private void onReject(int i) {
        if (log.isDebugEnabled()) {
            log.info("Key=" + myOptions.get(i).getKey() + " reject " + this.hashCode());
        }
        int acceptQuorum = (myOptions.get(i).isClassic() ? classicQuorum : fastQuorum);
        if (rejects.get(i).incrementAndGet() > (numVoters - acceptQuorum) &&
        		outcomeFlags.get(i).compareAndSet(false, true)) {
            callback.notifyOutcome(myOptions.get(i), false);
        }
    }
}
