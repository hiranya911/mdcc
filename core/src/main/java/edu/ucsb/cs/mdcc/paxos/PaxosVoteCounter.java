package edu.ucsb.cs.mdcc.paxos;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.AsyncClient.accept_call;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.AsyncClient.prepare_call;

public class PaxosVoteCounter implements AsyncMethodCallback {

	private AtomicInteger accepts = new AtomicInteger(0);
	private AtomicInteger rejects = new AtomicInteger(0);
    private AtomicBoolean outcomeReached = new AtomicBoolean(false);
	private VoteResultListener callback;
    private int acceptQuorum;
    private int numVoters;
    private Option myOption;
	
	public PaxosVoteCounter(Option option, VoteResultListener callback) {
        MDCCConfiguration config = MDCCConfiguration.getConfiguration();
        this.numVoters = config.getMembers().length;
		this.callback = callback;
        if (option.isClassic()) {
            this.acceptQuorum = (numVoters / 2) + 1;
        } else {
		    this.acceptQuorum = numVoters - (numVoters / 4) + ((numVoters + 1) % 2);
        }
		this.myOption = option;
	}
	
	public void onComplete(Object response) {
        boolean result = false;
        if (response instanceof accept_call) {
            try {
                result = ((accept_call) response).getResult();
            } catch (TException e) {
                onReject();
            }
        } else if (response instanceof prepare_call) {
            try {
                result = ((prepare_call) response).getResult();
            } catch (TException e) {
                onReject();
            }
        } else {
            return;
        }

        if (result) {
            onAccept();
        } else {
            onReject();
        }
	}

	public void onError(Exception exception) {
		onReject();
	}

    private void onAccept() {
        if (accepts.incrementAndGet() >= acceptQuorum &&
                outcomeReached.compareAndSet(false, true)) {
            callback.notifyOutcome(myOption, true);
        }
    }

    private void onReject() {
        if (rejects.incrementAndGet() > (numVoters - acceptQuorum) &&
                outcomeReached.compareAndSet(false, true)) {
            callback.notifyOutcome(myOption, false);
        }
    }
}
