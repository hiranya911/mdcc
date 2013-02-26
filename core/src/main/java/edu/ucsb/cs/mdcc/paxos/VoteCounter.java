package edu.ucsb.cs.mdcc.paxos;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.AsyncClient.accept_call;

public class VoteCounter implements  AsyncMethodCallback<MDCCCommunicationService.AsyncClient.accept_call> {

	private AtomicInteger accepts = new AtomicInteger(0);
	private AtomicInteger rejects = new AtomicInteger(0);
    private AtomicBoolean outcomeReached = new AtomicBoolean(false);
	private VoteResult callback;
    private int acceptQuorum;
    private int numVoters;
    private Option myOption;
	
	public VoteCounter(Option option, int acceptQuorum, int numVoters, VoteResult callback) {
		this.callback = callback;
		this.acceptQuorum = acceptQuorum;
		this.numVoters = numVoters;
		this.myOption = option;
	}
	
	public void onComplete(accept_call response) {
        try {
            if (response.getResult()) {
                onAccept();
            } else {
                onReject();
            }
        } catch (TException e) {
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
