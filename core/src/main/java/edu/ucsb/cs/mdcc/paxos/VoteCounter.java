package edu.ucsb.cs.mdcc.paxos;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.AsyncClient.accept_call;

public class VoteCounter implements  AsyncMethodCallback<MDCCCommunicationService.AsyncClient.accept_call> {

	private AtomicInteger accepts = new AtomicInteger(0);
	private AtomicInteger rejects = new AtomicInteger(0);
	private VoteResult callback;
	int acceptQuorum;
	int numVoters;
	Option myOption;
	
	public VoteCounter(Option option, int acceptQuroum, int numVoters, VoteResult callback) {
		this.callback = callback;
		this.acceptQuorum = acceptQuroum;
		this.numVoters = numVoters;
		this.myOption = option;
	}
	
	public int getAccepts() {
		return accepts.get();
	}
	
	public int getRejects() {
		return rejects.get();
	}
	
	@Override
	public void onComplete(accept_call response) {
		try {
			if (response.getResult()) {
				if (accepts.incrementAndGet() >= acceptQuorum)
					callback.Outcome(myOption, true);
			}
			else {
				if (rejects.incrementAndGet() > (numVoters - acceptQuorum))
					callback.Outcome(myOption, false);
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void onError(Exception exception) {
		if (rejects.incrementAndGet() > (numVoters - acceptQuorum))
			callback.Outcome(myOption, false);
	}
}
