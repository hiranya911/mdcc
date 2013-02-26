package edu.ucsb.cs.mdcc.paxos;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import edu.ucsb.cs.mdcc.Option;

public class VoteCollator implements VoteResult {

    private Queue<Option> acceptedOptions = new ConcurrentLinkedQueue<Option>();
	private Queue<Option> rejectedOptions = new ConcurrentLinkedQueue<Option>();
	
	public int getAccepts() {
		return acceptedOptions.size();
	}
	
	public int getRejects() {
		return rejectedOptions.size();
	}

    public int getTotal() {
        return acceptedOptions.size() + rejectedOptions.size();
    }

	public void notifyOutcome(Option option, boolean accepted) {
		if (accepted) {
            acceptedOptions.add(option);
		} else {
            rejectedOptions.add(option);
		}

        synchronized(this) {
            this.notifyAll();
        }
    }
}
