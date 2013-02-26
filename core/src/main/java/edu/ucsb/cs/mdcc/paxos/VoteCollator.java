package edu.ucsb.cs.mdcc.paxos;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.ucsb.cs.mdcc.Option;

public class VoteCollator implements VoteResult {
	private AtomicInteger accepts = new AtomicInteger(0);
	private AtomicInteger rejects = new AtomicInteger(0);
	private List<Option> acceptedOptions = new LinkedList<Option>();
	private List<Option> rejectedOptions = new LinkedList<Option>();
	
	public VoteCollator() {
		
	}
	
	public int getAccepts() {
		return accepts.get();
	}
	
	public int getRejects() {
		return rejects.get();
	}

	@Override
	public void Outcome(Option option, boolean accepted) {
		if (accepted)
		{
			synchronized(acceptedOptions) {
				acceptedOptions.add(option);
			}
			accepts.incrementAndGet();
			synchronized(this) {
				this.notifyAll();
			}
		} else {
			synchronized(acceptedOptions) {
				rejectedOptions.add(option);
			}
			rejects.incrementAndGet();
			synchronized(this) {
				this.notifyAll();
			}
		}
	}
}
