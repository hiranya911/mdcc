package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.Option;

public interface VoteResultListener {

	public void notifyOutcome(Option option, boolean accepted);

}
