package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.Option;

public interface VoteResult {

	public void notifyOutcome(Option option, boolean accepted);

}
