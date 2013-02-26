package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.Option;

public interface VoteResult {
	public void Outcome(Option option, boolean accepted);
}
