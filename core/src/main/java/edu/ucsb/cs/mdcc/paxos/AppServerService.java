package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.Result;

public interface AppServerService {
	public Result read(String key);
	public boolean commit(String transactionId, Collection<Option> options);
}
