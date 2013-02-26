package edu.ucsb.cs.mdcc.messaging;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.Iface;
import edu.ucsb.cs.mdcc.paxos.AgentService;

public class MDCCCommunicationServiceHandler implements Iface {

	private AgentService agent;

    public MDCCCommunicationServiceHandler(AgentService agent) {
        this.agent = agent;
    }

	public boolean ping() throws TException {
		// TODO Auto-generated method stub
		System.out.println("received ping");
		return true;
	}

	public boolean prepare(String object, BallotNumber version)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	public void decide(String transaction, boolean commit) throws TException {
		// TODO Auto-generated method stub
		agent.onDecide(transaction, commit);
	}

	public ReadValue read(String key) throws TException {
		// TODO Auto-generated method stub
		return agent.onRead(key);
	}

	public boolean accept(String transaction, String key, long oldVersion,
			BallotNumber ballot, ByteBuffer newValue) throws TException {
		return agent.onAccept(transaction, key, oldVersion, ballot, newValue);
	}

}
