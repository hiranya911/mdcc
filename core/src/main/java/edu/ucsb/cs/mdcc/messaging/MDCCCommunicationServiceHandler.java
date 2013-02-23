package edu.ucsb.cs.mdcc.messaging;

import org.apache.thrift.TException;

import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.Iface;
import edu.ucsb.cs.mdcc.paxos.AgentService;

public class MDCCCommunicationServiceHandler implements Iface {

	private AgentService agent;

    public MDCCCommunicationServiceHandler(AgentService agent) {
        this.agent = agent;
    }
    
    @Override
	public boolean ping() throws TException {
		// TODO Auto-generated method stub
		System.out.println("received ping");
		return true;
	}

	@Override
	public boolean prepare(String object, RecordVersion version)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean accept(String transaction, String object,
			RecordVersion oldVersion, String processId, String newValue) throws TException {
		// TODO Auto-generated method stub
		return agent.onAccept(transaction, object, oldVersion, processId, newValue);
	}

	@Override
	public void decide(String transaction, boolean commit) throws TException {
		// TODO Auto-generated method stub
		agent.onDecide(transaction, commit);
	}

	@Override
	public String get(String object) throws TException {
		// TODO Auto-generated method stub
		return agent.onGet(object);
	}

}
