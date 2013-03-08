package edu.ucsb.cs.mdcc.messaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.Iface;
import edu.ucsb.cs.mdcc.paxos.AgentService;

public class MDCCCommunicationServiceHandler implements Iface {

	private AgentService agent;

    public MDCCCommunicationServiceHandler(AgentService agent) {
        this.agent = agent;
    }

	public boolean ping() throws TException {
		System.out.println("received ping");
		return true;
	}

	public boolean prepare(String key, BallotNumber ballot, long classicEndVersion)
			throws TException {
		edu.ucsb.cs.mdcc.paxos.Prepare prepare = new edu.ucsb.cs.mdcc.paxos.Prepare(
                key, toPaxosBallot(ballot), classicEndVersion);
		return agent.onPrepare(prepare);
	}

	public void decide(String transaction, boolean commit) throws TException {
		agent.onDecide(transaction, commit);
	}

	public ReadValue read(String key) throws TException {
		return agent.onRead(key);
	}

	public boolean accept(Accept a) throws TException {
		edu.ucsb.cs.mdcc.paxos.Accept accept = toPaxosAccept(a);
		return agent.onAccept(accept);
	}
	
	public List<Boolean> bulkAccept(List<Accept> accepts) throws TException {
		List<Boolean> responses = new ArrayList<Boolean>(accepts.size());
		for (Accept accept : accepts) {
			responses.add(agent.onAccept(toPaxosAccept(accept)));
		}
		return responses;
	}

	public Map<String, ReadValue> recover(Map<String, Long> versions)
			throws TException {
		return agent.onRecover(versions);
	}

	public boolean runClassic(String transaction, String key, long oldVersion,
			ByteBuffer newValue) throws TException {
        ByteBuffer slice = newValue.slice();
        byte[] data = new byte[slice.limit()];
        slice.get(data);
		return agent.runClassic(transaction, key, oldVersion, data);
	}

    private edu.ucsb.cs.mdcc.paxos.BallotNumber toPaxosBallot(BallotNumber b) {
        return new edu.ucsb.cs.mdcc.paxos.BallotNumber(b.getNumber(), b.getProcessId());
    }
    
    private edu.ucsb.cs.mdcc.paxos.Accept toPaxosAccept(Accept a) {
    	return new edu.ucsb.cs.mdcc.paxos.Accept(a.getTransactionId(),
                toPaxosBallot(a.getBallot()), a.getKey(), a.getOldVersion(),
                a.getNewValue());
    }
}
