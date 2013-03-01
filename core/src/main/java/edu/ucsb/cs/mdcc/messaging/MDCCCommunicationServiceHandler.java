package edu.ucsb.cs.mdcc.messaging;

import java.nio.ByteBuffer;
import java.util.Map;

import edu.ucsb.cs.mdcc.paxos.Accept;
import edu.ucsb.cs.mdcc.paxos.Prepare;
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

	public boolean prepare(String key, BallotNumber ballot, long classicEndVersion)
			throws TException {
        Prepare prepare = new Prepare(key, toPaxosBallot(ballot), classicEndVersion);
		return agent.onPrepare(prepare);
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
        Accept accept = new Accept(transaction, toPaxosBallot(ballot), key,
                oldVersion, newValue);
		return agent.onAccept(accept);
	}

	public Map<String, ReadValue> recover(Map<String, Long> versions)
			throws TException {
		return agent.onRecover(versions);
	}

	public boolean runClassic(String transaction, String key, long oldVersion,
			ByteBuffer newValue) throws TException {
		return agent.runClassic(transaction, key, oldVersion, newValue);
	}

    private edu.ucsb.cs.mdcc.paxos.BallotNumber toPaxosBallot(BallotNumber b) {
        return new edu.ucsb.cs.mdcc.paxos.BallotNumber(b.getNumber(), b.getProcessId());
    }
}
