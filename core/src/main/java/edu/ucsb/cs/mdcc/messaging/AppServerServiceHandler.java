package edu.ucsb.cs.mdcc.messaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.thrift.TException;

import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.messaging.MDCCAppServerService.Iface;
import edu.ucsb.cs.mdcc.paxos.AppServerService;

public class AppServerServiceHandler implements Iface{
	
	private AppServerService appServer;

	public AppServerServiceHandler(AppServerService appServer) {
		this.appServer = appServer;
	}

	public boolean ping() throws TException {
		return true;
	}

	public ReadValue read(String key) throws TException {
		return toThriftReadValue(appServer.read(key));
	}

	public boolean commit(String transactionId, List<Option> options)
			throws TException {
		Collection<edu.ucsb.cs.mdcc.Option> mOptions = new ArrayList<edu.ucsb.cs.mdcc.Option>(options.size());
		for(Option o : options) {
			mOptions.add(toPaxosOption(o));
		}
		return appServer.commit(transactionId, mOptions);
	}
	
	private static ReadValue toThriftReadValue(Result r) {
		//This does not return the correct classicEndVersion, but it does allow the client to determine classic mode
		long classicEndVersion = (r.isClassic() ? r.getVersion() : r.getVersion() - 1);
		return new ReadValue(r.getVersion(), classicEndVersion, r.getValue());
	}
	
	private static edu.ucsb.cs.mdcc.Option toPaxosOption(Option o) {
		return new edu.ucsb.cs.mdcc.Option(o.getKey(), ByteBuffer.wrap(o.getValue()), o.getOldVersion(), o.isClassic());
	}
}
