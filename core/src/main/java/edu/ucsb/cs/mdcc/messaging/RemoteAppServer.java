package edu.ucsb.cs.mdcc.messaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.config.Member;

public class RemoteAppServer implements edu.ucsb.cs.mdcc.paxos.AppServerService {

    private static final Log log = LogFactory.getLog(RemoteAppServer.class);
	
	private Member member;
	
	public RemoteAppServer(Member member) {
		this.member = member;
	}
	
	//check whether an AppServer is up and reachable
	public boolean ping() {
        String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            MDCCAppServerService.Client client = getClient(transport);
            return client.ping();
        } catch (TException e) {
            handleException(host, e);
            return false;
        } finally {
            close(transport);
        }
    }

	public Result read(String key) {
		String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
        	MDCCAppServerService.Client client = getClient(transport);
            return toPaxosResult(key, client.read(key));
        } catch (TException e) {
            handleException(host, e);
            return null;
        } finally {
            close(transport);
        }
	}

	public boolean commit(String transactionId, Collection<edu.ucsb.cs.mdcc.Option> options) {
		String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
        	MDCCAppServerService.Client client = getClient(transport);
        	List<Option> tOptions = new ArrayList<Option>(options.size());
        	for(edu.ucsb.cs.mdcc.Option o : options) {
        		tOptions.add(toThriftOption(o));
        	}
            return client.commit(transactionId, tOptions);
        } catch (TException e) {
            handleException(host, e);
            return false;
        } finally {
            close(transport);
        }
	}

    public void stop() {

    }

    private MDCCAppServerService.Client getClient(
            TTransport transport) throws TTransportException {
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new MDCCAppServerService.Client(protocol);
    }
	
	private void close(TTransport transport) {
        if (transport.isOpen()) {
            transport.close();
        }
    }
	
	private void handleException(String target, Exception e) {
        String msg = "Error contacting the remote member: " + target;
        log.warn(msg, e);
    }
	
	private static Result toPaxosResult(String key, ReadValue r) {
		return new Result(key, r.getValue(), r.getVersion(),
                r.classicEndVersion >= r.getVersion());
	}
	
	private static Option toThriftOption(edu.ucsb.cs.mdcc.Option o) {
		return new Option(o.getKey(), o.getOldVersion(),
                ByteBuffer.wrap(o.getValue()), o.isClassic());
	}
	
}
