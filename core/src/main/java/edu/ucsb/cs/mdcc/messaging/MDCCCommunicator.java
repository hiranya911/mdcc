package edu.ucsb.cs.mdcc.messaging;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.config.Member;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.server.TNonblockingServer;

import edu.ucsb.cs.mdcc.paxos.AgentService;
import edu.ucsb.cs.mdcc.paxos.VoteCounter;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MDCCCommunicator {

    private static final Log log = LogFactory.getLog(MDCCCommunicator.class);

    private ExecutorService exec;
    private TServer server;
	
	//start listener to handle incoming calls
    public void startListener(final AgentService agent, final int port) {
        exec = Executors.newSingleThreadExecutor();
        exec.submit(new Runnable() {
            public void run() {
                try {
                    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
                    MDCCCommunicationService.Processor processor = new MDCCCommunicationService.Processor(
                            new MDCCCommunicationServiceHandler(agent));
                    server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).
                            processor(processor));
                    log.info("Starting server on port: " + port);
                    server.serve();
                } catch (TTransportException e) {
                    log.error("Error while initializing the Thrift service", e);
                }
            }
        });
    }

    public void stopListener() {
        server.stop();
        exec.shutdownNow();
    }

    //check whether a node is up and reachable
	public boolean ping(Member member) {
        String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            MDCCCommunicationService.Client client = getClient(transport);
            return client.ping();
        } catch (TException e) {
            handleException(host, e);
            return false;
        } finally {
            close(transport);
        }
    }
	
	//send an accept message to another node, returns true if node accepts the proposal
	public boolean sendAccept(Member member, String transaction, BallotNumber ballot, Option option) {
        String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            MDCCCommunicationService.Client client = getClient(transport);
            return client.accept(transaction, option.getKey(),
                    option.getOldVersion(), ballot, option.getValue().toString());
        } catch (TException e) {
            handleException(host, e);
            return false;
        } finally {
            close(transport);
        }
	}
	
	public void sendAcceptAsync(Member member, String transaction, BallotNumber ballot, Option option, VoteCounter voting) {
		try {
			MDCCCommunicationService.AsyncClient client = new MDCCCommunicationService.
                    AsyncClient(new TBinaryProtocol.Factory(), new TAsyncClientManager(),
                                new TNonblockingSocket(member.getHostName(), member.getPort()));

            client.accept(transaction, option.getKey(),
                    option.getOldVersion(), ballot, option.getValue().toString(), voting);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean sendDecide(Member member, String transaction, boolean commit) {
        String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            MDCCCommunicationService.Client client = getClient(transport);
            client.decide(transaction, commit);
            return true;
        } catch (TException e) {
            handleException(host, e);
            return false;
        } finally {
            close(transport);
        }
	}
	
	public String get(Member member, String object) {
        String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            MDCCCommunicationService.Client client = getClient(transport);
            return client.get(object);
        } catch (TException e) {
            handleException(host, e);
            return null;
        } finally {
            close(transport);
        }
	}
	
	private MDCCCommunicationService.Client getClient(
            TTransport transport) throws TTransportException {
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new MDCCCommunicationService.Client(protocol);
    }

    private void close(TTransport transport) {
        if (transport.isOpen()) {
            transport.close();
        }
    }

    private void handleException(String target, TException e) {
        String msg = "Error contacting the remote member: " + target;
        log.debug(msg, e);
    }

}
