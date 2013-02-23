package edu.ucsb.cs.mdcc.messaging;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.server.TNonblockingServer;

import edu.ucsb.cs.mdcc.paxos.Agent;
import edu.ucsb.cs.mdcc.paxos.AgentService;
import edu.ucsb.cs.mdcc.paxos.FPAgent;

public class MDCCCommunicator {
	//protected final Log log = LogFactory.getLog(this.getClass());
	
	//start listener to handle incoming calls
    public void StartListener(AgentService agent, int port) {
        try {
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
            MDCCCommunicationService.Processor processor = new MDCCCommunicationService.Processor(
            		new MDCCCommunicationServiceHandler(agent));

            TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).
                    processor(processor));
            System.out.println("Starting server on port " + port + " ...");
            server.serve();
        } catch (TTransportException e) {
            //e.printStackTrace();
        	System.out.println(e.toString());

        }
    }

    //check whether a node is up and reachable
	public boolean ping(String hostName, int port) {
        //TTransport transport = new TSocket(hostName, port);
		TTransport transport = new TFramedTransport(new TSocket(hostName, port));
        try {
            edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.Client client = getClient(transport);
            return client.ping();
        } catch (TException e) {
            handleException(hostName, e);
            return false;
        } finally {
            close(transport);
        }
    }
	
	//send an accept message to another node, returns true if node accepts the proposal
	public boolean sendAccept(String hostName, int port, String transaction, String object, RecordVersion oldVersion, String processId, String value) {
		TTransport transport = new TFramedTransport(new TSocket(hostName, port));
        try {
            edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.Client client = getClient(transport);
            return client.accept(transaction, object, oldVersion, processId, value);
        } catch (TException e) {
            handleException(hostName, e);
            return false;
        } finally {
            close(transport);
        }
	}
	
	public boolean sendDecide(String hostName, int port, String transaction, boolean commit) {
		TTransport transport = new TFramedTransport(new TSocket(hostName, port));
        try {
            edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.Client client = getClient(transport);
            client.decide(transaction, commit);
            return true;
        } catch (TException e) {
            handleException(hostName, e);
            return false;
        } finally {
            close(transport);
        }
	}
	
	public String get(String hostName, int port, String object)
	{
		TTransport transport = new TFramedTransport(new TSocket(hostName, port));
        try {
            edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.Client client = getClient(transport);
            return client.get(object);
        } catch (TException e) {
            handleException(hostName, e);
            return null;
        } finally {
            close(transport);
        }
	}
	
	public void sendAcceptAsync(String hostName, int port, String transaction, String object, RecordVersion oldVersion, String processId, String value,
			AsyncMethodCallback<MDCCCommunicationService.AsyncClient.accept_call> callback) {

        try {
        	MDCCCommunicationService.AsyncClient client = new MDCCCommunicationService.
                    AsyncClient(new TBinaryProtocol.Factory(), new TAsyncClientManager(),
                                new TNonblockingSocket(hostName, port));

            client.accept(transaction, object, oldVersion, processId, value, callback);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	private MDCCCommunicationService.Client getClient(TTransport transport) throws TTransportException {
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
        //log.debug(msg, e);
        System.out.println(msg + e.toString());
    }
    
    //Main function for testing Communicator
    public static void main(String args[])
    {
        /*try {
            TServerSocket serverTransport = new TServerSocket(7911);

            MDCCCommunicationService.Processor processor = new MDCCCommunicationService.Processor(new MDCCCommunicationServiceHandler());

            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).
                    processor(processor));
            System.out.println("Starting server on port 7911 ...");
            server.serve();
        } catch (TTransportException e) {
            e.printStackTrace();
        }*/
    	MDCCCommunicator comms = new MDCCCommunicator();
    	comms.StartListener(new FPAgent(), 7911);
    }
}
