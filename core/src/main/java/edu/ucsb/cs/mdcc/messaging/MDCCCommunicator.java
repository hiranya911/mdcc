package edu.ucsb.cs.mdcc.messaging;

import edu.ucsb.cs.mdcc.MDCCException;
import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.paxos.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPool;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.server.TNonblockingServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MDCCCommunicator {

    private static final Log log = LogFactory.getLog(MDCCCommunicator.class);

    private ExecutorService exec;
    private TServer server;
    private TAsyncClientManager clientManager;

    private KeyedObjectPool<Member,TTransport> blockingPool =
            new StackKeyedObjectPool<Member, TTransport>(new ThriftConnectionPool());
    private KeyedObjectPool<Member,TNonblockingSocket> nonBlockingPool =
            new StackKeyedObjectPool<Member, TNonblockingSocket>(new ThriftNonBlockingConnectionPool());

    public MDCCCommunicator() {
        try {
            this.clientManager = new TAsyncClientManager();
        } catch (IOException e) {
            throw new MDCCException("Failed to initialize Thrift client manager");
        }
    }

    public void stopSender() {
        clientManager.stop();
    }
	
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

    public boolean runClassicPaxos(Member member, String transaction,
                                Option option, AsyncMethodCallback voting) {
        AsyncMethodCallbackDecorator callback = null;
        try {
            TNonblockingSocket socket = nonBlockingPool.borrowObject(member);
            callback = new AsyncMethodCallbackDecorator(voting, socket, member, nonBlockingPool);
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            MDCCCommunicationService.AsyncClient client =
                    new MDCCCommunicationService.AsyncClient(protocolFactory,
                            clientManager, socket);
            ByteBuffer value = ByteBuffer.wrap(option.getValue());
            client.runClassic(transaction, option.getKey(), option.getOldVersion(),
                    value, callback);
            return true;
        } catch (Exception e) {
            if (callback != null) {
                callback.onError(e);
            } else {
                voting.onError(e);
            }
            handleException(member.getHostName(), e);
            return false;
        }
    }
	
	public void sendAcceptAsync(Member member, edu.ucsb.cs.mdcc.paxos.Accept accept, PaxosVoteCounter voting) {
        AsyncMethodCallbackDecorator callback = null;
		try {
            TNonblockingSocket socket = nonBlockingPool.borrowObject(member);
            callback = new AsyncMethodCallbackDecorator(voting, socket, member, nonBlockingPool);
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            MDCCCommunicationService.AsyncClient client =
                    new MDCCCommunicationService.AsyncClient(
                            protocolFactory,
                            clientManager,
                            socket);
            client.accept(toThriftAccept(accept), callback);
        } catch (Exception e) {
            if (callback != null) {
                callback.onError(e);
            } else {
                voting.onError(e);
            }
            handleException(member.getHostName(), e);
        }
	}
	
	public void sendBulkAcceptAsync(Member member, List<edu.ucsb.cs.mdcc.paxos.Accept> fastAccepts, PaxosBulkVoteCounter fastCallback) {
        AsyncMethodCallbackDecorator callback = null;
		try {
            TNonblockingSocket socket = nonBlockingPool.borrowObject(member);
            callback = new AsyncMethodCallbackDecorator(fastCallback, socket, member, nonBlockingPool);
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            MDCCCommunicationService.AsyncClient client =
                    new MDCCCommunicationService.AsyncClient(
                            protocolFactory,
                            clientManager,
                            socket);
            ArrayList<edu.ucsb.cs.mdcc.messaging.Accept> tAccepts = new ArrayList<Accept>(fastAccepts.size());
            for (edu.ucsb.cs.mdcc.paxos.Accept accept : fastAccepts) {
            	tAccepts.add(toThriftAccept(accept));
            }
            client.bulkAccept( tAccepts, callback);
        } catch (Exception e) {
            if (callback != null) {
                callback.onError(e);
            } else {
                fastCallback.onError(e);
            }
            handleException(member.getHostName(), e);
        }
	}

    public void sendPrepareAsync(Member member, Prepare prepare, PaxosVoteCounter voting) {
        AsyncMethodCallbackDecorator callback = null;
        try {
            TNonblockingSocket socket = nonBlockingPool.borrowObject(member);
            callback = new AsyncMethodCallbackDecorator(voting, socket, member, nonBlockingPool);
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            MDCCCommunicationService.AsyncClient client =
                    new MDCCCommunicationService.AsyncClient(protocolFactory,
                            clientManager, socket);
            client.prepare(prepare.getKey(), toThriftBallot(prepare.getBallotNumber()),
                    prepare.getClassicEndVersion(), callback);
        } catch (Exception e) {
            if (callback != null) {
                callback.onError(e);
            } else {
                voting.onError(e);
            }
            handleException(member.getHostName(), e);
        }
    }

	public void sendRecoverAsync(Member member, Map<String,Long> versions, RecoverySet callback) {
        try {
			TNonblockingSocket socket = new TNonblockingSocket(member.getHostName(),
			member.getPort());
			TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
			MDCCCommunicationService.AsyncClient client =
			new MDCCCommunicationService.AsyncClient(protocolFactory,
			        clientManager, socket);
			client.recover(versions, callback);
		} catch (Exception e) {
			callback.onError(e);
		}
	}
	
	public boolean sendDecideAsync(Member member, String transaction, boolean commit) {
        AsyncMethodCallbackDecorator callback = null;
        try {
            TNonblockingSocket socket = nonBlockingPool.borrowObject(member);
            callback = new AsyncMethodCallbackDecorator(socket, member, nonBlockingPool);
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            MDCCCommunicationService.AsyncClient client =
                    new MDCCCommunicationService.AsyncClient(protocolFactory,
                            clientManager, socket);
            client.decide(transaction, commit, callback);
        } catch (Exception e) {
            if (callback != null) {
                callback.onError(e);
            }
            handleException(member.getHostName(), e);
        }
        return true;
    }
	
	public ReadValue get(Member member, String key) {
        TTransport transport = null;
        boolean error = false;
        try {
            transport = blockingPool.borrowObject(member);
            TProtocol protocol = new TBinaryProtocol(transport);
            MDCCCommunicationService.Client client = new MDCCCommunicationService.Client(protocol);
            return client.read(key);
        } catch (Exception e) {
            error = true;
            handleException(member.getHostName(), e);
            return null;
        } finally {
            if (transport != null) {
                try {
                    if (error) {
                        blockingPool.invalidateObject(member, transport);
                    } else {
                        blockingPool.returnObject(member, transport);
                    }
                } catch (Exception ignored) {
                }
            }
        }
	}
	
	private void handleException(String target, Exception e) {
        String msg = "Error contacting the remote member: " + target;
        log.warn(msg, e);
    }
    
    private BallotNumber toThriftBallot(edu.ucsb.cs.mdcc.paxos.BallotNumber b) {
        return new BallotNumber(b.getNumber(), b.getProcessId());
    }
    
    private Accept toThriftAccept(edu.ucsb.cs.mdcc.paxos.Accept accept) {
        ByteBuffer value = ByteBuffer.wrap(accept.getValue());
    	return new Accept(accept.getTransactionId(), toThriftBallot(accept.getBallotNumber()),
                accept.getKey(), accept.getOldVersion(), value);
    }
}
