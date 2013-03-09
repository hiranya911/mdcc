package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.config.AppServerConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.messaging.AppServerServiceHandler;
import edu.ucsb.cs.mdcc.messaging.MDCCAppServerService;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;
import edu.ucsb.cs.mdcc.messaging.ReadValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

public class AppServer implements AppServerService {

    private static final Log log = LogFactory.getLog(AppServer.class);

	private AppServerConfiguration configuration;
    private MDCCCommunicator communicator;
    private TServer server;
    private ExecutorService exec;

    public AppServer() {
        this.configuration = AppServerConfiguration.getConfiguration();
        this.communicator = new MDCCCommunicator();
	}

    public void stop() {
        communicator.stopSender();
    }
    
	public Result read(String key) {
        Member[] members = configuration.getMembers(key);
        ReadValue r = null;
        int memberIndex = 0;
        while (r == null && memberIndex < members.length) {
        	r = communicator.get(members[memberIndex], key);
            memberIndex++;
        }
		if (r == null) {
			return null;
        } else {
            boolean classic = r.getClassicEndVersion() >= r.getVersion();
			return new Result(key, r.getValue(), r.getVersion(), classic);
		}
	}
	
	public boolean commit(String txnId, Collection<Option> options) {
		boolean success;
        FastPaxosVoteListener voteListener = new FastPaxosVoteListener(options,
                communicator, txnId);
        voteListener.start();

        synchronized (voteListener) {
            long start = System.currentTimeMillis();
        	while (voteListener.getTotal() < options.size()) {
                try {
					voteListener.wait(5000);
				} catch (InterruptedException ignored) {
				}

                if (System.currentTimeMillis() - start > 60000) {
                    log.warn("Transaction " + txnId + " timed out");
                    break;
                }
        	}
        }
        
        success = voteListener.getAccepts() == options.size();
        // No need to call commit for read-only txns
        if (options.size() > 0) {
            int shards = configuration.getShards();
            for (int i = 0; i < shards; i++) {
                Member[] members = configuration.getMembers(i);
                for (Member member : members) {
                    communicator.sendDecideAsync(member, txnId, success);
                }
            }
        }

        if (!success && log.isDebugEnabled()) {
            log.debug("Expected accepts: " + options.size() +
                    "; Received accepts: " + voteListener.getAccepts());
        }
        return success;
	}
    
	//start listener to handle incoming calls
    public void startListener() {
        exec = Executors.newSingleThreadExecutor();
        final AppServer appServer = this;
        String appServerURL = configuration.getAppServerUrl();
        if (appServerURL == null) {
        	log.error("AppServerURL not specified");
        	return;
        }
        final int port = Integer.parseInt(appServerURL.substring(appServerURL.indexOf(':') + 1));
        exec.submit(new Runnable() {
            public void run() {
                try {
                    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
                    MDCCAppServerService.Processor processor = new MDCCAppServerService.Processor(
                            new AppServerServiceHandler(appServer));
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
    	if (server != null) {
    		server.stop();
    	}
        exec.shutdownNow();
    }
	
	public static void main(String[] args) {
		final AppServer server = new AppServer();
		Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
            	log.info("Shutting Down AppServer");
            	server.stop();
            	server.stopListener();
            }
		});
		server.startListener();
	}

}
