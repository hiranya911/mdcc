package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.messaging.RemoteAppServer;
import edu.ucsb.cs.mdcc.paxos.AppServer;
import edu.ucsb.cs.mdcc.paxos.AppServerService;
import edu.ucsb.cs.mdcc.paxos.Transaction;

public class TransactionFactory {

    private boolean local;
    private AppServerService appServer;

    public TransactionFactory() {
        MDCCConfiguration config = MDCCConfiguration.getConfiguration();
        if (config.getAppServerUrl() == null) {
            this.local = true;
            this.appServer = new AppServer();
        } else {
        	String appServerURL = config.getAppServerUrl();
        	Member appServerMember = new Member(appServerURL, "AppServer", false);
        	this.appServer = new RemoteAppServer(appServerMember);
        }
    }

    public Transaction create() {
    	return new MDCCTransaction(appServer);
    }

    public void close() {
        appServer.stop();
    }

    public boolean isLocal() {
        return local;
    }
}
