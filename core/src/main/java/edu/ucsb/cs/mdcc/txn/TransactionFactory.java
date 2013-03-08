package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.MDCCException;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.paxos.AppServer;
import edu.ucsb.cs.mdcc.paxos.Transaction;

public class TransactionFactory {

    private boolean local;
    private AppServer appServer;

    public TransactionFactory() {
        MDCCConfiguration config = MDCCConfiguration.getConfiguration();
        if (config.getAppServerUrl() == null) {
            this.local = true;
            this.appServer = new AppServer();
        }
    }

    public Transaction create() {
        if (local) {
            return new LocalTransaction(appServer);
        } else {
            throw new MDCCException("Not yet implemented");
        }
    }

    public void close() {
        if (appServer != null) {
            appServer.stop();
        }
    }

    public boolean isLocal() {
        return local;
    }
}
