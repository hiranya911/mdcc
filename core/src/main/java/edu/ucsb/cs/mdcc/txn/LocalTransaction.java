package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.paxos.*;

import java.util.Collection;
import java.util.Properties;

public class LocalTransaction extends Transaction {

    private AppServer appServer;

    public LocalTransaction() {
        super();
        Properties prop = new Properties();
        prop.setProperty("mdcc.myid", "local");
        prop.setProperty("mdcc.server.node1", "localhost:7911");
        this.appServer = new AppServer(new MDCCConfiguration(prop));
    }

    protected Result doRead(String key) {
        return appServer.read(key);
    }

    @Override
    protected void doCommit(String transactionId,
                            Collection<Option> options) throws TransactionException {
        boolean success = appServer.commit(transactionId, options);
        if (!success) {
            throw new TransactionException("Failed to commit txn: " + transactionId);
        }
    }
}
