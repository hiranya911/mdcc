package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.paxos.*;

import java.util.Collection;
import java.util.Properties;

public class MDCCTransaction extends Transaction {

    private AppServerService appServer;

    public MDCCTransaction(AppServerService appServer) {
        super();
        this.appServer = appServer;
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
