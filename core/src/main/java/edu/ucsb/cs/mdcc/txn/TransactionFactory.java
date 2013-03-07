package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.MDCCException;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.paxos.Transaction;

public class TransactionFactory {

    private boolean local;

    public TransactionFactory() {
        MDCCConfiguration config = MDCCConfiguration.getConfiguration();
        if (config.getAppServerUrl() == null) {
            this.local = true;
        }
    }

    public Transaction create() {
        if (local) {
            return new LocalTransaction();
        } else {
            throw new MDCCException("Not yet implemented");
        }
    }

    public boolean isLocal() {
        return local;
    }
}
