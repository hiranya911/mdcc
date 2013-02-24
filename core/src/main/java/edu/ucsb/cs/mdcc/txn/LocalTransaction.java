package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.paxos.AppServer;
import edu.ucsb.cs.mdcc.paxos.Result;

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

    @Override
    protected Result doRead(String key) {
        return null;
    }

    @Override
    protected void doCommit(String transactionId, Collection<Option> options) throws TransactionException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
