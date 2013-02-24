package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.paxos.AppServer;
import edu.ucsb.cs.mdcc.paxos.Result;

import java.util.Collection;

public class LocalTransaction extends Transaction {

    private AppServer appServer;

    public LocalTransaction() {
        super();
        String[] hosts = { "localhost", "localhost" , "localhost", "localhost", "localhost"};
        int[] ports = { 7911, 7912, 7913, 7914, 7915 };
        String procId = "proc0";

        this.appServer = new AppServer(hosts, ports, procId);
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
