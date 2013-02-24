package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public abstract class Transaction {

    protected String transactionId;
    protected boolean complete;

    private Map<String,Result> readSet = new HashMap<String, Result>();
    protected Map<String,Option> writeSet = new HashMap<String, Option>();

    public void begin() {
        this.transactionId = UUID.randomUUID().toString();
    }

    public synchronized Object read(String key) throws TransactionException {
        assertState();

        if (readSet.containsKey(key)) {
            return readSet.get(key).getValue();
        } else {
            Result result = doRead(key);
            if (result != null) {
                readSet.put(result.getKey(), result);
                return result.getValue();
            } else {
                throw new TransactionException("No object exists by the key: " + key);
            }
        }
    }

    public synchronized void write(String key, Object value) throws TransactionException {
        assertState();

        Option option;
        Result result = readSet.get(key);
        if (result != null) {
            // We have already read this object.
            // Update the value in the read-set so future reads can see this write.
            option = new Option(key, value, result.getVersion());
            result.setValue(value);
        } else {
            // We haven't read this object before (blind write).
            // Do an implicit read from the database.
            result = doRead(key);
            if (result == null) {
                // Object doesn't exist in the DB - Insert (version = 0)
                result = new Result(key, value, (long)0);
            } else {
                // Object exists in the DB.
                // Update the value and add to the read-set so future reads can
                // see this write.
                result.setValue(value);
            }
            option = new Option(key, value, result.getVersion());
            readSet.put(result.getKey(), result);
        }
        writeSet.put(key, option);
    }

    public synchronized void commit() throws TransactionException {
        assertState();
        try {
            doCommit(transactionId, writeSet.values());
        } finally {
            this.complete = true;
        }
    }

    private void assertState() throws TransactionException {
        if (this.transactionId == null) {
            throw new TransactionException("Read operation invoked before begin");
        } else if (this.complete) {
            throw new TransactionException("Attempted operation on completed transaction");
        }
    }

    protected abstract Result doRead(String key);

    protected abstract void doCommit(String transactionId,
                                     Collection<Option> options) throws TransactionException;

}
