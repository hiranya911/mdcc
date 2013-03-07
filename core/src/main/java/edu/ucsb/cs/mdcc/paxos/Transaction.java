package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.dao.Database;

import java.nio.ByteBuffer;
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

    public synchronized ByteBuffer read(String key) throws TransactionException {
        assertState();

        if (readSet.containsKey(key)) {
            Result result = readSet.get(key);
            if (result.isDeleted()) {
                throw new TransactionException("No object exists by the key: " + key);
            }
            if (result.getVersion() == 0) {
                throw new TransactionException("No object exists by the key: " + key);
            }
            return result.getValue();
        } else {
            Result result = doRead(key);
            if (result != null) {
                readSet.put(result.getKey(), result);
                if (result.isDeleted()) {
                    throw new TransactionException("No object exists by the key: " + key);
                }

                if (result.getVersion() == 0) {
                    throw new TransactionException("No object exists by the key: " + key);
                }
                return result.getValue();
            } else {
                throw new TransactionException("No object exists by the key: " + key);
            }
        }
    }

    public synchronized void delete(String key) throws TransactionException {
        assertState();

        Option option;
        Result result = readSet.get(key);
        if (result != null) {
            // We have already read this object.
            // Update the value in the read-set so future reads can see this write.
            if (result.isDeleted()) {
                throw new TransactionException("Object already deleted: " + key);
            }
            result.setDeleted(true);
            option = new Option(key, ByteBuffer.wrap(Database.DELETE_VALUE.getBytes()),
                    result.getVersion(), result.isClassic());
        } else {
            result = doRead(key);
            if (result == null) {
                // Object doesn't exist in the DB - Error!
                throw new TransactionException("Unable to delete non existing object: " + key);
            } else {
                // Object exists in the DB.
                // Update the value and add to the read-set so future reads can
                // see this write.
                result.setDeleted(true);
            }
            option = new Option(key, ByteBuffer.wrap(Database.DELETE_VALUE.getBytes()),
                    result.getVersion(), result.isClassic());
            readSet.put(result.getKey(), result);
        }
        writeSet.put(key, option);
    }

    public synchronized void write(String key, ByteBuffer value) throws TransactionException {
        assertState();

        Option option;
        Result result = readSet.get(key);
        if (result != null) {
            // We have already read this object.
            // Update the value in the read-set so future reads can see this write.
            option = new Option(key, value, result.getVersion(), result.isClassic());
            result.setValue(value);
        } else {
            // We haven't read this object before (blind write).
            // Do an implicit read from the database.
            result = doRead(key);
            if (result == null) {
                // Object doesn't exist in the DB - Insert (version = 0)
                result = new Result(key, value, (long) 0, false);
            } else {
                // Object exists in the DB.
                // Update the value and add to the read-set so future reads can
                // see this write.
                result.setValue(value);
            }
            option = new Option(key, value, result.getVersion(), result.isClassic());
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
