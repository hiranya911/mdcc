package edu.ucsb.cs.mdcc.dao;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class InMemoryDatabase implements Database {

    private Map<String,Record> db = new HashMap<String, Record>();
    private Map<String,TransactionRecord> transactions = new HashMap<String, TransactionRecord>();

    public Record get(String key) {
        Record record = db.get(key);
        if (record == null) {
            record = new Record(key);
        }
        return record;
    }

    public void init() {

    }

    public void shutdown() {

    }

    public Collection<Record> getAll() {
        return db.values();
    }

    public void put(Record record) {
        db.put(record.getKey(), record);
    }

    public TransactionRecord getTransactionRecord(String transactionId) {
        TransactionRecord record = transactions.get(transactionId);
        if (record == null) {
            record = new TransactionRecord(transactionId);
        }
        return record;
    }

    public void putTransactionRecord(TransactionRecord record) {
        transactions.put(record.getTransactionId(), record);
    }
}
