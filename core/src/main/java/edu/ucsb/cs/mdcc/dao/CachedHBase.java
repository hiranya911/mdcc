package edu.ucsb.cs.mdcc.dao;

import edu.ucsb.cs.mdcc.util.LRUCache;

public class CachedHBase extends HBase {

    private LRUCache<String,Record> records = new LRUCache<String, Record>(1000);
    private LRUCache<String,TransactionRecord> transactions =
            new LRUCache<String,TransactionRecord>(100);

    @Override
    public void put(Record record) {
        super.put(record);
        records.put(record.getKey(), record);
    }

    @Override
    public Record get(String key) {
        Record record = records.get(key);
        if (record == null) {
            record = super.get(key);
            records.put(key, record);
        }
        return record;
    }

    @Override
    public void putTransactionRecord(TransactionRecord record) {
        super.putTransactionRecord(record);
        transactions.put(record.getTransactionId(), record);
    }

    @Override
    public TransactionRecord getTransactionRecord(String transactionId) {
        TransactionRecord record = transactions.get(transactionId);
        if (record == null) {
            record = super.getTransactionRecord(transactionId);
            transactions.put(transactionId, record);
        }
        return record;
    }
}
