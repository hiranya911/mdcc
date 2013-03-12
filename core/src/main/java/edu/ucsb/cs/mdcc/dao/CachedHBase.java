package edu.ucsb.cs.mdcc.dao;

import edu.ucsb.cs.mdcc.util.LRUCache;

import java.util.Map;

public class CachedHBase extends HBase {

    private final LRUCache<String,Record> records = new RecordLRUCache<String, Record>(1000, this);
    private final LRUCache<String,TransactionRecord> transactions =
            new RecordLRUCache<String,TransactionRecord>(100, this);

    @Override
    public void put(Record record) {
        record.setDirty(false);
        super.put(record);
        records.put(record.getKey(), record);
    }

    public void weakPut(Record record) {
        record.setDirty(true);
        records.put(record.getKey(), record);
    }

    @Override
    public Record get(String key) {
        Record record = records.get(key);
        if (record == null) {
            synchronized (records) {
                record = records.get(key);
                if (record == null) {
                    record = super.get(key);
                    records.put(key, record);
                }
            }
        }
        return record;
    }

    @Override
    public void putTransactionRecord(TransactionRecord record) {
        record.setDirty(false);
        super.putTransactionRecord(record);
        transactions.put(record.getTransactionId(), record);
    }

    public void weakPutTransactionRecord(TransactionRecord record) {
        record.setDirty(true);
        transactions.put(record.getTransactionId(), record);
    }

    @Override
    public TransactionRecord getTransactionRecord(String transactionId) {
        TransactionRecord record = transactions.get(transactionId);
        if (record == null) {
            synchronized (transactions) {
                record = transactions.get(transactionId);
                if (record == null) {
                    record = super.getTransactionRecord(transactionId);
                    transactions.put(transactionId, record);
                }
            }
        }
        return record;
    }

    private static class RecordLRUCache<String,V extends Cacheable> extends LRUCache<String,V> {

        private Database db;

        public RecordLRUCache(int maxEntries, Database database) {
            super(maxEntries);
            this.db = database;
        }

        @Override
        protected boolean isRemovable(Map.Entry<String,V> eldest) {
            Cacheable value = eldest.getValue();
            if (value.isDirty()) {
                if (value instanceof Record) {
                    db.put((Record) value);
                } else if (value instanceof TransactionRecord) {
                    db.putTransactionRecord((TransactionRecord) value);
                }
            }
            return true;
        }
    }
}
