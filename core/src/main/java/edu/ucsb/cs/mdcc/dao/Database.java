package edu.ucsb.cs.mdcc.dao;

import java.util.Collection;

public interface Database {

    public Record get(String key);

    public Collection<Record> getAll();

    public void put(Record record);

    public TransactionRecord getTransactionRecord(String transactionId);

    public void putTransactionRecord(TransactionRecord record);
}
