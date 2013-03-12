package edu.ucsb.cs.mdcc.dao;

import java.util.Collection;

public interface Database {

    public static final String DELETE_VALUE_STRING = "__MDCC_DELETE__";
    public static final byte[] DELETE_VALUE = DELETE_VALUE_STRING.getBytes();

    public void init();

    public void shutdown();

    public Record get(String key);

    public Collection<Record> getAll();

    public void put(Record record);

    public void weakPut(Record record);

    public TransactionRecord getTransactionRecord(String transactionId);

    public void putTransactionRecord(TransactionRecord record);

    public void weakPutTransactionRecord(TransactionRecord record);
}
