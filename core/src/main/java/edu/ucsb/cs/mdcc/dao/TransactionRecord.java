package edu.ucsb.cs.mdcc.dao;

import edu.ucsb.cs.mdcc.Option;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

public class TransactionRecord extends Cacheable {

    public static final int STATUS_UNDECIDED = 100;
    public static final int STATUS_COMMITTED = 101;
    public static final int STATUS_ABORTED = 102;

    private String transactionId;
    private int status = STATUS_UNDECIDED;
    private boolean dirty;
    private Set<Option> options = new TreeSet<Option>(new Comparator<Option>() {
        public int compare(Option o1, Option o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    });

    public TransactionRecord(String transactionId) {
        this.transactionId = transactionId;
    }

    public void addOption(Option option) {
        options.add(option);
    }

    public Collection<Option> getOptions() {
        return options;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public void finish(boolean commit) {
        if (commit) {
            status = STATUS_COMMITTED;
        } else {
            status = STATUS_ABORTED;
            options.clear();
        }
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }
}
