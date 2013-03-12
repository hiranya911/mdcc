package edu.ucsb.cs.mdcc.dao;

import edu.ucsb.cs.mdcc.Option;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

public class TransactionRecord extends Cacheable {

    private String transactionId;
    private boolean complete = false;
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

    public boolean isComplete() {
        return complete;
    }

    public void finish(boolean commit) {
        this.complete = true;
        if (!commit) {
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
