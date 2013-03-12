package edu.ucsb.cs.mdcc.dao;

public abstract class Cacheable {

    public abstract boolean isDirty();

    public abstract void setDirty(boolean dirty);

}
