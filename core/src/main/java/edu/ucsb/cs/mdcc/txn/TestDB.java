package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.dao.*;
import org.apache.hadoop.hbase.util.Bytes;

public class TestDB {

    public static void main(String[] args) throws Exception {
        Database db = new HBase();
        db.onStartup();
        Record record = db.get("Y");
        System.out.println(record.getOutstanding());
        System.out.println(record.getVersion());
        db.onShutdown();
    }
}
