package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.dao.CachedHBase;
import edu.ucsb.cs.mdcc.dao.Database;
import edu.ucsb.cs.mdcc.dao.TransactionRecord;
import org.apache.hadoop.hbase.util.Bytes;

public class TestDB {

    public static void main(String[] args) throws Exception {
        Bytes.toBytes((String) null);
        /*Database db = new CachedHBase();
        TransactionRecord record = db.getTransactionRecord("329d59c6-b8c8-46c8-b37e-ae14177369aa");
        System.out.println(record.getTransactionId());*/
    }
}
