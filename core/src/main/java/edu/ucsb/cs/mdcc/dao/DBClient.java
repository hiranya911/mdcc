package edu.ucsb.cs.mdcc.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DBClient {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.setInt("hbase.master.port", 60000);
        conf.setInt("hbase.zookeeper.property.clientPort", 42181);
        String tableName = "MyTable";
        String[] familys = new String[] { "CF1", "CF2" };
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }

        String rowKey = "key3";

        /*try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes("CF1"), Bytes.toBytes("test"), Bytes
                            .toBytes("TEST VALUE"));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        try {
            HTable table = new HTable(conf, tableName);
            Get get = new Get(Bytes.toBytes("key3"));
            Result result = table.get(get);
            for (KeyValue kv : result.raw()){
                System.out.print(new String(kv.getRow()) + " " );
                System.out.print(new String(kv.getFamily()) + ":" );
                System.out.print(new String(kv.getQualifier()) + " " );
                System.out.print(kv.getTimestamp() + " " );
                System.out.println(new String(kv.getValue()));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
