package edu.ucsb.cs.mdcc.dao;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;   
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import edu.ucsb.cs.mdcc.MDCCException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;     
import org.apache.hadoop.hbase.client.HTable;     
import org.apache.hadoop.hbase.client.Result;     
import org.apache.hadoop.hbase.client.ResultScanner;     
import org.apache.hadoop.hbase.client.Scan;     
import org.apache.hadoop.hbase.client.Put;     
import org.apache.hadoop.hbase.util.Bytes;     

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.paxos.BallotNumber;

public class HBase implements Database {

    private static final Log log = LogFactory.getLog(HBase.class);

    public static final String RECORDS_TABLE = "RECORDS";
    public static final String TRANSACTIONS_TABLE = "TRANSACTIONS";

    public static final String VALUE = "value";
    public static final String VERSION = "version";
    public static final String PREPARED = "prepared";
    public static final String CLASSIC_END_VERSION = "classicEndVersion";
    public static final String BALLOT_NUMBER = "ballotNumber";
    public static final String OUTSTANDING = "outstanding";

    public static final String COMPLETE = "complete";
    public static final String OPTIONS = "options";
	
    private static Configuration conf =null;

    public HBase() {
        try {
            Properties properties = new Properties();
            String configPath = System.getProperty("mdcc.config.dir", "conf");
            String hbasePath = System.getProperty("mdcc.hbase.dir", "hbase");
            File configFile = new File(configPath, "hbase.properties");
            properties.load(new FileInputStream(configFile));
            conf = HBaseConfiguration.create();
            File hbaseDir = new File(hbasePath, "data");
            conf.set(HConstants.HBASE_DIR, hbaseDir.getAbsolutePath());
            for (String key : properties.stringPropertyNames()) {
                if (key.startsWith("hbase.")) {
                    conf.set(key, properties.getProperty(key));
                } else {
                    String name = HConstants.ZK_CFG_PROPERTY_PREFIX + key;
                    conf.set(name, properties.getProperty(key));
                }
            }
        } catch (IOException e) {
            handleException("Error while initializing HBase client configuration", e);
        }
    }

    public void onStartup() {

    }

    /**
     * create table   
     */    
    public static void createTable(String tableName, String[] familys) throws Exception {     
        HBaseAdmin admin = new HBaseAdmin(conf);     
        if (admin.tableExists(tableName)) {     
            System.out.println("table already exists!");     
        } else {     
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);     
            for (int i=0; i<familys.length; i++){
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));     
            }     
            admin.createTable(tableDesc);     
            System.out.println("create table " + tableName + " ok.");     
        }      
        admin.close();
    }     

    /**   
     * drop table  
     */    
    public static void deleteTable(String tableName) throws Exception {     
       try {     
           HBaseAdmin admin = new HBaseAdmin(conf);     
           admin.disableTable(tableName);     
           admin.deleteTable(tableName);     
           System.out.println("delete table " + tableName + " ok.");           
           admin.close();
       } catch (MasterNotRunningException e) {     
           e.printStackTrace();     
       } catch (ZooKeeperConnectionException e) {     
           e.printStackTrace();     
       }     
    }     

    /**
     * implements Database.java
     */ 
    public void put(Record record){
        try {
            HTable table = new HTable(conf, RECORDS_TABLE);
            Put put = new Put(record.getKey().getBytes());
            put.add(Bytes.toBytes(VALUE),Bytes.toBytes(""),
                    Bytes.toBytes(record.getValue()));
            put.add(Bytes.toBytes(VERSION),Bytes.toBytes(""),
                    Bytes.toBytes(record.getVersion()));
            put.add(Bytes.toBytes(CLASSIC_END_VERSION),Bytes.toBytes(""),
                    Bytes.toBytes(record.getClassicEndVersion()));
            put.add(Bytes.toBytes(BALLOT_NUMBER),Bytes.toBytes(""),
                    Bytes.toBytes(record.getBallot().toString()));
            put.add(Bytes.toBytes(PREPARED),Bytes.toBytes(""),
                    Bytes.toBytes(record.isPrepared()));
            put.add(Bytes.toBytes(OUTSTANDING),Bytes.toBytes(""),
                    Bytes.toBytes(record.getOutstanding()));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void putTransactionRecord(TransactionRecord record){    	
    	try{    		
    		Collection<Option> options = record.getOptions();
    		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    		for (Option option : options){
                ByteBuffer byteBuffer = option.toBytes();
                int optionBytesLength = byteBuffer.array().length;
    			byte[] optionBytesLengthBytes = Bytes.toBytes(optionBytesLength);
    			outputStream.write(optionBytesLengthBytes);
    			outputStream.write(byteBuffer.array());
    		}
    		byte[] optionCollectionBytes = outputStream.toByteArray();
    		outputStream.close();
    		
    		HTable table = new HTable(conf, TRANSACTIONS_TABLE);
            Put put = new Put(record.getTransactionId().getBytes());    		
    		put.add(Bytes.toBytes(COMPLETE),Bytes.toBytes(""),
                    Bytes.toBytes(record.isComplete()));
    		put.add(Bytes.toBytes(OPTIONS),Bytes.toBytes(""),
    				optionCollectionBytes);
            table.put(put);
            table.close();
    	}catch (Exception e) {     
            e.printStackTrace();     
        } 	
    }
    
    public Record get(String key){
		Record rec = new Record(key);
		try {
			HTable table = new HTable(conf, RECORDS_TABLE);
	        Get get = new Get(key.getBytes());     
	        Result rs = table.get(get);
			if (!rs.isEmpty()){
                populateRecord(rec, rs);
            }
            table.close();
		} catch (IOException e) {
			handleException("Error while accessing HBase", e);
		}
		return rec;
    }

    private void populateRecord(Record rec, Result rs) {
        for (KeyValue kv : rs.raw()) {
            String familyName = Bytes.toString(kv.getFamily());
            byte[] columnValue = kv.getValue();
            if (VALUE.equals(familyName)) {
                rec.setValue(ByteBuffer.wrap(columnValue));
            } else if (VERSION.equals(familyName)) {
                rec.setVersion(Bytes.toLong(columnValue));
            } else if (CLASSIC_END_VERSION.equals(familyName)) {
                rec.setClassicEndVersion(Bytes.toLong(columnValue));
            } else if (BALLOT_NUMBER.equals(familyName)) {
                rec.setBallot(new BallotNumber(Bytes.toString(columnValue)));
            } else if (PREPARED.equals(familyName)) {
                rec.setPrepared(Bytes.toBoolean(columnValue));
            } else if (OUTSTANDING.equals(familyName)) {
                rec.setOutstanding(Bytes.toString(columnValue));
            }
        }
    }

    public Collection<Record> getAll(){
    	Map<String,Record> db = new HashMap<String, Record>();
    	try{     
            HTable table = new HTable(conf, RECORDS_TABLE);
            Scan scanner = new Scan();
            ResultScanner resultScanner = table.getScanner(scanner);
            for (Result result : resultScanner){
                for (KeyValue kv : result.raw()) {
                    String key = new String(kv.getRow());
                    Record record = db.get(key);
                    if (record == null) {
                        record = new Record(key);
                        db.put(key, record);
                    }
                    populateRecord(record, result);
                }     
            }
            table.close();
            return db.values();
       } catch (IOException e){     
            handleException("Error while accessing HBase", e);
            return null;
       }     
    }
    
    public TransactionRecord getTransactionRecord(String transactionId){
        TransactionRecord record = new TransactionRecord(transactionId);
    	try {
            HTable table = new HTable(conf, TRANSACTIONS_TABLE);
            Get get = new Get(transactionId.getBytes());
            Result rs = table.get(get);
			if (rs.isEmpty()){
                return record;
	        }
            for(KeyValue kv : rs.raw()){
                String familyName = Bytes.toString(kv.getFamily());
                if (COMPLETE.equals(familyName)){
                    String columnValue =  Bytes.toString(kv.getValue());
                    record.finish(Boolean.valueOf(columnValue));
                } else if(OPTIONS.equals(familyName)){
                    byte[] valueBytes =  kv.getValue();
                    boolean isLength = true;
                    int optionBytesLength = 0;
                    byte[] optionBytesLengthBytes = new byte[4];
                    int lengthCount = 0;
                    int optionBytesCount = 0;
                    byte[] optionBytes = null;
                    for (int i = 0; i < valueBytes.length ; i++) {
                        if (isLength){
                            lengthCount++;
                            if (lengthCount < 4){
                                optionBytesLengthBytes[lengthCount] = valueBytes[i];
                            } else {
                                optionBytesLength = Bytes.toInt(optionBytesLengthBytes);
                                optionBytes = new byte[optionBytesLength];
                                lengthCount = 0;
                                isLength = false;
                            }
                        } else {
                            optionBytesCount++;
                            if (optionBytesCount < optionBytesLength){
                                optionBytes[optionBytesCount] = valueBytes[i];
                            } else {
                                ByteBuffer serialized = ByteBuffer.wrap(optionBytes);
                                Option newOption = new Option(serialized);
                                record.addOption(newOption);
                                optionBytesCount = 0;
                                isLength = true;
                            }
                        }
                    }
                }
            }
            return record;
		} catch (IOException e) {
			handleException("Error while accessing HBase", e);
            return null;
		}
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new MDCCException(msg, e);
    }
}    