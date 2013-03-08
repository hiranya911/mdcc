package edu.ucsb.cs.mdcc.dao;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;   
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import edu.ucsb.cs.mdcc.MDCCException;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.util.Utils;
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

    private static final String NULL = "NULL";
	
    private Configuration conf = null;
    private Map<String,HTable> tables = new HashMap<String, HTable>();

    public HBase() {
        try {
            Properties properties = new Properties();
            String configPath = System.getProperty("mdcc.config.dir", "conf");
            String hbasePath = System.getProperty("mdcc.hbase.dir", "hbase");
            File configFile = new File(configPath, "hbase.properties");
            properties.load(new FileInputStream(configFile));
            int myId = MDCCConfiguration.getConfiguration().getMyId();
            Utils.incrementPort(properties, "clientPort", myId);
            Utils.incrementPort(properties, HConstants.MASTER_PORT, myId);
            Utils.incrementPort(properties, HConstants.REGIONSERVER_PORT, myId);
            Utils.rewriteQuorumPorts(properties, myId);
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

    /**
     * create table   
     */    
    private void createTableIfNotExists(String tableName, String[] families) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);     
        if (admin.tableExists(tableName)) {     
            log.debug("table already exists!");
        } else {     
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);     
            for (int i = 0; i < families.length; i++){
                tableDesc.addFamily(new HColumnDescriptor(families[i]));
            }     
            admin.createTable(tableDesc);     
            log.info("create table " + tableName + " ok.");
        }      
        admin.close();

        tables.put(tableName, new HTable(conf,  tableName));
        log.info("Opened table: " + tableName);
    }

    public void init() {
    	String[] recordFamilies = {
                VALUE, VERSION, PREPARED, CLASSIC_END_VERSION,
                BALLOT_NUMBER, OUTSTANDING
        };
    	String[] transactionFamilies = { COMPLETE, OPTIONS };
    	try {
			createTableIfNotExists(RECORDS_TABLE, recordFamilies);
	    	createTableIfNotExists(TRANSACTIONS_TABLE, transactionFamilies);
		} catch (Exception e) {
			handleException("Error while creating tables", e);
		}    
    }

    public void shutdown() {
        for (HTable table : tables.values()) {
            try {
                table.close();
            } catch (IOException e) {
                log.warn("Error while closing table: " +
                        Bytes.toString(table.getTableName()), e);
            }
        }
        tables.clear();
    }

    public void put(Record record){
        try {
            HTable table = tables.get(RECORDS_TABLE);
            Put put = new Put(record.getKey().getBytes());
            put.add(Bytes.toBytes(VALUE),Bytes.toBytes(""), record.getValue());
            put.add(Bytes.toBytes(VERSION),Bytes.toBytes(""),
                    Bytes.toBytes(record.getVersion()));
            put.add(Bytes.toBytes(CLASSIC_END_VERSION),Bytes.toBytes(""),
                    Bytes.toBytes(record.getClassicEndVersion()));
            put.add(Bytes.toBytes(BALLOT_NUMBER),Bytes.toBytes(""),
                    Bytes.toBytes(record.getBallot().toString()));
            put.add(Bytes.toBytes(PREPARED),Bytes.toBytes(""),
                    Bytes.toBytes(record.isPrepared()));
            if (record.getOutstanding() != null) {
                put.add(Bytes.toBytes(OUTSTANDING),Bytes.toBytes(""),
                        Bytes.toBytes(record.getOutstanding()));
            } else {
                put.add(Bytes.toBytes(OUTSTANDING),Bytes.toBytes(""),
                        Bytes.toBytes(NULL));
            }
            table.put(put);
        } catch (IOException e) {
            handleException("Error while accessing HBase", e);
        }
    }
    
    public void putTransactionRecord(TransactionRecord record){    	
    	try {
    		Collection<Option> options = record.getOptions();
    		byte[] optionCollectionBytes = Utils.serialize(options);
    		
    		HTable table = tables.get(TRANSACTIONS_TABLE);
            Put put = new Put(record.getTransactionId().getBytes());    		
    		put.add(Bytes.toBytes(COMPLETE),Bytes.toBytes(""),
                    Bytes.toBytes(record.isComplete()));
    		put.add(Bytes.toBytes(OPTIONS),Bytes.toBytes(""),
    				optionCollectionBytes);
            table.put(put);
    	} catch (Exception e) {
            handleException("Error while accessing HBase", e);
        } 	
    }
    
    public Record get(String key){
		Record rec = new Record(key);
		try {
            HTable table = tables.get(RECORDS_TABLE);
	        Get get = new Get(key.getBytes());     
	        Result rs = table.get(get);
			if (!rs.isEmpty()){
                populateRecord(rec, rs);
            }
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
                rec.setValue(columnValue);
            } else if (VERSION.equals(familyName)) {
                rec.setVersion(Bytes.toLong(columnValue));
            } else if (CLASSIC_END_VERSION.equals(familyName)) {
                rec.setClassicEndVersion(Bytes.toLong(columnValue));
            } else if (BALLOT_NUMBER.equals(familyName)) {
                rec.setBallot(new BallotNumber(Bytes.toString(columnValue)));
            } else if (PREPARED.equals(familyName)) {
                rec.setPrepared(Bytes.toBoolean(columnValue));
            } else if (OUTSTANDING.equals(familyName)) {
                String outstanding = Bytes.toString(columnValue);
                if (NULL.equals(outstanding)) {
                    rec.setOutstanding(null);
                } else {
                    rec.setOutstanding(outstanding);
                }
            }
        }
    }

    public Collection<Record> getAll(){
    	Map<String,Record> db = new HashMap<String, Record>();
    	try{
            HTable table = tables.get(RECORDS_TABLE);
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
            return db.values();
       } catch (IOException e){     
            handleException("Error while accessing HBase", e);
            return null;
       }     
    }
    
    public TransactionRecord getTransactionRecord(String transactionId){
        TransactionRecord record = new TransactionRecord(transactionId);
    	try {
            HTable table = tables.get(TRANSACTIONS_TABLE);
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
                    Collection<Option> options = Utils.deserialize(valueBytes);
                    for (Option option : options) {
                        record.addOption(option);
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