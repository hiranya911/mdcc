package edu.ucsb.cs.mdcc.dao;

import java.io.IOException;     
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.ArrayList;     
import java.util.HashMap;
import java.util.List;     
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;     
import org.apache.hadoop.hbase.HBaseConfiguration;     
import org.apache.hadoop.hbase.HColumnDescriptor;     
import org.apache.hadoop.hbase.HTableDescriptor;     
import org.apache.hadoop.hbase.KeyValue;     
import org.apache.hadoop.hbase.MasterNotRunningException;     
import org.apache.hadoop.hbase.ZooKeeperConnectionException;     
import org.apache.hadoop.hbase.client.Delete;     
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

public class HBase implements Database{       
	
    private static Configuration conf =null;  
     /** 
      * config init 
     */  
     static {  
         conf = HBaseConfiguration.create();  
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
            for(int i=0; i<familys.length; i++){     
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
     * add record   
     */    
    public static void addRecord (String tableName, String rowKey, String family, String qualifier, String value)     
            throws Exception{     
        try {     
            HTable table = new HTable(conf, tableName);     
            Put put = new Put(rowKey.getBytes());     
            put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier), Bytes.toBytes(value));     
            table.put(put);     
            System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");   
            table.close();
        } catch (IOException e) {     
            e.printStackTrace();     
        }
    }

    /**   
     * add record overloaded  
     */ 
    private static void addRecord(String tableName, String rowKey, String family, String qualifier, 
    					byte[] optionCollectionBytes) throws Exception{
    	try {     
            HTable table = new HTable(conf, tableName);     
            Put put = new Put(rowKey.getBytes());     
            put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier), optionCollectionBytes);     
            table.put(put);     
            System.out.println("insert recored " + rowKey + " to table " + tableName +" ok."); 
            table.close();   
        } catch (IOException e) {     
            e.printStackTrace();     
        }
	}
    
    /**   
     * del record   
     */    
    public static void delRecord (String tableName, String rowKey) throws IOException{     
        HTable table = new HTable(conf, tableName);     
        List list = new ArrayList();     
        Delete del = new Delete(rowKey.getBytes());     
        list.add(del);     
        table.delete(list);
        table.close();
        System.out.println("del recored " + rowKey + " ok.");     
    }     

    /**   
     * get one   
     */    
    public static Result getOneRecord (String tableName, String rowKey) throws IOException{     
        HTable table = new HTable(conf, tableName);     
        Get get = new Get(rowKey.getBytes());     
        Result rs = table.get(get);
        if (rs.isEmpty()){
        	System.out.print("Not found!");  
        }
        for(KeyValue kv : rs.raw()){     
            System.out.print(new String(kv.getRow()) + " " );     
            System.out.print(new String(kv.getFamily()) + ":" );     
            System.out.print(new String(kv.getQualifier()) + " " );     
            System.out.print(kv.getTimestamp() + " " );     
            System.out.println(new String(kv.getValue()));     
        }
        table.close();
        return rs;
    }     

    /**   
     * get all   
     * @throws IOException 
     */    
    public static ResultScanner getAllRecord (String tableName) throws IOException {           
         HTable table = new HTable(conf, tableName);     
         Scan s = new Scan();     
         ResultScanner ss = table.getScanner(s);     
         for(Result r:ss){     
             for(KeyValue kv : r.raw()){     
                System.out.print(new String(kv.getRow()) + " ");     
                System.out.print(new String(kv.getFamily()) + ":");     
                System.out.print(new String(kv.getQualifier()) + " ");     
                System.out.print(kv.getTimestamp() + " ");     
                System.out.println(new String(kv.getValue()));     
             }     
         }     
         table.close();
         return ss;        
    }     

    public static void insertToRecords(String key, ByteBuffer value, Long version, Long classicEndVersion, 
    		BallotNumber ballot, Boolean prepared, String outstanding){
    	try{
    		HBase.addRecord("Records",key,"value", "", Bytes.toString(value.array())); 
    		HBase.addRecord("Records",key,"version","",Long.toString(version));
    		HBase.addRecord("Records",key,"classicEndVersion","",Long.toString(classicEndVersion));
    		HBase.addRecord("Records",key,"BallotNumber","",ballot.toString());
    		HBase.addRecord("Records",key,"prepared","",prepared.toString());        		
    		HBase.addRecord("Records",key,"outstanding","",outstanding);        		
    	}catch (Exception e) {     
            e.printStackTrace();     
        }
    }
    
    public static void insertToTxnRecords(String txn_id, Boolean complete, Collection<Option> options ){
    	try{
    		// add 'tex_id/complete'
    		HBase.addRecord("TxnRecords",txn_id,"complete","",complete.toString());
    		
    		//serialize Collection<Option>
    		Option[] optionArray = options.toArray(new Option[0]);
    		int arrayLength = optionArray.length;    		
    		ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
    		for (int i = 0; i < arrayLength; i++){
    			int optionBytesLength = optionArray[i].toBytes().array().length;
    			byte[] optionBytesLengthBytes = Bytes.toBytes(optionBytesLength);
    			outputStream.write( optionBytesLengthBytes);
    			outputStream.write( optionArray[i].toBytes().array());
    		}
    		byte[] optionCollectionBytes = outputStream.toByteArray( );
    		outputStream.close();
    		//add 'tex_id/Collection<Option>'
    		HBase.addRecord("TxnRecords",txn_id,"options","", optionCollectionBytes);
    	}catch (Exception e) {     
            e.printStackTrace();     
        }
    }
     
	/**   
     * implementes Database.java   
     */ 
    public void put(Record record){
    	insertToRecords(record.getKey(), record.getValue(), record.getVersion(), record.getClassicEndVersion(), 
    			record.getBallot(), record.isPrepared(), record.getOutstanding());
    }
    
    public void putTransactionRecord(TransactionRecord record){    	
    	insertToTxnRecords(record.getTransactionId(), record.isComplete(), record.getOptions());    	
    }
    
    public Record get(String key){
    	Result rs;
		Record rec = new Record(key);
		try {
			rs = HBase.getOneRecord("Records", key);
			if (rs.isEmpty()){
	        	System.out.print("Not found!"); 
	        }
			else{
				for(KeyValue kv : rs.raw()){  
		            String familyName = Bytes.toString(kv.getFamily());
		            String columnValue =  Bytes.toString(kv.getValue());
		            switch(familyName){
		            	case "value":  
		            		rec.setValue(ByteBuffer.wrap(columnValue.getBytes()));
		            		break;
		            	case "version":
		            		rec.setVersion(Long.valueOf(columnValue));
		            		break;
		            	case "classicEndVersion":
		            		rec.setClassicEndVersion(Long.valueOf(columnValue));
		            		break;
		            	case "BallotNumber":
		            		rec.setBallot(new BallotNumber(columnValue));
		            		break;
		            	case "prepared":
		            		rec.setPrepared(Boolean.valueOf(columnValue));
		            		break;
		            	case "outstanding":
		            		rec.setOutstanding(columnValue);
		            		break;
		            	default: 
		            		break;
		            }
				}
	        }
			return rec;
		} catch (IOException e) {
			e.printStackTrace();
			return rec;
		}
    }
    
    public Collection<Record> getAll(){
    	Map<String,Record> db = new HashMap<String, Record>();
    	try{     
            HTable table = new HTable(conf, "Records");     
            Scan s = new Scan();     
            ResultScanner ss = table.getScanner(s);     
            for(Result r:ss){     
                for(KeyValue kv : r.raw()){    
                	String key = new String(kv.getRow());
		            String familyName = Bytes.toString(kv.getFamily());
		            String columnValue =  Bytes.toString(kv.getValue());
		            if (!db.containsKey(key)){
		            	db.put(key, new Record(key));
		            }
	            	switch(familyName){
		            	case "value":  
		            		db.get(key).setValue(ByteBuffer.wrap(columnValue.getBytes()));
		            		break;
		            	case "version":
		            		db.get(key).setVersion(Long.valueOf(columnValue));
		            		break;
		            	case "classicEndVersion":
		            		db.get(key).setClassicEndVersion(Long.valueOf(columnValue));
		            		break;
		            	case "BallotNumber":
		            		db.get(key).setBallot(new BallotNumber(columnValue));
		            		break;
		            	case "prepared":
		            		db.get(key).setPrepared(Boolean.valueOf(columnValue));
		            		break;
		            	case "outstanding":
		            		db.get(key).setOutstanding(columnValue);
		            		break;
		            	default: 
		            		break;
		            }
                }     
            }
            table.close();
            return db.values();
       } catch (IOException e){     
           e.printStackTrace();  
           return db.values();
       }     
    }
    
    public TransactionRecord getTransactionRecord(String transactionId){
    	Result rs;
    	TransactionRecord rec = new TransactionRecord(transactionId);
		try {
			rs = HBase.getOneRecord("TxnRecords", transactionId);
			if (rs.isEmpty()){
	        	System.out.print("Not found!"); 
	        }
			else{
				for(KeyValue kv : rs.raw()){  
		            String familyName = Bytes.toString(kv.getFamily());		            
		            switch(familyName){
		            	case "complete":  
		            		String columnValue =  Bytes.toString(kv.getValue());
		            		rec.finish(Boolean.valueOf(columnValue));
		            		break;
		            	case "options":
		            	{
		            		byte[] valueBytes =  kv.getValue();
		            		boolean isLength = true;
		            		int optionBytesLength = 0;
		            		byte[] optionBytesLengthBytes = new byte[32];
		            		int lengthCount = 0;
		            		int optionBytesCount = 0;
		            		for (int i = 0; i < valueBytes.length ; i++) {
		            			if (isLength){
		            				lengthCount++;
			            			if (lengthCount < 32){
			            				optionBytesLengthBytes[lengthCount] = valueBytes[i];
			            			}
			            			else{
			            				optionBytesLength = Bytes.toInt(optionBytesLengthBytes);
			            				lengthCount = 0;
			            				isLength = false;
			            			}
		            			}
		            			else{
		            				byte[] optionBytes = new byte[optionBytesLength];
		            				optionBytesCount++;
		            				if (optionBytesCount < optionBytesLength){
		            					optionBytes[optionBytesCount] = valueBytes[i];
		            				}
		            				else{
		            					ByteBuffer serialized = ByteBuffer.wrap(optionBytes);
		            					Option newOption = new Option(serialized);
		            					rec.addOption(newOption);
		            					optionBytesCount = 0;
			            				isLength = true;
		            				}
		            			}
		            		}
		            	}
		            		break;
		            	default: 
		            		break;		            	
		            }
				}
	        }
			return rec;
		} catch (IOException e) {
			e.printStackTrace();
			return rec;
		}
    } 
}    