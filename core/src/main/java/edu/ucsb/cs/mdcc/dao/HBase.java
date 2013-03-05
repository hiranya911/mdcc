package edu.ucsb.cs.mdcc.dao;

import java.io.IOException;     
import java.nio.ByteBuffer;
import java.util.Collection;   
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;     
import org.apache.hadoop.hbase.HBaseConfiguration;     
import org.apache.hadoop.hbase.HColumnDescriptor;     
import org.apache.hadoop.hbase.HTableDescriptor;     
import org.apache.hadoop.hbase.KeyValue;     
import org.apache.hadoop.hbase.MasterNotRunningException;     
import org.apache.hadoop.hbase.ZooKeeperConnectionException;    
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
	
    private static Configuration conf =null;

    public HBase() {
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
     * get one   
     */    
    private Result getOneRecord (String tableName, String rowKey) throws IOException{
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
     
	/**   
     * implements Database.java
     */ 
    public void put(Record record){
        try {
            HTable table = new HTable(conf, "Records");
            Put put = new Put(record.getKey().getBytes());
            put.add(Bytes.toBytes("value"),Bytes.toBytes(""),
                    Bytes.toBytes(record.getValue()));
            put.add(Bytes.toBytes("version"),Bytes.toBytes(""),
                    Bytes.toBytes(record.getVersion()));
            put.add(Bytes.toBytes("classicEndVersion"),Bytes.toBytes(""),
                    Bytes.toBytes(record.getClassicEndVersion()));
            put.add(Bytes.toBytes("BallotNumber"),Bytes.toBytes(""),
                    Bytes.toBytes(record.getBallot().toString()));
            put.add(Bytes.toBytes("prepared"),Bytes.toBytes(""),
                    Bytes.toBytes(record.isPrepared()));
            put.add(Bytes.toBytes("outstanding"),Bytes.toBytes(""),
                    Bytes.toBytes(record.getOutstanding()));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void putTransactionRecord(TransactionRecord record){    	
    	try{    		
    		//serialize Collection<Option>
    		Option[] optionArray = record.getOptions().toArray(new Option[0]);
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
    		
    		HTable table = new HTable(conf, "TxnRecords");
            Put put = new Put(record.getTransactionId().getBytes());    		
    		put.add(Bytes.toBytes("complete"),Bytes.toBytes(""),
                    Bytes.toBytes(record.isComplete()));
    		put.add(Bytes.toBytes("options"),Bytes.toBytes(""),
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
			HTable table = new HTable(conf, "Records");     
	        Get get = new Get(key.getBytes());     
	        Result rs = table.get(get);
			if (rs.isEmpty()){	        } 
			else {
				for(KeyValue kv : rs.raw()){  
		            String familyName = Bytes.toString(kv.getFamily());
		            String columnValue =  Bytes.toString(kv.getValue());
		            if (familyName == "value"){
		            		rec.setValue(ByteBuffer.wrap(columnValue.getBytes()));
		            		}
		            else if (familyName == "version"){
		            		rec.setVersion(Long.valueOf(columnValue));		            
		            		}
		            else if (familyName == "classicEndVersion"){
		            		rec.setClassicEndVersion(Long.valueOf(columnValue));
		            		}
		            else if (familyName == "BallotNumber"){
		            		rec.setBallot(new BallotNumber(columnValue));
		            		}
		            else if (familyName == "prepared"){
		            		rec.setPrepared(Boolean.valueOf(columnValue));
		            		}
		            else if (familyName == "outstanding"){
		            		rec.setOutstanding(columnValue);
		            		}
		            }
				}	        
            table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rec;
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
		            if (familyName == "value"){
	            		db.get(key).setValue(ByteBuffer.wrap(columnValue.getBytes()));
	            		}
		            else if (familyName == "version"){
		            	db.get(key).setVersion(Long.valueOf(columnValue));		            
	            		}
		            else if (familyName == "classicEndVersion"){
		            	db.get(key).setClassicEndVersion(Long.valueOf(columnValue));
	            		}
		            else if (familyName == "BallotNumber"){
		            	db.get(key).setBallot(new BallotNumber(columnValue));
	            		}
		            else if (familyName == "prepared"){
		            	db.get(key).setPrepared(Boolean.valueOf(columnValue));
	            		}
	            	else if (familyName == "outstanding"){
	            		db.get(key).setOutstanding(columnValue);
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
			rs = getOneRecord("TxnRecords", transactionId);
			if (rs.isEmpty()){
	        	System.out.print("Not found!"); 
	        }
			else{
				for(KeyValue kv : rs.raw()){  
		            String familyName = Bytes.toString(kv.getFamily());		            
		            if (familyName == "complete"){
		            	String columnValue =  Bytes.toString(kv.getValue());
		            	rec.finish(Boolean.valueOf(columnValue));
		            	}
		            else if(familyName == "options"){
	            		byte[] valueBytes =  kv.getValue();
	            		boolean isLength = true;
	            		int optionBytesLength = 0;
	            		byte[] optionBytesLengthBytes = new byte[4];
	            		int lengthCount = 0;
	            		int optionBytesCount = 0;
	            		for (int i = 0; i < valueBytes.length ; i++) {
	            			if (isLength){
	            				lengthCount++;
		            			if (lengthCount < 4){
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
				}
	        }
			return rec;
		} catch (IOException e) {
			e.printStackTrace();
			return rec;
		}
    } 
}    