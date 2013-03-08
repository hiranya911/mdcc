package edu.ucsb.cs.mdcc;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class Option {

    private static final Log log = LogFactory.getLog(Option.class);

    private String key;
    private byte[] value;
    private long oldVersion;
    private boolean classic;

    public Option(String key, byte[] value, long oldVersion, boolean classic) {
        this.key = key;
        this.value = value;
        this.oldVersion = oldVersion;
        this.classic = classic;
    }

    public Option(ByteBuffer serialized){
        int offset = 0;
        byte[] concatenated = serialized.array();

        int keyLength = Bytes.toInt(concatenated, offset, 4);
        offset += 4;

        //parse the key
        this.key = Bytes.toString(concatenated, offset, keyLength);
        offset += keyLength;

        //parse the valueLength
        int valueLength = Bytes.toInt(concatenated, offset, 4);
        offset += 4;

        //parse the value
        this.value = new byte[valueLength];
        System.arraycopy(concatenated, offset, value, 0, valueLength);
        offset += valueLength;

        this.oldVersion = Bytes.toLong(concatenated, offset, 8);
        this.classic = false;
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getOldVersion() {
        return oldVersion;
    }

    public boolean isClassic() {
        return classic;
    }

    public void setClassic() {
        classic = true;
    }
    
    public ByteBuffer toBytes(){
    	byte[] keyBytes = Bytes.toBytes(key);
    	byte[] keyLengthBytes = Bytes.toBytes(keyBytes.length);
    	byte[] valueLengthBytes = Bytes.toBytes(value.length);
    	byte[] oldVersionBytes = Bytes.toBytes(oldVersion);
    	
    	ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
    	try {
			outputStream.write(keyLengthBytes);
	    	outputStream.write(keyBytes);
	    	outputStream.write(valueLengthBytes);
	    	outputStream.write(value);
	    	outputStream.write(oldVersionBytes);
		} catch (IOException e) {
			log.error("Unexpected error while serializing an option", e);
		}

        byte[] concatenated = outputStream.toByteArray( );
    	ByteBuffer serialized = ByteBuffer.wrap(concatenated);
    	try {
			outputStream.close();
		} catch (IOException ignored) {
		}
    	return serialized;    
    }

}
