package edu.ucsb.cs.mdcc.util;

import edu.ucsb.cs.mdcc.Option;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class Utils {

    public static void incrementPort(Properties properties, String name, int offset) {
        int value = Integer.parseInt(properties.getProperty(name));
        value += offset;
        properties.setProperty(name, String.valueOf(value));
    }

    public static void rewriteQuorumPorts(Properties properties, int offset) {
        String value = properties.getProperty("server.0");
        String[] segments = value.split(":");
        int port1 = Integer.parseInt(segments[1]) + offset;
        int port2 = Integer.parseInt(segments[2]) + offset;
        properties.setProperty("server.0", segments[0] + ":" + port1 + ":" + port2);
    }

    public static byte[] serialize(Collection<Option> options) throws IOException {
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
        return optionCollectionBytes;
    }

    public static Collection<Option> deserialize(byte[] data) {
        boolean isLength = true;
        int optionBytesLength = 0;
        byte[] optionBytesLengthBytes = new byte[4];
        int lengthCount = 0;
        int optionBytesCount = 0;
        byte[] optionBytes = null;
        List<Option> options = new ArrayList<Option>();
        for (int i = 0; i < data.length ; i++) {
            if (isLength){
                if (lengthCount < 4){
                    optionBytesLengthBytes[lengthCount++] = data[i];
                    if (lengthCount == 4) {
                        optionBytesLength = Bytes.toInt(optionBytesLengthBytes);
                        optionBytes = new byte[optionBytesLength];
                        lengthCount = 0;
                        isLength = false;
                    }
                }
            } else {
                if (optionBytesCount < optionBytesLength){
                    optionBytes[optionBytesCount++] = data[i];
                    if (optionBytesCount == optionBytesLength) {
                        ByteBuffer serialized = ByteBuffer.wrap(optionBytes);
                        Option newOption = new Option(serialized);
                        options.add(newOption);
                        optionBytesCount = 0;
                        isLength = true;
                    }
                }
            }
        }
        return options;
    }
}
