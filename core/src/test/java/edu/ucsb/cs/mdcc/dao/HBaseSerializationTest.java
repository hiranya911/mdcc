package edu.ucsb.cs.mdcc.dao;

import edu.ucsb.cs.mdcc.Option;
import junit.framework.TestCase;

import java.nio.ByteBuffer;

public class HBaseSerializationTest extends TestCase {

    public void testOptionSerialization() {
        String key = "foo";
        ByteBuffer value = ByteBuffer.wrap("foo-value".getBytes());
        Option option = new Option(key, value, 100, false);

        ByteBuffer serialization = option.toBytes();
        Option option2 = new Option(serialization);

        assertEquals(option2.getKey(), key);
        assertEquals(new String(option2.getValue().array()), "foo-value");
        assertEquals(option2.getOldVersion(), 100);
    }

}
