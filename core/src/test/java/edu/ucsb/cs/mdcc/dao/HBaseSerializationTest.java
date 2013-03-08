package edu.ucsb.cs.mdcc.dao;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.util.Utils;
import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HBaseSerializationTest extends TestCase {

    public void testOptionSerialization() {
        String key = "foo";
        byte[] value = "foo-value".getBytes();
        Option option = new Option(key, value, 100, false);

        ByteBuffer serialization = option.toBytes();
        Option option2 = new Option(serialization);

        assertEquals(option2.getKey(), key);
        assertEquals(new String(option2.getValue()), "foo-value");
        assertEquals(option2.getOldVersion(), 100);
    }

    public void testOptionCollectionSerialization0() throws Exception {
        List<Option> options = new ArrayList<Option>();

        byte[] data = Utils.serialize(options);
        Collection<Option> options2 = Utils.deserialize(data);
        assertEquals(options2.size(), 0);
    }

    public void testOptionCollectionSerialization1() throws Exception {
        List<Option> options = new ArrayList<Option>();
        Option o1 = new Option("foo", "foo-value".getBytes(), 100, false);
        options.add(o1);

        byte[] data = Utils.serialize(options);
        Collection<Option> options2 = Utils.deserialize(data);
        Option o1Copy = options2.toArray(new Option[options2.size()])[0];
        assertEquals(o1Copy.getKey(), "foo");
        assertEquals(new String(o1Copy.getValue()), "foo-value");
        assertEquals(o1Copy.getOldVersion(), 100);
    }

    public void testOptionCollectionSerialization2() throws Exception {
        List<Option> options = new ArrayList<Option>();
        Option o1 = new Option("foo", "foo-value".getBytes(), 100, false);
        Option o2 = new Option("bar", "bar-value".getBytes(), 200, false);
        options.add(o1);
        options.add(o2);

        byte[] data = Utils.serialize(options);
        Collection<Option> options2 = Utils.deserialize(data);
        Option o1Copy = options2.toArray(new Option[options2.size()])[0];
        Option o2Copy = options2.toArray(new Option[options2.size()])[1];
        assertEquals(o1Copy.getKey(), "foo");
        assertEquals(new String(o1Copy.getValue()), "foo-value");
        assertEquals(o1Copy.getOldVersion(), 100);

        assertEquals(o2Copy.getKey(), "bar");
        assertEquals(new String(o2Copy.getValue()), "bar-value");
        assertEquals(o2Copy.getOldVersion(), 200);
    }

}
