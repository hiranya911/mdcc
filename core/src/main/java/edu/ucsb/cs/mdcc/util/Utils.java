package edu.ucsb.cs.mdcc.util;

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
}
