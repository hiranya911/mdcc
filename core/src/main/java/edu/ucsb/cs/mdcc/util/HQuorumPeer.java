package edu.ucsb.cs.mdcc.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;

/*
 * Code borrowed from HBase 0.94.5 tag with thanks
 */
public class HQuorumPeer implements Runnable {

    private Properties hbaseProperties;

    public HQuorumPeer(Properties hbaseProperties) {
        this.hbaseProperties = hbaseProperties;
    }

    public void run() {
        Configuration conf = HBaseConfiguration.create();
        try {
            Properties zkProperties = ZKConfig.makeZKProps(conf);

            String hbasePath = System.getProperty("mdcc.hbase.dir", "hbase");
            File zkDataDir = new File(hbasePath, "zk");
            zkProperties.setProperty("dataDir", zkDataDir.getAbsolutePath());
            for (String key : hbaseProperties.stringPropertyNames()) {
                zkProperties.setProperty(key, hbaseProperties.getProperty(key));
            }

            writeMyID(zkProperties);
            QuorumPeerConfig zkConfig = new QuorumPeerConfig();
            zkConfig.parseProperties(zkProperties);

            // login the zookeeper server principal (if using security)
            ZKUtil.loginServer(conf, "hbase.zookeeper.server.keytab.file",
                    "hbase.zookeeper.server.kerberos.principal",
                    zkConfig.getClientPortAddress().getHostName());

            runZKServer(zkConfig);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void runZKServer(QuorumPeerConfig zkConfig) throws IOException {
        if (zkConfig.isDistributed()) {
            QuorumPeerMain qp = new QuorumPeerMain();
            qp.runFromConfig(zkConfig);
        } else {
            ZooKeeperServerMain zk = new ZooKeeperServerMain();
            ServerConfig serverConfig = new ServerConfig();
            serverConfig.readFrom(zkConfig);
            zk.runFromConfig(serverConfig);
        }
    }

    private static boolean addressIsLocalHost(String address) {
        return address.equals("localhost") || address.equals("127.0.0.1");
    }

    static void writeMyID(Properties properties) throws IOException {
        long myId = -1;

        Configuration conf = HBaseConfiguration.create();
        String myAddress = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
                conf.get("hbase.zookeeper.dns.interface", "default"),
                conf.get("hbase.zookeeper.dns.nameserver", "default")));

        List<String> ips = new ArrayList<String>();

        // Add what could be the best (configured) match
        ips.add(myAddress.contains(".") ?
                myAddress :
                StringUtils.simpleHostname(myAddress));

        // For all nics get all hostnames and IPs
        Enumeration<?> nics = NetworkInterface.getNetworkInterfaces();
        while(nics.hasMoreElements()) {
            Enumeration<?> rawAdrs =
                    ((NetworkInterface)nics.nextElement()).getInetAddresses();
            while(rawAdrs.hasMoreElements()) {
                InetAddress inet = (InetAddress) rawAdrs.nextElement();
                ips.add(StringUtils.simpleHostname(inet.getHostName()));
                ips.add(inet.getHostAddress());
            }
        }

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            if (key.startsWith("server.")) {
                int dot = key.indexOf('.');
                long id = Long.parseLong(key.substring(dot + 1));
                String[] parts = value.split(":");
                String address = parts[0];
                if (addressIsLocalHost(address) || ips.contains(address)) {
                    myId = id;
                    break;
                }
            }
        }

        // Set the max session timeout from the provided client-side timeout
        properties.setProperty("maxSessionTimeout",
                conf.get("zookeeper.session.timeout", "180000"));

        if (myId == -1) {
            throw new IOException("Could not find my address: " + myAddress +
                    " in list of ZooKeeper quorum servers");
        }

        String dataDirStr = properties.get("dataDir").toString().trim();
        File dataDir = new File(dataDirStr);
        if (!dataDir.isDirectory()) {
            if (!dataDir.mkdirs()) {
                throw new IOException("Unable to create data dir " + dataDir);
            }
        }

        File myIdFile = new File(dataDir, "myid");
        PrintWriter w = new PrintWriter(myIdFile);
        w.println(myId);
        w.close();
    }
}
