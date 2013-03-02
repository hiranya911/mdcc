package edu.ucsb.cs.mdcc.util;

import edu.ucsb.cs.mdcc.MDCCException;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class HBaseServer {

    private static final Log log = LogFactory.getLog(HBaseServer.class);

    private ExecutorService exec;

    private HMaster master;
    private HRegionServer regionServer;
    private Future masterFuture;
    private Future regionServerFuture;

    public void start() {
        exec = Executors.newFixedThreadPool(3);

        Properties properties = new Properties();
        String configPath = System.getProperty("mdcc.config.dir", "conf");
        String hbasePath = System.getProperty("mdcc.hbase.dir", "hbase");
        File configFile = new File(configPath, "hbase.properties");
        try {
            properties.load(new FileInputStream(configFile));
            int myId = MDCCConfiguration.getConfiguration().getMyId();
            Utils.incrementPort(properties, "clientPort", myId);
            Utils.incrementPort(properties, "hbase.master.port", myId);
            Utils.incrementPort(properties, "hbase.regionserver.port", myId);
            Utils.rewriteQuorumPorts(properties, myId);
        } catch (IOException e) {
            handleException("Error loading the ZooKeeper configuration", e);
        }

        exec.submit(new HQuorumPeer(properties));
        log.info("HBase ZooKeeper server started");

        Configuration config = HBaseConfiguration.create();
        File hbaseDir = new File(hbasePath, "data");
        config.set("hbase.rootdir", hbaseDir.getAbsolutePath());
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("hbase.")) {
                config.set(key, properties.getProperty(key));
            } else {
                String name = "hbase.zookeeper.property." + key;
                config.set(name, properties.getProperty(key));
            }
        }

        try {
            master = new HMaster(config);
            regionServer = new HRegionServer(config);
            masterFuture = exec.submit(master);
            regionServerFuture = exec.submit(regionServer);
            log.info("HBase server is up and running...");
        } catch (Exception e) {
            handleException("Error while initializing HBase server", e);
        }
    }

    public void stop() {
        master.stop("Shutting down HBase master");
        regionServer.stop("Shutting down HBase region server");
        while (!masterFuture.isDone()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        while (!regionServerFuture.isDone()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        exec.shutdownNow();
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new MDCCException(msg, e);
    }
}
