package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.MDCCException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class Agent implements Watcher, AsyncCallback.ChildrenCallback, AgentService {

    protected final Log log = LogFactory.getLog(this.getClass());

    private static final String ELECTION_NODE = "/ELECTION";

    private ExecutorService zkService = Executors.newSingleThreadExecutor();
    private ZooKeeper zkClient;

    public void start() {
        Properties properties = new Properties();
        String configPath = System.getProperty("mdcc.config.dir", "conf");
        String dataPath = System.getProperty("mdcc.zk.dir", "zk");
        File configFile = new File(configPath, "zk.properties");
        try {
            properties.load(new FileInputStream(configFile));
            properties.setProperty("dataDir", dataPath);
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(properties);
            ZKServer server = new ZKServer(config);
            zkService.submit(server);
            log.info("ZooKeeper server started");
        } catch (IOException e) {
            handleException("Error loading the ZooKeeper configuration", e);
        } catch (QuorumPeerConfig.ConfigException e) {
            handleException("Error loading the ZooKeeper configuration", e);
        }

        String connection = "localhost:" + properties.getProperty("clientPort");
        try {
            zkClient = new ZooKeeper(connection, 5000, this);
        } catch (IOException e) {
            handleException("Error initializing the ZooKeeper client", e);
        }
    }

    public void stop() {
        zkService.shutdownNow();
        System.out.println("Program terminated");
    }

    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            handleNoneEvent(event);
        }
    }

    public void processResult(int code, String s, Object o, List<String> children) {

    }

    private void handleNoneEvent(WatchedEvent event) {
        switch (event.getState()) {
            case SyncConnected:
                log.info("Successfully connected to the ZooKeeper server");
                try {
                    Stat stat = zkClient.exists(ELECTION_NODE, false);
                    if (stat == null) {
                        try {
                            zkClient.create(ELECTION_NODE, new byte[0],
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        } catch (KeeperException.NodeExistsException ignored) {
                        }
                    }
                } catch (Exception e) {
                    handleFatalException("Error while creating the ELECTION root", e);
                }
                break;
            case Disconnected:
                log.warn("Lost the connection to ZooKeeper server");
                break;
            case Expired:
                handleFatalException("Connection to ZooKeeper server expired", null);
        }
    }

    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new MDCCException(msg, e);
    }

    private void handleFatalException(String msg, Exception e) {
        if (e != null) {
            log.fatal(msg, e);
        } else {
            log.fatal(msg);
        }
        System.exit(1);
    }

    private class ZKServer implements Runnable {

        private QuorumPeerConfig config;
        private QuorumPeerMain peer;

        private ZKServer(QuorumPeerConfig config) {
            this.config = config;
            this.peer = new QuorumPeerMain();
        }

        public void run() {
            try {
                peer.runFromConfig(config);
            } catch (IOException e) {
                handleFatalException("Fatal error encountered in ZooKeeper server", e);
            }
        }
    }
}
