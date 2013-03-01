package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.MDCCException;

import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.util.HBaseServer;
import edu.ucsb.cs.mdcc.util.ZKServer;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class Agent implements Watcher, AsyncCallback.ChildrenCallback, AgentService {

    protected final Log log = LogFactory.getLog(this.getClass());

    private static final String ELECTION_NODE = "/ELECTION_";

    private ExecutorService zkService = Executors.newSingleThreadExecutor();
    private HBaseServer hbaseServer;
    private ZooKeeper zkClient;
    private Map<String,Member> leaders = new ConcurrentHashMap<String, Member>();

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

        hbaseServer = new HBaseServer();
        hbaseServer.start();
    }

    public void stop() {
        zkService.shutdownNow();
        hbaseServer.stop();
        System.out.println("Program terminated");
    }

    public Member findLeader(String key, boolean force) {
        if (!force && leaders.containsKey(key)) {
            return leaders.get(key);
        } else {
            leaders.remove(key);
        }

        MDCCConfiguration config = MDCCConfiguration.getConfiguration();
        String electionDir = ELECTION_NODE + key;
        try {
            Stat stat = zkClient.exists(electionDir, false);
            if (stat == null) {
                try {
                    zkClient.create(electionDir, new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    log.info("Created ZK node: " + electionDir);
                } catch (KeeperException.NodeExistsException ignored) {
                }
            }
        } catch (Exception e) {
            handleFatalException("Error while creating the ELECTION root", e);
        }

        zkClient.getChildren(electionDir, true, this, null);
        String zNode = electionDir + "/" + config.getLocalMember().getProcessId() + "_";
        try {
            zkClient.create(zNode, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (Exception e) {
            handleFatalException("Failed to create z_node", e);
        }

        synchronized (leaders) {
            while (!leaders.containsKey(key)) {
                try {
                    log.info("Waiting for leader election to complete");
                    leaders.wait(5000);
                } catch (InterruptedException ignored) {
                }
            }
        }
        return leaders.get(key);
    }

    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            handleNoneEvent(event);
        } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
            log.info("Registering event for " + event.getPath());
            zkClient.getChildren(event.getPath(), true, this, null);
        }
    }

    public void processResult(int code, String path, Object o, List<String> children) {
        if (code != KeeperException.Code.OK.intValue()) {
            log.error("Unexpected response code from ZooKeeper server");
            return;
        }

        long smallest = Long.MAX_VALUE;
        String processId = null;
        for (String child : children) {
            int index = child.lastIndexOf('_');
            long sequenceNumber = Long.parseLong(child.substring(index + 1));
            if (sequenceNumber < smallest) {
                smallest = sequenceNumber;
                processId = child.substring(0, index);
            }
        }

        if (processId != null) {
            synchronized (leaders) {
                String key = path.substring(ELECTION_NODE.length());
                Member member = MDCCConfiguration.getConfiguration().getMember(processId);
                leaders.put(key, member);
                leaders.notifyAll();
            }
        }
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
}
