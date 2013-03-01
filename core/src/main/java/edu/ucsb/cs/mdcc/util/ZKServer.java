package edu.ucsb.cs.mdcc.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import java.io.IOException;

public class ZKServer implements Runnable {

    private static final Log log = LogFactory.getLog(ZKServer.class);

    private QuorumPeerConfig config;
    private QuorumPeerMain peer;

    public ZKServer(QuorumPeerConfig config) {
        this.config = config;
        this.peer = new QuorumPeerMain();
    }

    public void run() {
        try {
            peer.runFromConfig(config);
        } catch (IOException e) {
            log.fatal("Fatal error encountered in ZooKeeper server", e);
            System.exit(1);
        }
    }
}
