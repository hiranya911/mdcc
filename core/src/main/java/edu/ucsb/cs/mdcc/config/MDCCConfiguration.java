package edu.ucsb.cs.mdcc.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucsb.cs.mdcc.MDCCException;

public class MDCCConfiguration {

	private static final Log log = LogFactory.getLog(MDCCConfiguration.class);

	private static volatile MDCCConfiguration config = null;
	
	private String serverId;
	private Member[] members = {
        new Member("DefaultNode", 7911, "localhost", true)
    };
	
	public MDCCConfiguration(Properties properties) {
		serverId = properties.getProperty("mdcc.myid");

		// eventually we will probably want to read myid from zookeeper to
		// have one less configuration variable to set
		/*File zkDir = new File(System.getProperty("mdcc.zk.dir"));
        File myIdFile = new File(zkDir, "myid");
        String myId = null;
        try {
            myId = FileUtils.readFileToString(myIdFile).trim();
        } catch (IOException e) {
            throw new WrenchException("Unable to read the ZK myid file", e);
        }*/
		
		List<Member> allMembers = new ArrayList<Member>();
		for (String property : properties.stringPropertyNames()) {
            if (property.startsWith("mdcc.server")) {
                String value = properties.getProperty(property);
                String processId = property.substring(property.lastIndexOf('.') + 1);
                String[] connection = value.split(":");
                boolean local = processId.equals(serverId);
                Member member = new Member(connection[0],
                        Integer.parseInt(connection[1]), processId, local);
                allMembers.add(member);
            }
        }
		members = allMembers.toArray(new Member[allMembers.size()]);
	}
	
	public static MDCCConfiguration getConfiguration() {
        if (config == null) {
            synchronized (MDCCConfiguration.class) {
                if (config == null) {
                    String configPath = System.getProperty("mdcc.config.dir", "conf");
                    Properties props = new Properties();
                    File configFile = new File(configPath, "mdcc.properties");
                    try {
                        props.load(new FileInputStream(configFile));
                        config = new MDCCConfiguration(props);
                    } catch (IOException e) {
                        String msg = "Error loading MDCC configuration from: " + configFile.getPath();
                        log.error(msg, e);
                        throw new MDCCException(msg, e);
                    }
                }
            }
        }
        return config;
    }
	
    public Member[] getMembers() {
        return members;
    }
    
    public String getServerId() {
    	return serverId;
    }

}
