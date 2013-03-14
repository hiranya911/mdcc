package edu.ucsb.cs.mdcc.config;

import edu.ucsb.cs.mdcc.MDCCException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class AppServerConfiguration {

    private static final Log log = LogFactory.getLog(AppServerConfiguration.class);

    private static volatile AppServerConfiguration config = null;

    private Properties properties;
    private Map<Integer,Member[]> members = new HashMap<Integer, Member[]>();

    private AppServerConfiguration(Properties properties) {
        this.properties = properties;

        Map<Integer,List<Member>> tempMembers = new HashMap<Integer,List<Member>>();
        for (String property : properties.stringPropertyNames()) {
            if (property.startsWith("mdcc.server")) {
                int shardId = Integer.parseInt(property.substring(12, property.indexOf('.', 13)));
                String value = properties.getProperty(property);
                String processId = property.substring(property.lastIndexOf('.') + 1);
                String[] connection = value.split(":");
                Member member = new Member(connection[0],
                        Integer.parseInt(connection[1]), processId, false);
                List<Member> temp = tempMembers.get(shardId);
                if (temp == null) {
                    temp = new ArrayList<Member>();
                    tempMembers.put(shardId, temp);
                }
                temp.add(member);
            }
        }

        for (Map.Entry<Integer,List<Member>> entry : tempMembers.entrySet()) {
            Collections.sort(entry.getValue(), new Comparator<Member>() {
                public int compare(Member o1, Member o2) {
                    try {
                        int o1Id = Integer.parseInt(o1.getProcessId());
                        int o2Id = Integer.parseInt(o2.getProcessId());
                        return o1Id - o2Id;
                    } catch (NumberFormatException e) {
                        return o1.getProcessId().compareTo(o2.getProcessId());
                    }
                }
            });
            members.put(entry.getKey(), entry.getValue().toArray(
                    new Member[entry.getValue().size()]));
        }

    }

    public static AppServerConfiguration getConfiguration() {
        if (config == null) {
            synchronized (MDCCConfiguration.class) {
                if (config == null) {
                    String configPath = System.getProperty("mdcc.config.dir", "conf");
                    Properties props = new Properties();
                    File configFile = new File(configPath, "app-server.properties");
                    try {
                        props.load(new FileInputStream(configFile));
                        config = new AppServerConfiguration(props);
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

    public Member[] getMembers(int shardId) {
        return members.get(shardId);
    }

    public Member[] getMembers(String key) {
        return members.get(getShardId(key));
    }

    public int getShardId(String key) {
        return Math.abs(key.hashCode() % members.size());
    }

    public int getShards() {
        return members.size();
    }

    public String getAppServerUrl() {
        return properties.getProperty("mdcc.app.server");
    }

    public boolean reorderMembers(int shardId, String primary) {
        try {
            getMember(shardId, primary);
        } catch (Exception e) {
            return false;
        }

        Member[] temp = new Member[members.get(shardId).length];
        int index = 1;
        for (Member member : members.get(shardId)) {
            if (member.getProcessId().equals(primary)) {
                temp[0] = member;
            } else {
                temp[index] = member;
                index++;
            }
        }
        members.put(shardId, temp);
        return true;
    }

    public Member getMember(int shardId, String id) {
        for (Member member : members.get(shardId)) {
            if (member.getProcessId().equals(id)) {
                return member;
            }
        }
        throw new MDCCException("Unable to locate a member by ID: " + id);
    }

}
