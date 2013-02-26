package edu.ucsb.cs.mdcc.paxos;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StorageNode extends Agent {

    private static final Log log = LogFactory.getLog(StorageNode.class);

	private Map<String, Boolean> outstandingOptions = new HashMap<String, Boolean>();
    private Map<String,String> db = new HashMap<String, String>();
    private  Map<String, List<String>> transactions = new HashMap<String, List<String>>();
    private MDCCConfiguration config;

    private MDCCCommunicator communicator;

    public StorageNode() {
        this.config = MDCCConfiguration.getConfiguration();
        this.communicator = new MDCCCommunicator();
    }

    @Override
    public void start() {
        super.start();
        int port = config.getLocalMember().getPort();
        communicator.startListener(this, port);
    }

    @Override
    public void stop() {
        super.stop();
        communicator.stopListener();
    }

    public boolean onAccept(String transaction, String key,
			long oldVersion, BallotNumber ballot, String value) {
		log.info("received accept message for: txn=" + transaction + "; obj=" + key);
		
		synchronized (key.intern()) {
            Boolean outstanding = outstandingOptions.get(key);
			if (Boolean.TRUE.equals(outstanding)) {
                log.warn("Outstanding option detected on " + key +
                        " - Denying the new option");
				return false;
            }

            String currentEntry;
            if (!db.containsKey(key)) {
                db.put(key, "0|0:|");
                currentEntry = "0|0:|";
            } else {
                currentEntry = db.get(key);
            }

            long version = Long.parseLong(currentEntry.substring(0,
                    currentEntry.indexOf('|')));
            currentEntry = currentEntry.substring(currentEntry.indexOf('|') + 1);
            BallotNumber oldBallot = new BallotNumber(Long.parseLong(
                    currentEntry.substring(0, currentEntry.indexOf(':'))),
                    currentEntry.substring(currentEntry.indexOf(':') + 1,
                            currentEntry.indexOf('|')));
            //if it is a new insert
            boolean success = (version == oldVersion) && (ballot.getBallot() == -1 ||
                    ((ballot.getBallot() + ":" + ballot.getProcessId()).compareTo(
                            oldBallot.getBallot() + ":" + oldBallot.getProcessId()) >= 0));

            if (success) {
                outstandingOptions.put(key, true);
                if (!transactions.containsKey(transaction)) {
                    transactions.put(transaction, new LinkedList<String>());
                }
                transactions.get(transaction).add(key + "|" + (oldVersion + 1) + "|" +
                        oldBallot.getBallot() + ":" + oldBallot.getProcessId() + "|" + value);
				log.info("option accepted");
            } else {
				log.warn("option denied");
            }
			return success;
		}
	}

	public void onDecide(String transaction, boolean commit) {
		if (commit) {
			log.info("Received Commit decision on transaction id: " + transaction);
        } else {
			log.info("Received Abort on transaction id: " + transaction);
        }

		if (commit && transactions.containsKey(transaction)) {
			try {
				for (String option : transactions.get(transaction)) {
					String object = option.substring(0, option.indexOf('|'));
					String newVersion = option.substring(option.indexOf('|') + 1);
					db.put(object, newVersion);
					outstandingOptions.remove(object);
				}
			} catch(Exception ex) {
				System.out.println(ex.toString());
			}
		}
		transactions.remove(transaction);
	}

	public String onRead(String object) {
		if (db.containsKey(object)) {
			return db.get(object);
        } else {
			return "||";
        }
	}

	public static void main(String[] args) {
        final StorageNode storageNode = new StorageNode();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                storageNode.stop();
            }
        });
        storageNode.start();
	}

	public boolean onPrepare(String object, BallotNumber ballot) {
		return false;
	}

}
