package edu.ucsb.cs.mdcc.paxos;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;
import edu.ucsb.cs.mdcc.messaging.ReadValue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StorageNode extends Agent {

    private static final Log log = LogFactory.getLog(StorageNode.class);

	private Map<String, Boolean> outstandingOptions = new HashMap<String, Boolean>();
    private Map<String,BallotNumber> ballots = new HashMap<String, BallotNumber>();
    private Map<String, ReadValue> db = new HashMap<String, ReadValue>();
    private  Map<String, List<Option>> transactions = new HashMap<String, List<Option>>();
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
			long oldVersion, BallotNumber ballot, ByteBuffer value) {
		log.info("received accept message for: txn=" + transaction + "; obj=" + key);
		
		synchronized (key.intern()) {
            Boolean outstanding = outstandingOptions.get(key);
			if (Boolean.TRUE.equals(outstanding)) {
                log.warn("Outstanding option detected on " + key +
                        " - Denying the new option");
				return false;
            }

            ReadValue entryValue;
            BallotNumber entryBallot;
            if (!db.containsKey(key)) {
            	entryValue = new ReadValue(0, ByteBuffer.allocate(0));
            	entryBallot = new BallotNumber(0,"");
                ballots.put(key, entryBallot);
            } else {
                entryValue = db.get(key);
                entryBallot = ballots.get(key);
            }

            long version = entryValue.getVersion();
            //if it is a new insert
            boolean success = (version == oldVersion) && (ballot.getNumber() == -1 ||
                    ((ballot.getNumber() + ":" + ballot.getProcessId()).compareTo(
                            entryBallot.getNumber() + ":" + entryBallot.getProcessId()) >= 0));

            if (success) {
                outstandingOptions.put(key, true);
                if (!transactions.containsKey(transaction)) {
                    transactions.put(transaction, new LinkedList<Option>());
                }
                transactions.get(transaction).add(
                        new Option(key, value, entryValue.getVersion()));
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
            for (Option option : transactions.get(transaction)) {
                db.put(option.getKey(), new ReadValue(option.getOldVersion() + 1,
                        option.getValue()));
                outstandingOptions.remove(option.getKey());
            }
		}
		transactions.remove(transaction);
	}

	public ReadValue onRead(String object) {
		if (db.containsKey(object)) {
			return db.get(object);
        } else {
			return new ReadValue(0, ByteBuffer.allocate(0));
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
