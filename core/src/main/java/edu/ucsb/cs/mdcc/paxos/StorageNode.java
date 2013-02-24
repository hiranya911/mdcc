package edu.ucsb.cs.mdcc.paxos;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;

public class StorageNode extends Agent {

	private Map<String, Boolean> outstandingOptions = new HashMap<String, Boolean>();
    private Map<String,String> db = new HashMap<String, String>();
    private  Map<String, List<String>> txns = new HashMap<String, List<String>>();

    private MDCCCommunicator communicator;
    private int port;

    public StorageNode(int port) {
        this.port = port;
        this.communicator = new MDCCCommunicator();
    }

    @Override
    public void start() {
        //super.start();
        communicator.startListener(this, port);
    }

    @Override
    public void stop() {
        //super.stop();
        communicator.stopListener();
    }

    public boolean onAccept(String transaction, String key,
			long oldVersion, BallotNumber ballot, String value) {
		System.out.println("received accept message for: txn=" + transaction + "; obj=" + key);
		
		synchronized (key.intern()) {
            Boolean outstanding = outstandingOptions.get(key);
			if (Boolean.TRUE.equals(outstanding)) {
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
                if (!txns.containsKey(transaction)) {
                    txns.put(transaction, new LinkedList<String>());
                }
                txns.get(transaction).add(key + "|" + (oldVersion + 1) + "|" +
                        oldBallot.getBallot() + ":" + oldBallot.getProcessId() + "|" + value);
				System.out.println("option accepted");
            } else {
				System.out.println("option denied");
            }
			return success;
		}
	}

	public void onDecide(String transaction, boolean commit) {
		if (commit) {
			System.out.println("Received Commit decision on transaction id: " + transaction);
        } else {
			System.out.println("Received Abort on transaction id: " + transaction);
        }

		if (commit && txns.containsKey(transaction)) {
			try {
				for (String option : txns.get(transaction)) {
					String object = option.substring(0, option.indexOf('|'));
					String newVersion = option.substring(option.indexOf('|') + 1);
					db.put(object, newVersion);
					outstandingOptions.remove(object);
				}
			} catch(Exception ex) {
				System.out.println(ex.toString());
			}
		}
		txns.remove(transaction);
	}

	public String onRead(String object) {
		if (db.containsKey(object)) {
			return db.get(object);
        } else {
			return "||";
        }
	}

	public static void main(String[] args) {
        StorageNode node1 = new StorageNode(7911);
        node1.start();

        StorageNode node2 = new StorageNode(7912);
        node2.start();

        StorageNode node3 = new StorageNode(7913);
        node3.start();

        StorageNode node4 = new StorageNode(7914);
        node4.start();

        StorageNode node5 = new StorageNode(7915);
        node5.start();
	}

	public boolean onPrepare(String object, BallotNumber ballot) {
		return false;
	}

}
