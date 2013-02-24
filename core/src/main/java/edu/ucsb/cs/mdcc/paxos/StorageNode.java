package edu.ucsb.cs.mdcc.paxos;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;

public class StorageNode extends Agent {

	private Map<String, Boolean> outstandingOptions = new HashMap<String, Boolean>();
    private Map<String,String> db = new HashMap<String, String>();
    private  Map<String, List<String>> txns = new HashMap<String, List<String>>();

    private List<String> peers;

	public boolean onAccept(String transaction, String key,
			long oldVersion, BallotNumber ballot, String value) {
		System.out.println("received accept message for: txn=" + transaction + "; obj=" + key);
		boolean success = false;
		
		synchronized (key.intern()) {
			if (outstandingOptions.containsKey(key) && outstandingOptions.get(key)) {
				return false;
            }

			try {
			    String currentEntry;
				if (!db.containsKey(key)) {
					db.put(key, "0|0:|");
					currentEntry = "0|0:|";
				} else {
					currentEntry = db.get(key);
                }

				long version = Long.parseLong(currentEntry.substring(0, currentEntry.indexOf('|')));
				currentEntry = currentEntry.substring(currentEntry.indexOf('|') + 1);
				BallotNumber oldBallot = new BallotNumber(Long.parseLong(currentEntry.substring(0, currentEntry.indexOf(':'))), 
						currentEntry.substring(currentEntry.indexOf(':') + 1, currentEntry.indexOf('|')));
				//if it is a new insert
				success = (version == oldVersion) && (ballot.getBallot() == -1 || 
						((ballot.getBallot() + ":" + ballot.getProcessId()).compareTo(
								oldBallot.getBallot() + ":" + oldBallot.getProcessId()) >= 0));
				
				if (success) {
					outstandingOptions.put(key, true);
					if (!txns.containsKey(transaction)) {
						txns.put(transaction, new LinkedList<String>());
                    }
					txns.get(transaction).add(key + "|" + (oldVersion + 1) + "|" + oldBallot.getBallot() + ":" + oldBallot.getProcessId() + "|" + value);
				}
			} catch(Exception ex) {
				System.out.println(ex.toString());
			}
			if (success) {
				System.out.println("option accepted");
            } else {
				System.out.println("option denied");
            }
			return success;
		}
	}

	public void onDecide(String transaction, boolean commit) {
		if (commit) {
			System.out.println("Recevied Commit decision on transaction id: " + transaction);
        } else {
			System.out.println("Recevied Abort on transaction id: " + transaction);
        }

		if (commit && txns.containsKey(transaction)) {
			try {
				for (String option : txns.get(transaction))
				{
					//System.out.println("Writing option: " + option);
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ExecutorService exec = Executors.newCachedThreadPool();
		
		exec.submit(new Runnable() {
			public void run() {
				MDCCCommunicator comms = new MDCCCommunicator();
		    	comms.StartListener(new StorageNode(), 7911);
			}
			
		});
		
		exec.submit(new Runnable() {
			public void run() {
				MDCCCommunicator comms = new MDCCCommunicator();
		    	comms.StartListener(new StorageNode(), 7912);
			}
			
		});
		
		exec.submit(new Runnable() {
			public void run() {
				MDCCCommunicator comms = new MDCCCommunicator();
		    	comms.StartListener(new StorageNode(), 7913);
			}
			
		});
		
		exec.submit(new Runnable() {
			public void run() {
				MDCCCommunicator comms = new MDCCCommunicator();
		    	comms.StartListener(new StorageNode(), 7914);
			}
			
		});
		
		exec.submit(new Runnable() {
			public void run() {
				MDCCCommunicator comms = new MDCCCommunicator();
		    	comms.StartListener(new StorageNode(), 7915);
			}
			
		});
		
		exec.shutdown();
		try {
			exec.awaitTermination(1000, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public boolean onPrepare(String object, BallotNumber ballot) {
		// TODO Auto-generated method stub
		return false;
	}

}
