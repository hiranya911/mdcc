package edu.ucsb.cs.mdcc.paxos;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;
import edu.ucsb.cs.mdcc.messaging.RecordVersion;

public class FPAgent extends Agent {
	Object objectLock = new Object();
	Map<String, Object> objectLocks = new HashMap<String, Object>();
	Map<String, boolean[]> outstandingOptions = new HashMap<String, boolean[]>();
	Map<String,String> db = new HashMap<String, String>();
	Map<String, List<String>> txns = new HashMap<String, List<String>>();
	
	public List<String> peers;
	
	@Override
	public void onElection() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onVictory(String hostName, int port) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean onLeaderQuery() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onAccept(String transaction, String object,
			RecordVersion oldVersion, String processId, String value) {
		System.out.println("received accept message for: txn=" + transaction + "; obj=" + object);
		// TODO Auto-generated method stub
		boolean success = false;
		synchronized (objectLock)
		{
			if (!objectLocks.containsKey(object))
				objectLocks.put(object, new Object());
		}
		
		synchronized (objectLocks.get(object))
		{
			if (outstandingOptions.containsKey(object) && outstandingOptions.get(object)[0])
				return false;
			try {
			
				// TODO Auto-generated method stub
				String currentEntry;
				if (!db.containsKey(object))
				{
					db.put(object, "|");
					currentEntry = "|";
				}
				else
					currentEntry = db.get(object);
				String currentBallot = currentEntry.substring(0,currentEntry.indexOf('|'));
				//if it is a new insert
				success = currentBallot.length() == 0 && oldVersion.getBallot() == 0 && oldVersion.getProcessId().length() == 0;
				if (!success) //else if they specified the correct current ballot
					success = currentBallot.compareTo(oldVersion.getBallot() + ":" + 
							oldVersion.getProcessId()) == 0;
				if (success)
				{
					outstandingOptions.put(object, new boolean[] { true });
					if (currentBallot.length() == 0)
						currentBallot = "1" + ":" + processId;
					else
						currentBallot = (Integer.parseInt(currentBallot.substring(0,currentBallot.indexOf(':')) + 1) +
							":" + processId);
					if (!txns.containsKey(transaction))
						txns.put(transaction, new LinkedList<String>());
					txns.get(transaction).add(object + "|" + currentBallot + "|" + value);
				}
			} catch(Exception ex) {
				System.out.println(ex.toString());
			}
			if (success)
				System.out.println("option accepted");
			else
				System.out.println("option denied");
			return success;
		}
	}

	@Override
	public void onDecide(String transaction, boolean commit) {
		// TODO Auto-generated method stub
		if (commit)
			System.out.println("Recevied Commit decision on transaction id: " + transaction);
		else
			System.out.println("Recevied Abort on transaction id: " + transaction);
		
		if (commit)
		{
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
		else if (txns.containsKey(transaction))
			txns.remove(transaction);
	}

	@Override
	public String onGet(String object) {
		if (db.containsKey(object))
			return db.get(object);
		else
			return "|";
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ExecutorService exec = Executors.newCachedThreadPool();
		
		exec.submit(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				MDCCCommunicator comms = new MDCCCommunicator();
		    	comms.StartListener(new FPAgent(), 7911);
			}
			
		});
		
		exec.submit(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				MDCCCommunicator comms = new MDCCCommunicator();
		    	comms.StartListener(new FPAgent(), 7912);
			}
			
		});
		
		exec.submit(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				MDCCCommunicator comms = new MDCCCommunicator();
		    	comms.StartListener(new FPAgent(), 7913);
			}
			
		});
		
		exec.submit(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				MDCCCommunicator comms = new MDCCCommunicator();
		    	comms.StartListener(new FPAgent(), 7914);
			}
			
		});
		
		exec.submit(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				MDCCCommunicator comms = new MDCCCommunicator();
		    	comms.StartListener(new FPAgent(), 7915);
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

}
