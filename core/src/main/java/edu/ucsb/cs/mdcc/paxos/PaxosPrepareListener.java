package edu.ucsb.cs.mdcc.paxos;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.AsyncClient.prepare_call;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;

public class PaxosPrepareListener implements AsyncMethodCallback<prepare_call> {

    private static final Log log = LogFactory.getLog(PaxosPrepareListener.class);

	private AtomicInteger accepts = new AtomicInteger(0);
	private AtomicInteger rejects = new AtomicInteger(0);
    private AtomicBoolean outcomeReached = new AtomicBoolean(false);
    private AtomicBoolean transactionSet = new AtomicBoolean(false);
    private int acceptQuorum;
    private int numVoters;
    private Prepare myPrepare;
    private long version;
    private MDCCCommunicator communicator;
    private Map<String, Integer> responses = new HashMap<String,Integer>();
    private String winningTransaction = null;
	
	public PaxosPrepareListener(MDCCCommunicator communicator, Prepare prepare, long version) {
        MDCCConfiguration config = MDCCConfiguration.getConfiguration();
        this.numVoters = config.getMembers().length;
		this.acceptQuorum = (numVoters / 2) + 1;
		this.version = version;
		this.communicator = communicator;
		this.myPrepare = prepare;
	}
	
	public long getVersion() {
		return version;
	}
	
	public void start() {
		Member[] members = MDCCConfiguration.getConfiguration().getMembers();
		log.debug("Running Prepare phase");
		
        for (Member member : members) {
        	//communicator.sendPrepareAsync(member, myPrepare, this);
        }
	}
	
	public String getWinner(String transactionid) {
		int loopcount = 0;
		synchronized (transactionSet) {
			if (transactionSet.get()) {
				return winningTransaction;
			}
		}
		
		synchronized (outcomeReached) {
			while (!outcomeReached.get() && loopcount < 10) {
				try {
					outcomeReached.wait(5000);
				} catch (InterruptedException ignored) {
				} finally {
					loopcount++;
				}
			}
		}
		
		//if there has been no winner after timeout, just set outcomeReached so we have a null winner and return
		synchronized (outcomeReached) {
			if (outcomeReached.compareAndSet(false, true)) {
				synchronized (transactionSet) {
					if (transactionSet.compareAndSet(false, true)) {
						winningTransaction = null;
						outcomeReached.notifyAll();
					}
				}
				
			}
		}
		
		//now we pick a transaction if one did not have a majority
		synchronized (transactionSet) {
			if (transactionSet.compareAndSet(false, true)) {
				winningTransaction = transactionid;
				transactionSet.set(true);
			}
		}
		//if there was no majority return null, otherwise return the winner
		if (transactionSet.get())
			return winningTransaction;
		else
			return null;
	}
	
	public void onComplete(prepare_call response) {
		edu.ucsb.cs.mdcc.messaging.PrepareResponse resp;
		/*try {
			resp = response.getResult();
		} catch (TException e) {
			onReject();
			return;
		}

        if (resp.isOk()) {
            onAccept(new PrepareResponse(resp.isOk(), resp.getOutstanding()));
        } else {
            onReject();
        }*/
	}
	
	

	public void onError(Exception exception) {
		onReject();
	}

    private void onAccept(PrepareResponse response) {
        if (log.isDebugEnabled()) {
            log.debug("Key=" + myPrepare.getKey() + " accept " + this.hashCode());
        }
        synchronized(responses) {
        	//if we got an outstanding transaction back, increment its count
        	if (response.getOutstanding() != "") {
	        	if (responses.containsKey(response.getOutstanding())) {
	        		responses.put(response.getOutstanding(), responses.get(response.getOutstanding()) + 1);
	        	} else {
	        		responses.put(response.getOutstanding(), 1);
	        	}
        	}
        }
        //we need to hear from at least a majority -1 to guarantee only one leader makes progress
        if (accepts.incrementAndGet() >= (acceptQuorum - 1) &&
                !outcomeReached.get()) {
        	
            synchronized(outcomeReached){
            	if (outcomeReached.compareAndSet(false, true)) {
            		//TODO: set winner here, winner should be first highest version, then the majority winner, then whatever we want
            		synchronized(responses) {
            			winningTransaction = null;
            			//we only want the highest version proposals
            			for (Map.Entry<String, Integer> entry : responses.entrySet()) {
            				if (entry.getValue() >= acceptQuorum) {
            					winningTransaction = entry.getKey();
            					break;
            				}
            			}
            		}
            		outcomeReached.notifyAll();
            	} 
            }
        }
    }

    private void onReject() {
        if (log.isDebugEnabled()) {
            log.info("Key=" + myPrepare.getKey() + " reject " + this.hashCode());
        }
        synchronized(outcomeReached) {
        	if (rejects.incrementAndGet() > (numVoters - acceptQuorum) &&
                outcomeReached.compareAndSet(false, true) && transactionSet.compareAndSet(false, true)) {
        		winningTransaction = null;
    			outcomeReached.notifyAll();
        	}
        }
    }
}
