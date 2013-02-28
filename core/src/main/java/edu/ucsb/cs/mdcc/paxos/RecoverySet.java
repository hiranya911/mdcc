package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.AsyncClient.recover_call;
import edu.ucsb.cs.mdcc.messaging.ReadValue;

public class RecoverySet implements
		AsyncMethodCallback<MDCCCommunicationService.AsyncClient.recover_call> {
	private static final Log log = LogFactory.getLog(RecoverySet.class);
	private LinkedBlockingQueue<Map<String, ReadValue>> versions = new LinkedBlockingQueue<Map<String, ReadValue>>();
	private AtomicInteger callsHandled = new AtomicInteger(0);
	private final Semaphore available;
	private int numCalls;
	
	public RecoverySet(int numCalls) {
		available =  new Semaphore(numCalls);
		this.numCalls = numCalls;
	}
	
	public Map<String, ReadValue> dequeueRecoveryInfo() {
		//if there are more calls to receive, wait for and then return one
		if (callsHandled.getAndIncrement() < numCalls) {
			int loopCount = 0;
			boolean success = false;
			while(!success && loopCount < 4 && callsHandled.get() <= numCalls) {
				try {
					//log.debug("awaiting recovery info");
					available.tryAcquire(5000, TimeUnit.MILLISECONDS);
					success = true;
				} catch (InterruptedException e) {
					log.error("Semaphore error during recovery", e);
				}
				loopCount++;
			}
			if (success) {
				try {
					return versions.poll(1000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					log.error("queue/semaphore mismatch during recovery", e);
					return null;
				}
			}
			else
				return null;
		}
		else {
			return null;
		}
	}

	@Override
	public void onComplete(recover_call response) {
		try {
			//log.debug("recovery version received");
			versions.add(response.getResult());
			available.release();
		} catch (TException e) {
			versions = null;
		}
	}

	@Override
	public void onError(Exception exception) {
		log.info("failed to receive recovery set, Thrift Error");
		log.debug("Thrift error during recovery", exception);
		
		//if there was someone waiting for the very last result and it failed, release a semaphore permit
		//  so they can get their null response
		if (callsHandled.getAndIncrement() >= numCalls)
			available.release();
	}

}
