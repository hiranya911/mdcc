package edu.ucsb.cs.mdcc.paxos;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import edu.ucsb.cs.mdcc.messaging.ReadValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.AsyncClient.recover_call;

public class RecoverySet implements
		AsyncMethodCallback<MDCCCommunicationService.AsyncClient.recover_call> {

	private static final Log log = LogFactory.getLog(RecoverySet.class);

    private final Queue<Map<String, ReadValue>> versions = new ConcurrentLinkedQueue<Map<String, ReadValue>>();

    private AtomicInteger expectedCalls;
	
	public RecoverySet(int numCalls) {
		this.expectedCalls = new AtomicInteger(numCalls);
	}
	
	public Map<String, ReadValue> dequeueRecoveryInfo() {
        synchronized (versions) {
            while (expectedCalls.get() > 0 && versions.isEmpty()) {
                try {
                    versions.wait(5000);
                } catch (InterruptedException ignored) {
                }
            }
            return versions.poll();
        }
	}

	public void onComplete(recover_call response) {
        try {
            log.debug("Recovery data received from remote member");
            synchronized (versions) {
                versions.add(response.getResult());
                expectedCalls.decrementAndGet();
                versions.notifyAll();
            }
        } catch (TException e) {
            onError(e);
        }
    }

	public void onError(Exception exception) {
		log.debug("Thrift error during recovery", exception);
        synchronized (versions) {
            expectedCalls.decrementAndGet();
            versions.notifyAll();
        }
	}

}
