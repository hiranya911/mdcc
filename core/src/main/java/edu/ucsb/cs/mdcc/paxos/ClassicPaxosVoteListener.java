package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.Option;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class ClassicPaxosVoteListener implements VoteResultListener {

    private static final Log log = LogFactory.getLog(ClassicPaxosVoteListener.class);

    private final AtomicBoolean done = new AtomicBoolean(false);
    private AtomicBoolean result = new AtomicBoolean(false);

    public void notifyOutcome(Option option, boolean accepted) {
        if (log.isDebugEnabled()) {
            log.info("Vote came to and end with the result: " + accepted);
        }
        synchronized (done) {
            if (done.compareAndSet(false, true)) {
                result.set(accepted);
                done.notifyAll();
            }
        }
    }

    public boolean getResult() {
        synchronized (done) {
            while (!done.get()) {
                log.debug("Waiting for the vote to come to an end");
                try {
                    done.wait(5000);
                } catch (InterruptedException ignored) {
                }
            }
        }
        return result.get();
    }
}
