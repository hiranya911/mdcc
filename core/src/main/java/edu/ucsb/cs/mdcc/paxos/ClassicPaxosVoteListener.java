package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.Option;

import java.util.concurrent.atomic.AtomicBoolean;

public class ClassicPaxosVoteListener implements VoteResultListener {

    private AtomicBoolean done = new AtomicBoolean(false);
    private AtomicBoolean result = new AtomicBoolean(false);

    public void notifyOutcome(Option option, boolean accepted) {
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
                try {
                    done.wait(5000);
                } catch (InterruptedException ignored) {
                }
            }
        }
        return result.get();
    }
}
