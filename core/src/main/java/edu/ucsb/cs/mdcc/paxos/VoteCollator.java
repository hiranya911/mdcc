package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class VoteCollator implements VoteResultListener {

    private static final Log log = LogFactory.getLog(VoteCollator.class);

    public static final String DEFAULT_SERVER_ID = "AppServer";

    private Queue<Option> acceptedOptions = new ConcurrentLinkedQueue<Option>();
	private Queue<Option> rejectedOptions = new ConcurrentLinkedQueue<Option>();
    private Collection<Option> options;
    private MDCCCommunicator communicator;
    private String txnId;

    public VoteCollator(Collection<Option> options, MDCCCommunicator communicator,
                        String txnId) {
        this.options = options;
        this.communicator = communicator;
        this.txnId = txnId;
    }

    public void start() {
        Member[] members = MDCCConfiguration.getConfiguration().getMembers();
        for (Option option : options) {
            if (!option.isClassic()) {
                log.info("Running fast mode");
                PaxosVoteCounter optionVoteCounter = new PaxosVoteCounter(option, this);
                BallotNumber ballot = new BallotNumber(-1, DEFAULT_SERVER_ID);
                for (Member member : members) {
                    communicator.sendAcceptAsync(member, txnId, ballot,
                            option, optionVoteCounter);
                }
            } else {
                log.info("Already in classic mode for key: " + option.getKey());
                boolean done = false;
                ClassicPaxosResultObserver observer = new ClassicPaxosResultObserver(option, this);
                for (Member member : members) {
                    if (communicator.runClassicPaxos(member, txnId,
                            option, observer)) {
                        done = true;
                        break;
                    }
                }
                if (!done) {
                    notifyOutcome(option, false);
                }
            }
        }
    }
	
	public int getAccepts() {
		return acceptedOptions.size();
	}
	
	public int getRejects() {
		return rejectedOptions.size();
	}

    public int getTotal() {
        return acceptedOptions.size() + rejectedOptions.size();
    }

	public void notifyOutcome(Option option, boolean accepted) {
		if (accepted) {
            acceptedOptions.add(option);
		} else {
            if (!option.isClassic()) {
                log.info("Possible conflict detected on key: " +
                        option.getKey() + " - Switching to Classic Paxos mode");
                option.setClassic();
                Member[] members = MDCCConfiguration.getConfiguration().getMembers();
                ClassicPaxosResultObserver observer = new ClassicPaxosResultObserver(option, this);
                for (Member member : members) {
                    if (communicator.runClassicPaxos(member, txnId,
                            option, observer)) {
                        return;
                    }
                }
            }
            rejectedOptions.add(option);
        }

        synchronized(this) {
            this.notifyAll();
        }
    }
}
