package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;

public class VoteCollator implements VoteResultListener {

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
            VoteCounter optionVoteCounter = new VoteCounter(option, this);
            if (!option.isClassic()) {
                BallotNumber ballot = new BallotNumber(-1, DEFAULT_SERVER_ID);
                for (Member member : members) {
                    communicator.sendAcceptAsync(member, txnId, ballot,
                            option, optionVoteCounter);
                }
            } else {
                boolean done = false;
                for (Member member : members) {
                    if (communicator.runClassicPaxos(member, txnId,
                            option, optionVoteCounter)) {
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
                option.setClassic();
                Member[] members = MDCCConfiguration.getConfiguration().getMembers();
                VoteCounter optionVoteCounter = new VoteCounter(option, this);
                for (Member member : members) {
                    if (communicator.runClassicPaxos(member, txnId,
                            option, optionVoteCounter)) {
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
