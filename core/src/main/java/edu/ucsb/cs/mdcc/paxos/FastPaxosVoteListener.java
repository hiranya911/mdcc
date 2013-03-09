package edu.ucsb.cs.mdcc.paxos;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.config.AppServerConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FastPaxosVoteListener implements VoteResultListener {

    private static final Log log = LogFactory.getLog(FastPaxosVoteListener.class);

    public static final String DEFAULT_SERVER_ID = "AppServer";

    private Queue<Option> acceptedOptions = new ConcurrentLinkedQueue<Option>();
	private Queue<Option> rejectedOptions = new ConcurrentLinkedQueue<Option>();
    private Collection<Option> options;
    private MDCCCommunicator communicator;
    private String txnId;

    public FastPaxosVoteListener(Collection<Option> options, MDCCCommunicator communicator,
                                 String txnId) {
        this.options = options;
        this.communicator = communicator;
        this.txnId = txnId;
    }

    public void start() {
        AppServerConfiguration config = AppServerConfiguration.getConfiguration();
        Map<Integer,List<Option>> fastOptionsMap = new HashMap<Integer, List<Option>>();
        Map<Integer,List<Accept>> fastAcceptsMap = new HashMap<Integer, List<Accept>>();
        List<Option> classicOptions = new ArrayList<Option>();
        BallotNumber fastBallot = new BallotNumber(-1, DEFAULT_SERVER_ID);

        for (Option option : options) {
        	if (!option.isClassic()) {
                if (log.isDebugEnabled()) {
                    log.debug("Running fast accept on: " + option.getKey());
                }
                int shardId = config.getShardId(option.getKey());
                List<Option> fastOptions = fastOptionsMap.get(shardId);
                if (fastOptions == null) {
                    fastOptions = new ArrayList<Option>();
                    fastOptionsMap.put(shardId, fastOptions);
                }
        		fastOptions.add(option);

                List<Accept> fastAccepts = fastAcceptsMap.get(shardId);
                if (fastAccepts == null) {
                    fastAccepts = new ArrayList<Accept>();
                    fastAcceptsMap.put(shardId, fastAccepts);
                }
        		fastAccepts.add(new Accept(txnId, fastBallot, option));
        	} else {
        		classicOptions.add(option);
        	}
        }
        
        for (Integer shardId : fastOptionsMap.keySet()) {
            List<Option> fastOptions = fastOptionsMap.get(shardId);
            List<Accept> fastAccepts = fastAcceptsMap.get(shardId);
            Member[] members = config.getMembers(shardId);
            PaxosBulkVoteCounter fastCallback = new PaxosBulkVoteCounter(fastOptions, this, members.length);
            if (fastAccepts.size() > 0) {
                for (Member member : members) {
                    communicator.sendBulkAcceptAsync(member, fastAccepts, fastCallback);
                }
            }
        }

        
        for (Option option : classicOptions) {
            if (log.isDebugEnabled()) {
                log.debug("Already in classic mode for key: " + option.getKey());
            }
            boolean done = false;
            ClassicPaxosResultObserver observer = new ClassicPaxosResultObserver(option, this);
            Member[] members = config.getMembers(option.getKey());
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
	
	public int getAccepts() {
		return acceptedOptions.size();
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
                Member[] members = AppServerConfiguration.getConfiguration().getMembers(option.getKey());
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
