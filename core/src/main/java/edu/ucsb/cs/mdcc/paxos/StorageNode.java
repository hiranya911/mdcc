package edu.ucsb.cs.mdcc.paxos;

import java.nio.ByteBuffer;
import java.util.*;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.config.MDCCConfiguration;
import edu.ucsb.cs.mdcc.config.Member;
import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;
import edu.ucsb.cs.mdcc.messaging.ReadValue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StorageNode extends Agent {

    private static final Log log = LogFactory.getLog(StorageNode.class);

	private Map<String, Boolean> outstandingOptions = new HashMap<String, Boolean>();
    private Map<String,BallotNumber> ballots = new HashMap<String, BallotNumber>();
    private Map<String, Boolean> prepare = new HashMap<String, Boolean>();
    private Map<String, ReadValue> db = new HashMap<String, ReadValue>();
    private Map<String, Set<Option>> transactions = new HashMap<String, Set<Option>>();
    private Set<String> outstandingClassicKeys = new HashSet<String>();
    private MDCCConfiguration config;

    private MDCCCommunicator communicator;

    public StorageNode() {
        this.config = MDCCConfiguration.getConfiguration();
        this.communicator = new MDCCCommunicator();
    }

    @Override
    public void start() {
        super.start();
        int port = config.getLocalMember().getPort();
        communicator.startListener(this, port);

        //now we talk to everyone else to do recovery
        runRecoveryPhase();
    }

    private void runRecoveryPhase() {
        Map<String, Long> myVersions = new HashMap<String, Long>();
        for (Map.Entry<String, ReadValue> entry : db.entrySet()) {
        	myVersions.put(entry.getKey(), entry.getValue().getVersion());
        }

        RecoverySet recoveryVersions = new RecoverySet(config.getMembers().length - 1);
        for (Member member : config.getMembers()) {
        	if (!member.isLocal()) {
        		communicator.sendRecoverAsync(member, myVersions, recoveryVersions);
        	}
        }

        Map<String, ReadValue> versions;
        while ((versions = recoveryVersions.dequeueRecoveryInfo()) != null) {
            log.info("Received recovery set");
            //replace our entries with any newer entries
            for (Map.Entry<String, ReadValue> entry : versions.entrySet()) {
                if (!db.containsKey(entry.getKey()) ||
                        (entry.getValue().getVersion() > db.get(entry.getKey()).getVersion())) {
                    log.debug("recovered value for '" + entry.getKey() + "'");
                    db.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    @Override
    public void stop() {
        super.stop();
        communicator.stopListener();
    }

    public boolean onAccept(String transaction, String key,
			long oldVersion, BallotNumber ballot, ByteBuffer value) {
		log.info("received accept message for: txn=" + transaction + "; obj=" + key);
		
		synchronized (key.intern()) {
            if (ballot.getNumber() < 0) {
                Boolean outstanding = outstandingOptions.get(key);
                if (Boolean.TRUE.equals(outstanding)) {
                    log.warn("Outstanding option detected on " + key +
                            " - Denying the new option");
                    return false;
                }
            }

            ReadValue entryValue;
            BallotNumber entryBallot;
            if (!db.containsKey(key)) {
            	entryValue = new ReadValue(0, -1, ByteBuffer.allocate(0));
            	entryBallot = new BallotNumber(0,"");
                ballots.put(key, entryBallot);
            } else {
                entryValue = db.get(key);
                entryBallot = ballots.get(key);
            }

            long version = entryValue.getVersion();
            //if it is a new insert
            boolean success = (version == oldVersion) && (ballot.getNumber() == -1 ||
                    ((ballot.getNumber() + ":" + ballot.getProcessId()).compareTo(
                            entryBallot.getNumber() + ":" + entryBallot.getProcessId()) >= 0));

            log.info("version=" + version + "; oldVersion=" + oldVersion);
            log.info("Ballot comparison: " + ((ballot.getNumber() + ":" + ballot.getProcessId()).compareTo(
                    entryBallot.getNumber() + ":" + entryBallot.getProcessId()) >= 0));

            if (success) {
                outstandingOptions.put(key, true);
                if (!transactions.containsKey(transaction)) {
                    Set<Option> set = new TreeSet<Option>(new Comparator<Option>() {
                        public int compare(Option o1, Option o2) {
                            return o1.getKey().compareTo(o2.getKey());
                        }
                    });
                    transactions.put(transaction, set);
                }
                transactions.get(transaction).add(
                        new Option(key, value, entryValue.getVersion(), false));
				log.info("option accepted");
            } else {
				log.warn("option denied");
            }
			return success;
		}
	}

	public void onDecide(String transaction, boolean commit) {
		if (commit) {
			log.info("Received Commit decision on transaction id: " + transaction);
        } else {
			log.info("Received Abort on transaction id: " + transaction);
        }

		if (commit && transactions.containsKey(transaction)) {
            for (Option option : transactions.get(transaction)) {
                ReadValue read = db.get(option.getKey());
                long classicEnd;
                if (read == null) {
                    classicEnd = -1;
                } else {
                    classicEnd = read.getClassicEndVersion();
                }
                db.put(option.getKey(), new ReadValue(option.getOldVersion() + 1,
                        classicEnd,
                        option.getValue()));
                log.info("[COMMIT] Saved option to DB");
                outstandingOptions.remove(option.getKey());
            }
		} else {
            log.warn("[COMMIT=" + commit + "] Not saving to DB");
        }

        for (Option option : transactions.get(transaction)) {
            synchronized (outstandingClassicKeys) {
                outstandingClassicKeys.remove(option.getKey());
            }
        }
        transactions.remove(transaction);
    }

	public ReadValue onRead(String object) {
		if (db.containsKey(object)) {
			return db.get(object);
        } else {
			return new ReadValue(0, -1, ByteBuffer.allocate(0));
        }
	}

	public static void main(String[] args) {
        final StorageNode storageNode = new StorageNode();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                storageNode.stop();
            }
        });
        storageNode.start();
	}

	public boolean onPrepare(String key, BallotNumber ballot, long classicEndVersion) {
        BallotNumber existingBallot = ballots.get(key);
        // TODO: Compare ballots
        ReadValue readValue = db.get(key);
        if (readValue == null) {
            readValue = new ReadValue(0, classicEndVersion, ByteBuffer.wrap("".getBytes()));
            db.put(key, readValue);
        } else {
            readValue.setClassicEndVersion(classicEndVersion);
        }

		return true;
	}

	public Map<String, ReadValue> onRecover(Map<String, Long> versions) {
		Map<String, ReadValue> newVersions = new HashMap<String, ReadValue>();
		log.debug("preparing recovery set");
		//add all the objects that the requester is outdated on
		for (Map.Entry<String, ReadValue> entry : db.entrySet()) {
			if (!versions.containsKey(entry.getKey()) ||
                    (entry.getValue().getVersion() > versions.get(entry.getKey()))) {
				newVersions.put(entry.getKey(), entry.getValue());
			}
		}
		
		return newVersions;
	}

	public boolean runClassic(String transaction, String key,
			long oldVersion, ByteBuffer value) {
        log.info("Requested classic paxos on key: " + key);
		Member leader = findLeader(key, false);
        log.info("Found leader (for key = " + key + ") : " + leader.getProcessId());
        Option option = new Option(key, value, oldVersion, true);
        boolean result;
        if (leader.isLocal()) {
            synchronized (outstandingClassicKeys) {
                if (outstandingClassicKeys.contains(key)) {
                    log.info("Outstanding classic key found for: " + key);
                    return false;
                }
                outstandingClassicKeys.add(key);
            }

            Member[] members = MDCCConfiguration.getConfiguration().getMembers();
            BallotNumber ballot = new BallotNumber(1, leader.getProcessId());
            if (!prepare.containsKey(key)) {
                // run prepare
                log.info("Running prepare phase");

                ReadValue readValue = db.get(key);
                if (readValue != null) {
                    readValue.setClassicEndVersion(readValue.getVersion() + 4);
                } else {
                    readValue = new ReadValue(0, 4, ByteBuffer.wrap("".getBytes()));
                    db.put(key, readValue);
                }

                ClassicPaxosVoteListener prepareListener = new ClassicPaxosVoteListener();
                PaxosVoteCounter prepareVoteCounter = new PaxosVoteCounter(option, prepareListener);
                for (Member member : members) {
                    communicator.sendPrepareAsync(member, key, ballot,
                            readValue.getClassicEndVersion(), prepareVoteCounter);
                }
                if (prepareListener.getResult()) {
                    log.info("Prepare phase SUCCESSFUL");
                    prepare.put(key, true);
                } else {
                    log.warn("Failed to run the prepare phase");
                    return false;
                }
            }

            ClassicPaxosVoteListener listener = new ClassicPaxosVoteListener();
            PaxosVoteCounter voteCounter = new PaxosVoteCounter(option, listener);
            log.info("Running accept phase");
            for (Member member : members) {
                communicator.sendAcceptAsync(member, transaction,
                        ballot, option, voteCounter);
            }

            ReadValue readValue = db.get(key);
            if (readValue.getVersion() == readValue.getClassicEndVersion()) {
                log.info("Done with the classic rounds - Reverting back to fast mode");
                prepare.remove(key);
            }
            result = listener.getResult();
        } else {
            ClassicPaxosVoteListener listener = new ClassicPaxosVoteListener();
            ClassicPaxosResultObserver observer = new ClassicPaxosResultObserver(option, listener);
            communicator.runClassicPaxos(leader, transaction, option, observer);
            result = listener.getResult();
        }
        return result;
    }

}
