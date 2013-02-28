package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.AsyncClient.runClassic_call;

public class ClassicPaxosResultObserver implements AsyncMethodCallback<runClassic_call> {

    private Option option;
    private VoteResultListener callback;

    public ClassicPaxosResultObserver(Option option, VoteResultListener callback) {
        this.option = option;
        this.callback = callback;
    }

    public void onComplete(runClassic_call runClassic_call) {
        try {
            boolean result = runClassic_call.getResult();
            callback.notifyOutcome(option, result);
        } catch (TException e) {
            onError(e);
        }
    }

    public void onError(Exception e) {
        callback.notifyOutcome(option, false);
    }
}
