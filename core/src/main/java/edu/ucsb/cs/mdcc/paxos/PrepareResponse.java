package edu.ucsb.cs.mdcc.paxos;

import java.nio.ByteBuffer;

import edu.ucsb.cs.mdcc.Option;

public class PrepareResponse {

    private boolean ok;
    private String outstanding;

    public PrepareResponse(boolean ok, String outstanding) {
        this.ok = ok;
        this.outstanding = outstanding;
    }

    public boolean isOK() {
        return ok;
    }

    public String getOutstanding() {
    	return outstanding;
    }
}
