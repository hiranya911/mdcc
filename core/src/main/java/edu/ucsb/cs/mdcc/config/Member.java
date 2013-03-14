package edu.ucsb.cs.mdcc.config;

import edu.ucsb.cs.mdcc.MDCCException;

public class Member {
	private String hostName;
	private int port;
	private String procId;
	private boolean local;
	
	public Member(String hostName, int port, String processId, boolean local) {
		this.hostName = hostName;
		this.setPort(port);
		this.setProcessId(processId);
		this.local = local;
	}
	
	public Member(String url, String processId, boolean local) throws MDCCException {
		int index = url.indexOf(':');
		if (index < 0) {
			throw new MDCCException("Invalid Member URL");
		}
		this.hostName = url.substring(0, index);
		this.setPort(Integer.parseInt(url.substring(index + 1)));
		this.setProcessId(processId);
		this.local = local;
	}
	
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getProcessId() {
		return procId;
	}

	public void setProcessId(String procId) {
		this.procId = procId;
	}

	public boolean isLocal() {
		return local;
	}

	public void setLocal(boolean local) {
		this.local = local;
	}

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Member) {
            Member member = (Member) obj;
            return member.getHostName().equals(hostName) && member.getPort() == port;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hostName.hashCode() + procId.hashCode() + port;
    }
}
