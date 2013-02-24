package edu.ucsb.cs.mdcc.config;

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
	
	
}
