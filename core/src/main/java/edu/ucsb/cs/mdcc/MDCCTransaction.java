package edu.ucsb.cs.mdcc;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;
import edu.ucsb.cs.mdcc.messaging.RecordVersion;

public class MDCCTransaction {
	String id;
	int fastQuorum;
	Map<String, String> writes = new HashMap<String, String>();
	
	Map<String, RecordVersion> readVersions = new HashMap<String, RecordVersion>();
	
	String[] hosts;
	int[] ports;
	String procId;
	
	public MDCCTransaction(String[] hosts, int[] ports, String procId)
	{
		this.hosts = hosts;
		this.ports = ports;
		id = UUID.randomUUID().toString();
		if (hosts.length < 5)
			fastQuorum = hosts.length;
		else
			fastQuorum = hosts.length - 1;
		this.procId = procId;
	}
	
	public String read(String object)
	{
		if (writes.containsKey(object))
			return writes.get(object);
		MDCCCommunicator comms = new MDCCCommunicator();
		String readString = comms.get(hosts[0], ports[0], object);
		if (readString == null)
			return null;
		else
		{
			if (readString.startsWith("|"))
				readVersions.put(object, new RecordVersion(0, ""));
			else
			{
				String versionString = readString.substring(0,readString.indexOf('|'));
				RecordVersion readVersion = new RecordVersion(
						Long.parseLong(versionString.substring(0, versionString.indexOf(':'))),
								versionString.substring(versionString.indexOf(':') + 1));
				readVersions.put(object, readVersion);
			}
			return readString.substring(readString.indexOf('|') + 1);
		}
	}
	
	public boolean write(String object, String value)
	{
		writes.put(object, value);
		return true;
	}
	
	public boolean commit()
	{
		MDCCCommunicator comms = new MDCCCommunicator();
		boolean success = true;
		for(String writeObject : writes.keySet())
		{
			int accepts = 0;
			int noaccepts = 0;
			for(int i = 0; i < hosts.length; i++)
			{
				RecordVersion oldVersion = readVersions.get(writeObject);
				RecordVersion newVersion = new RecordVersion( oldVersion.getBallot() + 1, procId);
	
				if (comms.sendAccept(hosts[i], ports[i], id, writeObject, 
						oldVersion, procId, writes.get(writeObject)))
					accepts++;
				else
					noaccepts++;
				
				if (hosts.length - noaccepts < fastQuorum)
					break;
			}
			if (accepts < fastQuorum)
			{
				success = false;
				break;
			}
		}
		
		if (success)
		{
			for(int i = 0; i < hosts.length; i++)
			{
				comms.sendDecide(hosts[i], ports[i], id, true);
			}
		}
		else
		{
			for(int i = 0; i < hosts.length; i++)
			{
				comms.sendDecide(hosts[i], ports[i], id, false);
			}
		}
		return success;
	}
	
	public void abort()
	{
		MDCCCommunicator comms = new MDCCCommunicator();
		for(int i = 0; i < hosts.length; i++)
		{
			comms.sendDecide(hosts[i], ports[i], id, false);
		}
	}
}
