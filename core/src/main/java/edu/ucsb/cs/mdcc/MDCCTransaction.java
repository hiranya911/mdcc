package edu.ucsb.cs.mdcc;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import edu.ucsb.cs.mdcc.messaging.BallotNumber;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicator;

public class MDCCTransaction {
	String id;
	int fastQuorum;
	Map<String, String> writes = new HashMap<String, String>();
	
	Map<String, Long> readVersions = new HashMap<String, Long>();
	
	String[] hosts;
	int[] ports;
	String procId;
	
	public MDCCTransaction(String[] hosts, int[] ports, String procId)
	{
		this.hosts = hosts;
		this.ports = ports;
		id = UUID.randomUUID().toString();
		int n = hosts.length;
		//fastQuorum = n + 1 + (int)Math.ceil((double)n / 4.0) + ((n + 1) % 2);
		fastQuorum = 4;
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
				readVersions.put(object, (long) 0);
			else
			{
				readVersions.put(object, Long.parseLong(readString.substring(0, readString.indexOf('|'))));
			}
			readString = readString.substring(readString.indexOf('|') + 1);
			readString = readString.substring(readString.indexOf('|') + 1);
			return readString;
		}
	}
	
	public boolean write(String object, String value)
	{
		if (!readVersions.containsKey(object))
			return false;
		else
		{
			writes.put(object, value);
			return true;
		}
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
				long oldVersion = readVersions.get(writeObject);
				BallotNumber ballot = new BallotNumber(-1, procId);
	
				if (comms.sendAccept(hosts[i], ports[i], id, writeObject, 
						oldVersion, ballot, writes.get(writeObject)))
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
