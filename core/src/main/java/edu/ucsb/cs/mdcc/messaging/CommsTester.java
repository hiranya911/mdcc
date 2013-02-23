package edu.ucsb.cs.mdcc.messaging;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import edu.ucsb.cs.mdcc.MDCCTransaction;
import edu.ucsb.cs.mdcc.messaging.MDCCCommunicationService.AsyncClient.accept_call;

public class CommsTester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*MDCCCommunicator comms = new MDCCCommunicator();
        System.out.println("ping status:" + comms.ping("localhost", 7911));
        RecordVersion v0 = new RecordVersion(0, "");
        System.out.println("accept(0,a,insert,bob,hello):" + comms.sendAccept("localhost", 7911, "0", "a", v0, "bob", "hello"));
        System.out.println("get(a):" + comms.get("localhost", 7911, "a"));
        System.out.println("decide(0,true):" + comms.sendDecide("localhost", 7911, "0", true));
        System.out.println("get(a):" + comms.get("localhost", 7911, "a"));
        
        System.out.println("accept(1,a,insert,bob,hello):" + comms.sendAccept("localhost", 7911, "1", "a", v0, "bob", "hello"));
        System.out.println("decide(1,false):" + comms.sendDecide("localhost", 7911, "1", false));
        
        RecordVersion v1 = new RecordVersion(1, "bob");
        System.out.println("accept(2,a,1,bob,goodbye):" + comms.sendAccept("localhost", 7911, "2", "a", v1, "bob", "goodbye"));
        System.out.println("get(a):" + comms.get("localhost", 7911, "a"));
        System.out.println("decide(2,true):" + comms.sendDecide("localhost", 7911, "2", true));
        System.out.println("get(a):" + comms.get("localhost", 7911, "a"));*/
		
		String[] hosts = { "localhost", "localhost" , "localhost", "localhost", "localhost"};
		int[] ports = { 7911, 7912, 7913, 7914, 7915 };
		String procId = "proc0";
		
		MDCCTransaction txn = new MDCCTransaction(hosts, ports, procId);
		System.out.println("begin transaction");
		System.out.println("read(a): " + txn.read("a"));
		System.out.println("write(a,hello): " + txn.write("a", "hello"));
		System.out.println("commit: " + txn.commit());
		
		txn = new MDCCTransaction(hosts, ports, procId);
		System.out.println("begin transaction");
		System.out.println("read(a): " + txn.read("a"));
		System.out.println("write(a,goodbye): " + txn.write("a", "goodbye"));
		System.out.println("commit: " + txn.commit());
        
        /*comms.sendAcceptAsync("localhost", 7911, "txn", "objectName", new RecordVersion(), "newValue", new 
        		AsyncMethodCallback<MDCCCommunicationService.AsyncClient.accept_call>() {

					public void onComplete(accept_call response) {
						// TODO Auto-generated method stub
						try {
							System.out.println("Accept status: " + response.getResult());
						} catch (TException e) {
							// TODO Auto-generated catch block
							System.out.println("Accept println failed");
							e.printStackTrace();
						}
					}

					public void onError(Exception exception) {
						// TODO Auto-generated method stub
						System.out.println("Accept Failed");
					}
        			
        		});*/
        /*try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

}
