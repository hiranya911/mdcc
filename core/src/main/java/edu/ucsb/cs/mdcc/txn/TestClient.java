package edu.ucsb.cs.mdcc.txn;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.ucsb.cs.mdcc.paxos.Transaction;
import edu.ucsb.cs.mdcc.paxos.TransactionException;

public class TestClient {

    public static void main(String[] args) {
        TransactionFactory factory = new TransactionFactory();

        ExecutorService exec = Executors.newFixedThreadPool(2);
        /*Runnable r1 = new Runnable() {
            public void run() {
                LocalTransaction txn1 = new LocalTransaction();
                try {
                    txn1.begin();
                    txn1.write("X", ByteBuffer.wrap("1".getBytes()));
                    txn1.commit();
                    System.out.println("Txn 1 committed");
                } catch (TransactionException e) {
                	System.out.println("Txn 1 Aborted");
                }
            }
        };

        Runnable r2 = new Runnable() {
            public void run() {
                LocalTransaction txn2 = new LocalTransaction();
                try {
                    txn2.begin();
                    txn2.write("X", ByteBuffer.wrap("2".getBytes()));
                    txn2.commit();
                    System.out.println("Txn 2 committed");
                } catch (TransactionException e) {
                	System.out.println("Txn 2 Aborted");
                }
            }
        };

        exec.submit(r1);
        exec.submit(r2);*/

        exec.shutdown();
        try {
            exec.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }

        Transaction txn1 = factory.create();
        try {
            txn1.begin();
            txn1.write("foo", "Foo 12345".getBytes());
            txn1.write("bar", "Bar 67890".getBytes());
            txn1.commit();
            System.out.println("Txn 1 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Transaction txn2 = factory.create();
        try {
            txn2.begin();
            byte[] object1 = txn2.read("foo");
            byte[] object2 = txn2.read("bar");
            System.out.println("Foo = " + new String(object1));
            System.out.println("Bar = " + new String(object2));
            txn2.delete("foo");
            txn2.write("bar", "Bar Value 4".getBytes());
            txn2.commit();
            System.out.println("Txn 2 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Transaction txn3 = factory.create();
        try {
            txn3.begin();
            byte[] object2 = txn3.read("bar");
            System.out.println("Bar = " + new String(object2));
            byte[] object1 = txn3.read("foo");
            System.out.println("Foo = " + new String(object1));
            txn3.commit();
            System.out.println("Txn 3 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
        }

        factory.close();
    }
}
