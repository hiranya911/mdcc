package edu.ucsb.cs.mdcc.txn;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.ucsb.cs.mdcc.paxos.TransactionException;

public class TestClient {

    public static void main(String[] args) {
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
                    e.printStackTrace();
                    System.exit(1);
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
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        };

        exec.submit(r1);
        exec.submit(r2);*/

        exec.shutdown();
        try {
            exec.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {

        }


        LocalTransaction txn1 = new LocalTransaction();
        try {
            txn1.begin();
            txn1.write("foo", ByteBuffer.wrap("Foo 1".getBytes()));
            txn1.write("bar", ByteBuffer.wrap("Bar 1".getBytes()));
            txn1.commit();
            System.out.println("Txn 1 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
            System.exit(1);
        }

        LocalTransaction txn2 = new LocalTransaction();
        try {
            txn2.begin();
            ByteBuffer object1 = txn2.read("foo");
            ByteBuffer object2 = txn2.read("bar");
            System.out.println("Foo = " + new String(object1.array()));
            System.out.println("Bar = " + new String(object2.array()));
            txn2.write("bar", ByteBuffer.wrap("Bar Value 4".getBytes()));
            txn2.commit();
            System.out.println("Txn 2 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
            System.exit(1);
        }

        LocalTransaction txn3 = new LocalTransaction();
        try {
            txn3.begin();
            ByteBuffer object1 = txn3.read("foo");
            ByteBuffer object2 = txn3.read("bar");
            System.out.println("Foo = " + new String(object1.array()));
            System.out.println("Bar = " + new String(object2.array()));
            txn3.commit();
            System.out.println("Txn 3 committed");
        } catch (TransactionException e) {
            e.printStackTrace();
        }
    }
}
